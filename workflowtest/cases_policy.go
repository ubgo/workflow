package workflowtest

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// concurrencyLabel is the Job.Labels key the concurrency and rate-limit
// subtests bucket runs by.
const concurrencyLabel = "k"

// tracker records which runs started, which saw their context end, and
// the peak number of runs active at once per concurrency key. A run's
// payload is "key:name", so the handler knows both without labels.
type tracker struct {
	mu        sync.Mutex
	active    map[string]int
	peak      map[string]int
	started   map[string]bool
	cancelled map[string]bool
}

func newTracker() *tracker {
	return &tracker{active: map[string]int{}, peak: map[string]int{}, started: map[string]bool{}, cancelled: map[string]bool{}}
}

// handler counts the run in, waits for g (or ctx), and counts it out.
func (tr *tracker) handler(g *gate) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		name := string(in)
		key, _, _ := strings.Cut(name, ":")
		tr.mu.Lock()
		tr.started[name] = true
		tr.active[key]++
		if tr.active[key] > tr.peak[key] {
			tr.peak[key] = tr.active[key]
		}
		tr.mu.Unlock()
		err := g.wait(ctx)
		tr.mu.Lock()
		tr.active[key]--
		if err != nil {
			tr.cancelled[name] = true
		}
		tr.mu.Unlock()
		return in, err
	}
}

func (tr *tracker) hasStarted(name string) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.started[name]
}

func (tr *tracker) wasCancelled(name string) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.cancelled[name]
}

func (tr *tracker) peakOf(key string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.peak[key]
}

// job is a run of wf in concurrency bucket key, named name.
func job(wf, key, name string) workflow.Job {
	return workflow.Job{Workflow: wf, Payload: []byte(key + ":" + name), Labels: map[string]string{concurrencyLabel: key}}
}

// concurrencyWorkflow caps runs at one per key with strategy s.
func concurrencyWorkflow(e *env, s workflow.ConcurrencyStrategy, tr *tracker, g *gate) workflow.Workflow {
	wf := oneStep(e.name("w"), tr.handler(g))
	wf.Concurrency = &workflow.Concurrency{Key: e.h.keyFromLabel(concurrencyLabel), MaxRuns: 1, Strategy: s}
	return wf
}

// newestRefused checks the new run of a full bucket under a drop or
// cancel-newest strategy: Enqueue accepts it (nil error, a RunRef), its
// status becomes StatusCancelled, and its step never runs. The rule is on
// the strategy constants in spec.go.
func newestRefused(e *env, s workflow.ConcurrencyStrategy) {
	e.need(workflow.CapConcurrencyKey)
	tr, g := newTracker(), newGate(e.t)
	wf := concurrencyWorkflow(e, s, tr, g)
	e.up(wf)
	a := e.enqueue(job(wf.Name, "x", "a"))
	e.eventually("run a to start", 0, func() bool { return tr.hasStarted("x:a") })
	b := e.enqueue(job(wf.Name, "x", "b"))
	e.waitStatus(b, workflow.StatusCancelled, 0)
	g.open()
	e.waitStatus(a, workflow.StatusSucceeded, 0)
	e.quiet()
	if tr.hasStarted("x:b") {
		e.t.Fatalf("the newest run of a full bucket started under %s", s)
	}
}

var concurrencyCases = []testCase{
	{"queue_newest: the new run waits and other keys run", func(e *env) {
		e.need(workflow.CapConcurrencyKey)
		tr, g := newTracker(), newGate(e.t)
		wf := concurrencyWorkflow(e, workflow.ConcurrencyQueueNewest, tr, g)
		e.up(wf)
		a := e.enqueue(job(wf.Name, "x", "a"))
		e.eventually("run a to start", 0, func() bool { return tr.hasStarted("x:a") })
		b := e.enqueue(job(wf.Name, "x", "b"))
		c := e.enqueue(job(wf.Name, "y", "c"))
		e.eventually("run c (another key) to start while a holds x", 0, func() bool { return tr.hasStarted("y:c") })
		e.quiet()
		if tr.hasStarted("x:b") {
			e.t.Fatal("run b started while run a held the only slot of its key")
		}
		g.open()
		for _, ref := range []workflow.RunRef{a, b, c} {
			e.waitStatus(ref, workflow.StatusSucceeded, 0)
		}
		if p := tr.peakOf("x"); p != 1 {
			e.t.Fatalf("key x had %d runs at once, want at most 1", p)
		}
	}},
	{"drop_newest: the new run is dropped", func(e *env) {
		newestRefused(e, workflow.ConcurrencyDropNewest)
	}},
	{"cancel_newest: the new run is cancelled", func(e *env) {
		newestRefused(e, workflow.ConcurrencyCancelNewest)
	}},
	{"cancel_in_progress: the running run is cancelled for the new one", func(e *env) {
		e.need(workflow.CapConcurrencyKey)
		tr, g := newTracker(), newGate(e.t)
		wf := concurrencyWorkflow(e, workflow.ConcurrencyCancelInProgress, tr, g)
		e.up(wf)
		a := e.enqueue(job(wf.Name, "x", "a"))
		e.eventually("run a to start", 0, func() bool { return tr.hasStarted("x:a") })
		b := e.enqueue(job(wf.Name, "x", "b"))
		e.waitStatus(a, workflow.StatusCancelled, 0)
		e.eventually("run a's context to end", 0, func() bool { return tr.wasCancelled("x:a") })
		e.eventually("run b to start", 0, func() bool { return tr.hasStarted("x:b") })
		g.open()
		e.waitStatus(b, workflow.StatusSucceeded, 0)
	}},
	{"round_robin: each key keeps its cap and every run finishes", func(e *env) {
		e.need(workflow.CapConcurrencyKey)
		tr, g := newTracker(), newGate(e.t)
		wf := concurrencyWorkflow(e, workflow.ConcurrencyRoundRobin, tr, g)
		e.up(wf)
		refs := []workflow.RunRef{
			e.enqueue(job(wf.Name, "x", "a1")),
			e.enqueue(job(wf.Name, "x", "a2")),
			e.enqueue(job(wf.Name, "y", "b1")),
		}
		e.eventually("a run of each key to start", 0, func() bool {
			return (tr.hasStarted("x:a1") || tr.hasStarted("x:a2")) && tr.hasStarted("y:b1")
		})
		e.quiet()
		g.open()
		for _, ref := range refs {
			e.waitStatus(ref, workflow.StatusSucceeded, 0)
		}
		if p := tr.peakOf("x"); p != 1 {
			e.t.Fatalf("key x had %d runs at once, want at most 1", p)
		}
	}},
}

var rateLimitCases = []testCase{
	{"a limit holds over windows of Per", func(e *env) {
		// "At most Limit starts per Per" is checked in the form both a
		// sliding-window and a fixed-window engine meet honestly:
		//   (a) burst: no span shorter than Per holds more than 2*Limit
		//       starts (a fixed window allows Limit just before a boundary
		//       and Limit just after it);
		//   (b) rate: N starts span at least (ceil(N/Limit)-2)*Per, the
		//       fixed-window minimum (the first and last windows may each
		//       be entered at their edge);
		//   (c) exactness: on a fake clock, exactly Limit start before
		//       the clock moves.
		// On real time the spans may be short by Quiet, to absorb the
		// delay between a driver admitting a run and its handler running.
		e.need(workflow.CapRateLimit)
		const limit, runs = 2, 8
		per := e.h.rateLimitPer(e.tm.unit)
		var slack time.Duration
		if e.clk == nil {
			slack = e.tm.quiet
		}
		var mu sync.Mutex
		var starts []time.Time
		wf := oneStep(e.name("w"), func(_ context.Context, in []byte) ([]byte, error) {
			mu.Lock()
			starts = append(starts, e.now())
			mu.Unlock()
			return in, nil
		})
		wf.RateLimits = []workflow.RateLimit{{Key: e.h.keyFromLabel(concurrencyLabel), Limit: limit, Per: per}}
		e.up(wf)
		refs := make([]workflow.RunRef, 0, runs)
		for i := 0; i < runs; i++ {
			refs = append(refs, e.enqueue(job(wf.Name, "x", strconv.Itoa(i))))
		}
		if e.clk != nil {
			e.eventually("the first window's starts", 0, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(starts) >= limit
			})
			e.quiet()
			mu.Lock()
			n := len(starts)
			mu.Unlock()
			if n != limit {
				e.t.Fatalf("(c) %d runs started before the clock moved, want exactly Limit %d", n, limit)
			}
		}
		for _, ref := range refs {
			// runs/limit windows must pass before the last run starts.
			e.waitStatusWithin(e.tm.wait+runs/limit*per, ref, workflow.StatusSucceeded, per/4)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(starts) != runs {
			e.t.Fatalf("%d starts, want %d", len(starts), runs)
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
		const burst = 2 * limit
		for i := burst; i < len(starts); i++ {
			if span := starts[i].Sub(starts[i-burst]); span < per-slack {
				e.t.Errorf("(a) starts %d to %d (%d starts) fit in %s, under Per %s: more than 2*Limit in one window", i-burst, i, burst+1, span, per)
			}
		}
		windows := (len(starts) + limit - 1) / limit
		if minSpan := time.Duration(windows-2) * per; minSpan > 0 {
			if span := starts[len(starts)-1].Sub(starts[0]); span < minSpan-slack {
				e.t.Errorf("(b) %d starts spanned %s, want at least %s ((ceil(N/Limit)-2)*Per)", len(starts), span, minSpan)
			}
		}
	}},
	{"a second start in the window waits, another key does not", func(e *env) {
		e.need(workflow.CapRateLimit)
		tr, g := newTracker(), newGate(e.t)
		g.open() // runs finish at once; only their starts are limited
		per := e.h.rateLimitPer(e.tm.unit)
		wf := oneStep(e.name("w"), tr.handler(g))
		wf.RateLimits = []workflow.RateLimit{{Key: e.h.keyFromLabel(concurrencyLabel), Limit: 1, Per: per}}
		e.up(wf)
		a := e.enqueue(job(wf.Name, "x", "a"))
		b := e.enqueue(job(wf.Name, "x", "b"))
		c := e.enqueue(job(wf.Name, "y", "c"))
		e.eventually("one run of key x and the run of key y to start", 0, func() bool {
			return (tr.hasStarted("x:a") || tr.hasStarted("x:b")) && tr.hasStarted("y:c")
		})
		e.quiet()
		if tr.hasStarted("x:a") && tr.hasStarted("x:b") {
			e.t.Fatalf("two runs of key x started within one %s window with Limit 1", per)
		}
		for _, ref := range []workflow.RunRef{a, b, c} {
			e.waitStatusWithin(e.tm.wait+per, ref, workflow.StatusSucceeded, per/4)
		}
	}},
}

// everyMinute is the cron expression the cron subtests use.
const everyMinute = "* * * * *"

// needCronFiring skips a firing subtest on real time unless the harness
// opted in, because each waits for a minute boundary.
func needCronFiring(e *env) {
	e.t.Helper()
	e.need(workflow.CapCron)
	if e.clk == nil && !e.h.RealCron {
		e.t.Skip("cron firing on real time waits for a minute boundary; set Harness.RealCron")
	}
}

// tick moves to the next minute boundary on a fake clock (after the
// driver armed want timers) and waits until c reaches n; on real time it
// just waits up to two minutes.
func tick(e *env, c *counter, want, n int) {
	e.t.Helper()
	if e.clk != nil {
		e.blockUntilTimers(want)
		e.clk.Advance(time.Minute)
		e.eventually("the cron to fire", 0, func() bool { return c.count() >= n })
		return
	}
	e.eventuallyWithin(realCronWait, "the cron to fire", 0, func() bool { return c.count() >= n })
}

var cronCases = []testCase{
	{"a declared cron registered twice fires once per tick", func(e *env) {
		needCronFiring(e)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		wf.Cron = []string{everyMinute}
		e.register(wf)
		e.up(wf) // registers the same name again, then starts
		tick(e, &c, 1, 1)
		e.quiet()
		if got := c.count(); got != 1 {
			e.t.Fatalf("one tick fired %d runs, want 1 (re-register must replace, not duplicate)", got)
		}
		if e.clk != nil {
			tick(e, &c, 1, 2)
			e.quiet()
			if got := c.count(); got != 2 {
				e.t.Fatalf("two ticks fired %d runs, want 2", got)
			}
		}
	}},
	{"Schedule fires with its Input and Unschedule stops it", func(e *env) {
		needCronFiring(e)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		input := []byte(`"scheduled"`)
		id, err := e.c.Schedule(ctx, workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: everyMinute, Location: time.UTC, Input: input})
		if err != nil {
			e.t.Fatalf("Schedule: %v", err)
		}
		if id.IsZero() {
			e.t.Fatal("Schedule returned an empty ScheduleID")
		}
		tick(e, &c, 1, 1)
		if got := c.input(); !bytes.Equal(got, input) {
			e.t.Errorf("scheduled run input = %q, want %q", got, input)
		}
		ctx2, cancel2 := e.callCtx()
		defer cancel2()
		if err := e.c.Unschedule(ctx2, id); err != nil {
			e.t.Fatalf("Unschedule: %v", err)
		}
		if err := e.c.Unschedule(ctx2, ""); err != nil {
			e.t.Fatalf("Unschedule of an empty id: %v, want nil", err)
		}
		if e.clk != nil {
			before := c.count()
			e.clk.Advance(2 * time.Minute)
			e.quiet()
			if got := c.count(); got != before {
				e.t.Fatalf("an unscheduled cron fired %d more runs", got-before)
			}
		}
	}},
	{"Schedule evaluates Cron in its Location, or refuses the zone", func(e *env) {
		e.need(workflow.CapCron)
		if e.clk == nil {
			e.t.Skip("checking the zone needs a fake clock to jump to a chosen hour")
		}
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		zone := time.FixedZone("UTC+5", 5*60*60)
		ctx, cancel := e.callCtx()
		defer cancel()
		// 09:00 in UTC+5 is 04:00 UTC; a driver that ignored the zone
		// would fire at 09:00 UTC instead.
		_, err := e.c.Schedule(ctx, workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: "0 9 * * *", Location: zone})
		if err != nil {
			assertUnsupported(e.t, err, "Location", 0)
			return
		}
		e.blockUntilTimers(1)
		now := e.clk.Now().UTC()
		fire := time.Date(now.Year(), now.Month(), now.Day(), 4, 0, 0, 0, time.UTC)
		for !fire.After(now.Add(time.Minute)) {
			fire = fire.AddDate(0, 0, 1)
		}
		e.clk.Set(fire.Add(-30 * time.Second))
		e.quiet()
		if got := c.count(); got != 0 {
			e.t.Fatalf("the schedule fired %d times before 09:00 in its zone", got)
		}
		e.clk.Advance(time.Minute)
		e.eventually("the schedule to fire at 09:00 in its zone", 0, func() bool { return c.count() == 1 })
	}},
	{"Schedule without a Location is ErrInvalid", func(e *env) {
		e.need(workflow.CapCron)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ctx, cancel := e.callCtx()
		defer cancel()
		_, err := e.c.Schedule(ctx, workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: everyMinute})
		if !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("Schedule without Location: err = %v, want ErrInvalid", err)
		}
	}},
}
