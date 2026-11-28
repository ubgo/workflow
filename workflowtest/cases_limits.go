package workflowtest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// Several concurrency keys per workflow (Workflow.ConcurrencyLimits,
// CapConcurrencyLimits). Every case uses two limits: one keyed by the job
// label "a", one by the label "b". Each run is held by a gate of its own,
// so a case ends runs one at a time and reads which waiting runs a freed
// slot let in.

// The labels the two limits of every case key on.
const (
	limitLabelA = "a"
	limitLabelB = "b"
)

// limitRun is one run of a limits case: its name and its key value under
// each limit.
type limitRun struct{ name, a, b string }

// limitJob is r as a job of wf. The payload is the run's name.
func limitJob(wf string, r limitRun) workflow.Job {
	return workflow.Job{Workflow: wf, Payload: []byte(r.name), Labels: map[string]string{limitLabelA: r.a, limitLabelB: r.b}}
}

// limitTracker holds each run of a limits case on its own gate and records
// the order of starts and the peak number of runs at once per bucket
// ("a=<value>", "b=<value>").
type limitTracker struct {
	mu     sync.Mutex
	runs   map[string]limitRun
	gates  map[string]*gate
	order  []string
	active map[string]int
	peak   map[string]int
	fail   map[string]bool // runs whose attempts fail with errBoom once their gate opens
}

func newLimitTracker(e *env, runs ...limitRun) *limitTracker {
	tr := &limitTracker{runs: map[string]limitRun{}, gates: map[string]*gate{}, active: map[string]int{}, peak: map[string]int{}, fail: map[string]bool{}}
	for _, r := range runs {
		tr.runs[r.name] = r
		tr.gates[r.name] = newGate(e.t)
	}
	return tr
}

// handler counts the run into both its buckets, waits for its gate (or
// ctx), and counts it out.
func (tr *limitTracker) handler() workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		name := string(in)
		tr.mu.Lock()
		r, g := tr.runs[name], tr.gates[name]
		tr.order = append(tr.order, name)
		for _, b := range []string{"a=" + r.a, "b=" + r.b} {
			tr.active[b]++
			tr.peak[b] = max(tr.peak[b], tr.active[b])
		}
		tr.mu.Unlock()
		err := g.wait(ctx)
		tr.mu.Lock()
		for _, b := range []string{"a=" + r.a, "b=" + r.b} {
			tr.active[b]--
		}
		fail := tr.fail[name]
		tr.mu.Unlock()
		if err == nil && fail {
			err = errBoom
		}
		return in, err
	}
}

func (tr *limitTracker) open(name string) { tr.gates[name].open() }

func (tr *limitTracker) started(name string) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, n := range tr.order {
		if n == name {
			return true
		}
	}
	return false
}

// starts returns how many times run name started.
func (tr *limitTracker) starts(name string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	n := 0
	for _, s := range tr.order {
		if s == name {
			n++
		}
	}
	return n
}

func (tr *limitTracker) startOrder() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]string(nil), tr.order...)
}

func (tr *limitTracker) peakOf(bucket string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.peak[bucket]
}

// limitsWorkflow is a one-step workflow capped at maxA runs per value of
// label "a" and maxB per value of label "b". Its step has no retries.
func limitsWorkflow(e *env, tr *limitTracker, maxA, maxB int) workflow.Workflow {
	wf := oneStep(e.name("limits"), tr.handler())
	wf.Steps[0].Retry = workflow.RetriesNone()
	wf.ConcurrencyLimits = []workflow.ConcurrencyLimit{
		{Key: e.h.keyFromLabel(limitLabelA), MaxRuns: maxA},
		{Key: e.h.keyFromLabel(limitLabelB), MaxRuns: maxB},
	}
	return wf
}

// assertNotStarted fails when any of names has started.
func assertNotStarted(e *env, tr *limitTracker, why string, names ...string) {
	e.t.Helper()
	for _, n := range names {
		if tr.started(n) {
			e.t.Fatalf("run %s started %s (starts: %v)", n, why, tr.startOrder())
		}
	}
}

// limitEnding is one way a run that holds both keys ends.
type limitEnding struct {
	name string
	end  func(e *env, tr *limitTracker, holder string, ref workflow.RunRef)
	want workflow.Status
}

var limitEndings = []limitEnding{
	{"finish", func(_ *env, tr *limitTracker, holder string, _ workflow.RunRef) { tr.open(holder) }, workflow.StatusSucceeded},
	{"failure", func(_ *env, tr *limitTracker, holder string, _ workflow.RunRef) {
		tr.mu.Lock()
		tr.fail[holder] = true
		tr.mu.Unlock()
		tr.open(holder)
	}, workflow.StatusFailed},
	{"cancel", func(e *env, _ *limitTracker, _ string, ref workflow.RunRef) {
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Cancel(ctx, ref); err != nil {
			e.t.Fatalf("Cancel: %v", err)
		}
	}, workflow.StatusCancelled},
}

// limitsReleaseOn checks that a run holding a slot of both keys gives both
// back when it ends the way ending says: a run waiting on its "a" bucket
// and a run waiting on its "b" bucket both start.
func limitsReleaseOn(e *env, ending limitEnding) {
	e.need(workflow.CapConcurrencyLimits)
	if ending.want == workflow.StatusCancelled {
		e.need(workflow.CapCancel)
	}
	holder, onA, onB := limitRun{"holder", "x", "p"}, limitRun{"on-a", "x", "q"}, limitRun{"on-b", "y", "p"}
	tr := newLimitTracker(e, holder, onA, onB)
	wf := limitsWorkflow(e, tr, 1, 1)
	e.up(wf)
	h := e.enqueue(limitJob(wf.Name, holder))
	e.eventually("the holder to start", 0, func() bool { return tr.started(holder.name) })
	a := e.enqueue(limitJob(wf.Name, onA))
	b := e.enqueue(limitJob(wf.Name, onB))
	e.quiet()
	assertNotStarted(e, tr, "while the holder had the only slot of one of its keys", onA.name, onB.name)
	ending.end(e, tr, holder.name, h)
	e.waitStatus(h, ending.want, 0)
	e.eventually("both waiting runs to start once the holder ended ("+ending.name+")", 0, func() bool {
		return tr.started(onA.name) && tr.started(onB.name)
	})
	tr.open(onA.name)
	tr.open(onB.name)
	e.waitStatus(a, workflow.StatusSucceeded, 0)
	e.waitStatus(b, workflow.StatusSucceeded, 0)
}

var concurrencyLimitCases = []testCase{
	{"two keys with different limits: a run starts only when both have room", func(e *env) {
		e.need(workflow.CapConcurrencyLimits)
		// a: 1 per value; b: 2 per value.
		r1, r2 := limitRun{"r1", "x", "p"}, limitRun{"r2", "y", "p"}
		r3 := limitRun{"r3", "z", "p"} // b=p is full (r1, r2) though a=z is free
		r4 := limitRun{"r4", "x", "q"} // a=x is full (r1) though b=q is free
		r5 := limitRun{"r5", "w", "r"} // both free
		tr := newLimitTracker(e, r1, r2, r3, r4, r5)
		wf := limitsWorkflow(e, tr, 1, 2)
		e.up(wf)
		refs := map[string]workflow.RunRef{}
		for _, r := range []limitRun{r1, r2} {
			refs[r.name] = e.enqueue(limitJob(wf.Name, r))
		}
		e.eventually("r1 and r2 to start (b=p has two slots)", 0, func() bool { return tr.started("r1") && tr.started("r2") })
		for _, r := range []limitRun{r3, r4, r5} {
			refs[r.name] = e.enqueue(limitJob(wf.Name, r))
		}
		e.eventually("r5 to start (both its keys have room)", 0, func() bool { return tr.started("r5") })
		e.quiet()
		assertNotStarted(e, tr, "while one of its keys was full", "r3", "r4")
		tr.open("r2") // frees b=p: r3 may start; a=x is still r1's
		e.eventually("r3 to start once b=p had room", 0, func() bool { return tr.started("r3") })
		e.quiet()
		assertNotStarted(e, tr, "while a=x was still full", "r4")
		tr.open("r1") // frees a=x
		e.eventually("r4 to start once a=x had room", 0, func() bool { return tr.started("r4") })
		for _, n := range []string{"r3", "r4", "r5"} {
			tr.open(n)
		}
		for _, ref := range refs {
			e.waitStatus(ref, workflow.StatusSucceeded, 0)
		}
		for bucket, limit := range map[string]int{"a=x": 1, "b=p": 2} {
			if p := tr.peakOf(bucket); p > limit {
				e.t.Fatalf("bucket %s had %d runs at once, want at most %d", bucket, p, limit)
			}
		}
	}},
	{"a run that needs two keys is not overtaken by runs that need one of them", func(e *env) {
		e.need(workflow.CapConcurrencyLimits)
		// big needs a=x (held by first) and b=p (free). The small runs
		// need b=p and an a value of their own. If they could take b=p
		// while big waits for a=x, a steady stream of them would keep
		// b=p busy whenever a=x frees, and big would never start.
		first := limitRun{"first", "x", "f"}
		big := limitRun{"big", "x", "p"}
		s1, s2 := limitRun{"s1", "u1", "p"}, limitRun{"s2", "u2", "p"}
		tr := newLimitTracker(e, first, big, s1, s2)
		wf := limitsWorkflow(e, tr, 1, 1)
		e.up(wf)
		f := e.enqueue(limitJob(wf.Name, first))
		e.eventually("the first run to start", 0, func() bool { return tr.started(first.name) })
		refs := []workflow.RunRef{f}
		for _, r := range []limitRun{big, s1, s2} {
			refs = append(refs, e.enqueue(limitJob(wf.Name, r)))
			e.quiet() // enqueue order is admission order
		}
		e.quiet()
		assertNotStarted(e, tr, "ahead of big, an earlier run waiting for b=p", big.name, s1.name, s2.name)
		tr.open(first.name)
		e.eventually("big to start once a=x had room", 0, func() bool { return tr.started(big.name) })
		e.quiet()
		assertNotStarted(e, tr, "while big held b=p", s1.name, s2.name)
		tr.open(big.name)
		e.eventually("s1 to start after big", 0, func() bool { return tr.started(s1.name) })
		tr.open(s1.name)
		e.eventually("s2 to start after s1", 0, func() bool { return tr.started(s2.name) })
		tr.open(s2.name)
		for _, ref := range refs {
			e.waitStatus(ref, workflow.StatusSucceeded, 0)
		}
		if got, want := strings.Join(tr.startOrder(), " "), "first big s1 s2"; got != want {
			e.t.Fatalf("order of starts:\n got %s\nwant %s", got, want)
		}
	}},
	{"both keys are given back when the run finishes", func(e *env) { limitsReleaseOn(e, limitEndings[0]) }},
	{"both keys are given back when the run fails", func(e *env) { limitsReleaseOn(e, limitEndings[1]) }},
	{"both keys are given back when the run is cancelled", func(e *env) { limitsReleaseOn(e, limitEndings[2]) }},
	{"a run waiting on its retry backoff keeps both keys until it ends", func(e *env) {
		// A run holds its slots until it ends, retries and backoffs
		// included (the rule of Concurrency in docs/jobs/concurrency.md),
		// and with several keys it holds all of them, never one: the
		// runs waiting on either key start only when it is cancelled.
		e.need(workflow.CapsOf(workflow.CapConcurrencyLimits, workflow.CapCancel))
		holder, onA, onB := limitRun{"holder", "x", "p"}, limitRun{"on-a", "x", "q"}, limitRun{"on-b", "y", "p"}
		tr := newLimitTracker(e, holder, onA, onB)
		tr.fail[holder.name] = true
		tr.open(holder.name) // every attempt fails at once
		wf := limitsWorkflow(e, tr, 1, 1)
		wf.Steps[0].Retry = &workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Hour}}
		e.up(wf)
		h := e.enqueue(limitJob(wf.Name, holder))
		e.eventually("the holder's first attempt", 0, func() bool { return tr.started(holder.name) })
		a := e.enqueue(limitJob(wf.Name, onA))
		b := e.enqueue(limitJob(wf.Name, onB))
		e.quiet()
		e.quiet()
		assertNotStarted(e, tr, "while the holder waited on its retry backoff", onA.name, onB.name)
		if n := tr.starts(holder.name); n != 1 {
			e.t.Fatalf("the holder made %d attempts, want 1 (the second is an hour away)", n)
		}
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Cancel(ctx, h); err != nil && !errors.Is(err, workflow.ErrNotFound) {
			e.t.Fatalf("Cancel: %v", err)
		}
		e.waitStatus(h, workflow.StatusCancelled, 0)
		e.eventually("both waiting runs to start once the holder was cancelled", 0, func() bool {
			return tr.started(onA.name) && tr.started(onB.name)
		})
		tr.open(onA.name)
		tr.open(onB.name)
		e.waitStatus(a, workflow.StatusSucceeded, 0)
		e.waitStatus(b, workflow.StatusSucceeded, 0)
	}},
	{"a limit with a strategy other than queue_newest is refused", func(e *env) {
		e.need(workflow.CapConcurrencyLimits)
		var c counter
		wf := oneStep(e.name("limits"), c.echo())
		wf.Concurrency = &workflow.Concurrency{Key: e.h.keyFromLabel(limitLabelA), MaxRuns: 1, Strategy: workflow.ConcurrencyDropNewest}
		wf.ConcurrencyLimits = []workflow.ConcurrencyLimit{{Key: e.h.keyFromLabel(limitLabelB), MaxRuns: 1}}
		err := e.c.Register(wf)
		if err == nil {
			err = e.tryStart()
		}
		if !errors.Is(err, workflow.ErrInvalid) {
			e.t.Fatalf("err = %v, want ErrInvalid", err)
		}
	}},
}
