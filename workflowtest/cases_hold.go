package workflowtest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// Hold policies (Concurrency.Hold, ConcurrencyLimit.Hold,
// CapHoldWhileRunning). Every case keys Concurrency on the job label "a"
// with MaxRuns 1 unless it says otherwise; the case on two policies adds a
// limit on label "b". A run's payload is its name, so each handler knows
// whose attempt it runs.

// holdTracker drives the runs of a hold case: per run an optional gate a
// successful attempt waits on, a number of first attempts per step that
// fail with errBoom, and a first attempt that snoozes. It records the
// order of attempts and, per bucket ("a=<value>", "b=<value>"), the peak
// number of distinct runs executing at once: two attempts of one run (a
// crashed worker's abandoned one and its reclaim) count once.
type holdTracker struct {
	e       *env
	mu      sync.Mutex
	runs    map[string]limitRun
	gates   map[string]*gate
	fail    map[string]int // run/step -> first attempts that fail
	snooze  map[string]bool
	tries   map[string]int // run/step -> attempts made
	running map[string]int // run -> handlers executing
	active  map[string]int // bucket -> runs executing
	peak    map[string]int
	order   []string // run name per attempt, in start order
	linger  time.Duration
}

func newHoldTracker(e *env, runs ...limitRun) *holdTracker {
	tr := &holdTracker{e: e, runs: map[string]limitRun{}, gates: map[string]*gate{}, fail: map[string]int{}, snooze: map[string]bool{},
		tries: map[string]int{}, running: map[string]int{}, active: map[string]int{}, peak: map[string]int{}}
	for _, r := range runs {
		tr.runs[r.name] = r
	}
	return tr
}

// gated makes the successful attempts of the named runs wait for open.
func (tr *holdTracker) gated(names ...string) {
	for _, n := range names {
		tr.gates[n] = newGate(tr.e.t)
	}
}

func (tr *holdTracker) open(name string) { tr.gates[name].open() }

// failFirst makes the first n attempts of step of run name fail.
func (tr *holdTracker) failFirst(name, step string, n int) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.fail[name+"/"+step] = n
}

func (tr *holdTracker) buckets(name string) []string {
	r := tr.runs[name]
	out := []string{"a=" + r.a}
	if r.b != "" {
		out = append(out, "b="+r.b)
	}
	return out
}

// handler is step's handler for every run of the case.
func (tr *holdTracker) handler(step string) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		name := string(in)
		tr.mu.Lock()
		tr.tries[name+"/"+step]++
		n := tr.tries[name+"/"+step]
		tr.order = append(tr.order, name)
		if tr.running[name]++; tr.running[name] == 1 {
			for _, b := range tr.buckets(name) {
				tr.active[b]++
				tr.peak[b] = max(tr.peak[b], tr.active[b])
			}
		}
		fail, snooze, g, linger := n <= tr.fail[name+"/"+step], n == 1 && tr.snooze[name], tr.gates[name], tr.linger
		tr.mu.Unlock()
		defer func() {
			tr.mu.Lock()
			if tr.running[name]--; tr.running[name] == 0 {
				for _, b := range tr.buckets(name) {
					tr.active[b]--
				}
			}
			tr.mu.Unlock()
		}()
		switch {
		case snooze:
			return nil, workflow.Snooze(2 * tr.e.tm.unit)
		case fail:
			return nil, errBoom
		}
		if linger > 0 {
			time.Sleep(linger)
		}
		if g != nil {
			if err := g.wait(ctx); err != nil {
				return nil, err
			}
		}
		return in, nil
	}
}

// attempts returns how many attempts runs name made, every step counted.
func (tr *holdTracker) attempts(name string) int {
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

func (tr *holdTracker) startOrder() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return strings.Join(tr.order, " ")
}

func (tr *holdTracker) peakOf(bucket string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.peak[bucket]
}

// assertPeak fails when bucket ever had more than limit runs executing.
func (tr *holdTracker) assertPeak(bucket string, limit int) {
	tr.e.t.Helper()
	if p := tr.peakOf(bucket); p > limit {
		tr.e.t.Fatalf("bucket %s had %d runs executing at once, want at most %d (attempts: %s)", bucket, p, limit, tr.startOrder())
	}
}

// assertAttempts fails unless run name made exactly want attempts.
func (tr *holdTracker) assertAttempts(name string, want int, why string) {
	tr.e.t.Helper()
	if got := tr.attempts(name); got != want {
		tr.e.t.Fatalf("run %s made %d attempts %s, want %d (attempts: %s)", name, got, why, want, tr.startOrder())
	}
}

// holdJob is r as a job of wf; the payload is the run's name.
func holdJob(wf string, r limitRun) workflow.Job { return limitJob(wf, r) }

// holdStep is step "a" of a hold case, retried up to three times after
// backoff.
func holdStep(tr *holdTracker, backoff time.Duration) workflow.Step {
	return workflow.Step{Name: "a", Run: tr.handler("a"), Retry: &workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: backoff}}}
}

// holdFlow is a workflow of steps capped at MaxRuns per value of label
// "a", holding its slot as hold says.
func holdFlow(e *env, hold workflow.ConcurrencyHold, max int, steps ...workflow.Step) workflow.Workflow {
	return workflow.Workflow{Name: e.name("hold"), Steps: steps,
		Concurrency: &workflow.Concurrency{Key: e.h.keyFromLabel(limitLabelA), MaxRuns: max, Hold: hold}}
}

// holdShape is where a case puts its HoldWhileRunning key: on
// Workflow.Concurrency or on a ConcurrencyLimits entry, which a driver may
// count apart.
type holdShape struct {
	name  string
	limit bool
}

var holdShapes = []holdShape{{"concurrency key", false}, {"concurrency limit", true}}

// holdFlowOn is holdFlow with its key placed as shape says.
func holdFlowOn(e *env, shape holdShape, steps ...workflow.Step) workflow.Workflow {
	if !shape.limit {
		return holdFlow(e, workflow.HoldWhileRunning, 1, steps...)
	}
	e.need(workflow.CapConcurrencyLimits)
	return workflow.Workflow{Name: e.name("hold"), Steps: steps,
		ConcurrencyLimits: []workflow.ConcurrencyLimit{{Key: e.h.keyFromLabel(limitLabelA), MaxRuns: 1, Hold: workflow.HoldWhileRunning}}}
}

// pass moves the case's time on by d: the fake clock, or real time.
func (e *env) pass(d time.Duration) {
	if e.clk != nil {
		e.clk.Advance(d)
		e.quiet()
		return
	}
	time.Sleep(d)
}

// cancelRun cancels ref and waits until it is cancelled.
func (e *env) cancelRun(ref workflow.RunRef) {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	if err := e.c.Cancel(ctx, ref); err != nil && !errors.Is(err, workflow.ErrNotFound) {
		e.t.Fatalf("Cancel: %v", err)
	}
	e.waitStatus(ref, workflow.StatusCancelled, 0)
}

// signal sends event name with key and payload, or fails the subtest.
func (e *env) signal(name, key string, payload []byte) {
	e.t.Helper()
	ctx, cancel := e.callCtx()
	defer cancel()
	if err := e.c.Signal(ctx, workflow.Event{Name: name, Key: key, Payload: payload}); err != nil {
		e.t.Fatalf("Signal: %v", err)
	}
}

// holdParksAndRetakes is the shape every "gives the slot back and takes
// it again" case shares: holder parks (park is how), next starts at once
// on the freed slot and holds it, holder's parked work becomes ready
// (wake) but does not run while next holds the key, and runs once next
// ends. holderBefore and holderAfter are holder's attempts before it
// parks and in all.
func holdParksAndRetakes(e *env, tr *holdTracker, wf workflow.Workflow, holder, next limitRun, holderBefore, holderAfter int, wake func()) {
	e.t.Helper()
	e.up(wf)
	h := e.enqueue(holdJob(wf.Name, holder))
	e.eventually("the holder to park", 0, func() bool { return tr.attempts(holder.name) >= holderBefore })
	n := e.enqueue(holdJob(wf.Name, next))
	e.eventually("the next run to start on the slot the parked holder gave back", 0, func() bool { return tr.attempts(next.name) > 0 })
	wake()
	e.quiet()
	e.quiet()
	tr.assertAttempts(holder.name, holderBefore, "while the next run held the key")
	tr.open(next.name)
	e.waitStatus(h, workflow.StatusSucceeded, e.tm.unit)
	e.waitStatus(n, workflow.StatusSucceeded, e.tm.unit)
	tr.assertAttempts(holder.name, holderAfter, "in all")
	tr.assertPeak("a=x", 1)
}

// concurrencyHoldCases is the conformance group for hold policies.
//
//ds:def id=workflowtest-concurrencyholdcases-t8w3vq5n owner=@khanakia stability=stable
var concurrencyHoldCases = []testCase{
	{"default hold: a run in its retry backoff keeps its slot", func(e *env) {
		// The rule of Concurrency since the first release: a run holds its
		// slot until it ends, retry backoffs included.
		e.need(workflow.CapsOf(workflow.CapConcurrencyKey, workflow.CapCancel))
		holder, next := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}
		tr := newHoldTracker(e, holder, next)
		tr.failFirst(holder.name, "a", 3)
		wf := holdFlow(e, "", 1, holdStep(tr, time.Hour))
		e.up(wf)
		h := e.enqueue(holdJob(wf.Name, holder))
		e.eventually("the holder's first attempt", 0, func() bool { return tr.attempts(holder.name) == 1 })
		n := e.enqueue(holdJob(wf.Name, next))
		e.quiet()
		e.quiet()
		tr.assertAttempts(next.name, 0, "while the holder waited on its retry backoff")
		e.cancelRun(h)
		e.waitStatus(n, workflow.StatusSucceeded, 0)
	}},
	{"while_running: a run in a long retry backoff does not block the next run on its key", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapHoldWhileRunning, workflow.CapCancel))
		holder, next := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}
		tr := newHoldTracker(e, holder, next)
		tr.failFirst(holder.name, "a", 3)
		wf := holdFlow(e, workflow.HoldWhileRunning, 1, holdStep(tr, time.Hour))
		e.up(wf)
		h := e.enqueue(holdJob(wf.Name, holder))
		e.eventually("the holder's first attempt", 0, func() bool { return tr.attempts(holder.name) == 1 })
		n := e.enqueue(holdJob(wf.Name, next))
		e.waitStatus(n, workflow.StatusSucceeded, 0) // promptly: no time passes
		tr.assertAttempts(holder.name, 1, "with its second attempt an hour away")
		if st := e.status(h); st.IsTerminal() {
			e.t.Fatalf("holder ended %q while in its backoff", st)
		}
		e.cancelRun(h)
	}},
	{"while_running: a parked run takes its slot back only when the key has room, at its original place", func(e *env) {
		e.need(workflow.CapHoldWhileRunning)
		// holder parks in a short backoff; next takes the slot; later is
		// enqueued after holder and waits. When holder's backoff ends the
		// key is still next's, so holder waits too. When next ends, holder
		// (enqueued before later) goes first: it rejoins at its original
		// place, not behind later.
		holder, next, later := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}, limitRun{"later", "x", ""}
		tr := newHoldTracker(e, holder, next, later)
		tr.failFirst(holder.name, "a", 1)
		tr.gated(next.name, later.name)
		wf := holdFlow(e, workflow.HoldWhileRunning, 1, holdStep(tr, e.tm.unit))
		e.up(wf)
		h := e.enqueue(holdJob(wf.Name, holder))
		e.eventually("the holder's first attempt", 0, func() bool { return tr.attempts(holder.name) == 1 })
		n := e.enqueue(holdJob(wf.Name, next))
		e.eventually("next to start on the slot holder gave back", 0, func() bool { return tr.attempts(next.name) == 1 })
		l := e.enqueue(holdJob(wf.Name, later))
		e.pass(2 * e.tm.unit) // holder's backoff ends; the key is next's
		e.quiet()
		tr.assertAttempts(holder.name, 1, "while next held the only slot")
		tr.assertAttempts(later.name, 0, "while next held the only slot")
		tr.open(next.name)
		e.waitStatus(h, workflow.StatusSucceeded, e.tm.unit)
		e.waitStatus(n, workflow.StatusSucceeded, 0)
		tr.open(later.name)
		e.waitStatus(l, workflow.StatusSucceeded, 0)
		if got, want := tr.startOrder(), "holder next holder later"; got != want {
			e.t.Fatalf("order of attempts:\n got %s\nwant %s", got, want)
		}
		tr.assertPeak("a=x", 1)
	}},
	{"while_running: under load on several workers a key never runs more than MaxRuns", func(e *env) {
		e.need(workflow.CapHoldWhileRunning)
		const runs, maxRuns = 12, 2
		var all []limitRun
		for i := range runs {
			all = append(all, limitRun{"r" + string(rune('a'+i)), "x", ""})
		}
		tr := newHoldTracker(e, all...)
		tr.linger = 10 * time.Millisecond // successful attempts overlap
		for _, r := range all {
			tr.failFirst(r.name, "a", 1)
		}
		wf := holdFlow(e, workflow.HoldWhileRunning, maxRuns, holdStep(tr, e.tm.unit))
		if e.h.NewWithQueues != nil {
			e.withQueues(workflow.QueueConfig{Name: workflow.DefaultQueue, Workers: 4})
		}
		e.up(wf)
		if e.peer != nil {
			e.startPeer(e.peer(e.t), wf) // a second node over the same store
		}
		var refs []workflow.RunRef
		for _, r := range all {
			refs = append(refs, e.enqueue(holdJob(wf.Name, r)))
		}
		for _, ref := range refs {
			e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit/4)
		}
		tr.assertPeak("a=x", maxRuns)
		for _, r := range all {
			tr.assertAttempts(r.name, 2, "(one failure, one success)")
		}
	}},
	{"while_running: a snoozed run gives its slot back and takes it again", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapHoldWhileRunning, workflow.CapSnooze))
		holder, next := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}
		tr := newHoldTracker(e, holder, next)
		tr.snooze[holder.name] = true
		tr.gated(next.name)
		wf := holdFlow(e, workflow.HoldWhileRunning, 1, holdStep(tr, e.tm.unit))
		holdParksAndRetakes(e, tr, wf, holder, next, 1, 2, func() { e.pass(3 * e.tm.unit) })
	}},
	{"while_running: a run in a durable sleep gives its slot back and takes it again", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapHoldWhileRunning, workflow.CapDurableSleep))
		holder, next := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}
		tr := newHoldTracker(e, holder, next)
		tr.gated(next.name)
		wf := holdFlow(e, workflow.HoldWhileRunning, 1,
			workflow.Step{Name: "a", Run: tr.handler("a")},
			workflow.Step{Name: "nap", After: []string{"a"}, Sleep: 2 * e.tm.unit},
			workflow.Step{Name: "b", After: []string{"nap"}, Run: tr.handler("b")})
		holdParksAndRetakes(e, tr, wf, holder, next, 1, 2, func() { e.pass(3 * e.tm.unit) })
	}},
	{"while_running: a run waiting for an event gives its slot back and takes it again", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapHoldWhileRunning, workflow.CapWaitForEvent))
		holder, next := limitRun{"holder", "x", "h"}, limitRun{"next", "x", "n"}
		tr := newHoldTracker(e, holder, next)
		tr.gated(next.name)
		event := e.name("ev")
		wf := holdFlow(e, workflow.HoldWhileRunning, 1,
			workflow.Step{Name: "a", Run: tr.handler("a")},
			workflow.Step{Name: "w", After: []string{"a"}, Wait: &workflow.WaitSpec{Event: event, KeyFrom: e.h.waitKeyFromLabel(limitLabelB)}},
			workflow.Step{Name: "b", After: []string{"w"}, Run: tr.handler("b")})
		// Both runs wait on their own key ("h", "n") under label b; the
		// tracker buckets them by label a only, so give them no b bucket.
		tr.runs[holder.name] = limitRun{holder.name, "x", ""}
		tr.runs[next.name] = limitRun{next.name, "x", ""}
		e.up(wf)
		h := e.enqueue(holdJob(wf.Name, holder))
		e.eventually("the holder to reach its wait", 0, func() bool { return tr.attempts(holder.name) == 1 })
		n := e.enqueue(holdJob(wf.Name, next))
		e.eventually("next to start on the slot the waiting holder gave back", 0, func() bool { return tr.attempts(next.name) == 1 })
		e.signal(event, holder.b, []byte(holder.name))
		e.quiet()
		e.quiet()
		tr.assertAttempts(holder.name, 1, "after its event while next held the key")
		tr.open(next.name) // next reaches its own wait and parks
		e.waitStatus(h, workflow.StatusSucceeded, 0)
		e.signal(event, next.b, []byte(next.name))
		e.waitStatus(n, workflow.StatusSucceeded, 0)
		tr.assertAttempts(holder.name, 2, "in all")
		tr.assertPeak("a=x", 1)
	}},
	{"while_running: two keys with different hold policies on one workflow", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapHoldWhileRunning, workflow.CapConcurrencyLimits, workflow.CapCancel))
		// Limit a holds for the whole run, limit b only while running: a
		// parked holder keeps a=x and gives back b=p.
		holder, onA, onB := limitRun{"holder", "x", "p"}, limitRun{"on-a", "x", "q"}, limitRun{"on-b", "y", "p"}
		tr := newHoldTracker(e, holder, onA, onB)
		tr.failFirst(holder.name, "a", 3)
		wf := workflow.Workflow{Name: e.name("hold"), Steps: []workflow.Step{holdStep(tr, time.Hour)},
			ConcurrencyLimits: []workflow.ConcurrencyLimit{
				{Key: e.h.keyFromLabel(limitLabelA), MaxRuns: 1, Hold: workflow.HoldWholeRun},
				{Key: e.h.keyFromLabel(limitLabelB), MaxRuns: 1, Hold: workflow.HoldWhileRunning},
			}}
		e.up(wf)
		h := e.enqueue(holdJob(wf.Name, holder))
		e.eventually("the holder's first attempt", 0, func() bool { return tr.attempts(holder.name) == 1 })
		a := e.enqueue(holdJob(wf.Name, onA))
		b := e.enqueue(holdJob(wf.Name, onB))
		e.waitStatus(b, workflow.StatusSucceeded, 0)
		tr.assertAttempts(onA.name, 0, "while the parked holder kept a=x (HoldWholeRun)")
		e.cancelRun(h)
		e.waitStatus(a, workflow.StatusSucceeded, 0)
	}},
	{"while_running: parallel steps keep the slot while any step executes", func(e *env) {
		e.need(workflow.CapHoldWhileRunning)
		holder, next := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}
		tr := newHoldTracker(e, holder, next)
		slow := newGate(e.t)
		tr.failFirst(holder.name, "flaky", 1)
		slowStep := func(ctx context.Context, in []byte) ([]byte, error) {
			if string(in) == holder.name {
				if err := slow.wait(ctx); err != nil {
					return nil, err
				}
			}
			return tr.handler("slow")(ctx, in)
		}
		wf := holdFlow(e, workflow.HoldWhileRunning, 1,
			workflow.Step{Name: "slow", Run: slowStep},
			workflow.Step{Name: "flaky", Run: tr.handler("flaky"), Retry: &workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: e.tm.unit}}})
		e.up(wf)
		h := e.enqueue(holdJob(wf.Name, holder))
		e.eventually("the holder's flaky step to fail into its backoff", 0, func() bool { return tr.attempts(holder.name) == 1 })
		n := e.enqueue(holdJob(wf.Name, next))
		e.quiet()
		e.quiet()
		tr.assertAttempts(next.name, 0, "while the holder's slow step still executed")
		slow.open() // only the backoff is left: the holder parks
		e.waitStatus(n, workflow.StatusSucceeded, 0)
		e.waitStatus(h, workflow.StatusSucceeded, e.tm.unit)
	}},
	{"while_running: a run cancelled while parked gives nothing back twice (concurrency key)", func(e *env) { holdCancelParked(e, holdShapes[0]) }},
	{"while_running: a run cancelled while parked gives nothing back twice (concurrency limit)", func(e *env) { holdCancelParked(e, holdShapes[1]) }},
	{"while_running: a run that fails while parked gives nothing back twice (concurrency key)", func(e *env) { holdFailParked(e, holdShapes[0]) }},
	{"while_running: a run that fails while parked gives nothing back twice (concurrency limit)", func(e *env) { holdFailParked(e, holdShapes[1]) }},
	{"while_running: a worker crash while a run is parked leaves the counts right", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapHoldWhileRunning, workflow.CapDurable))
		if e.crash == nil {
			e.t.Skip("Instance.Crash is nil")
		}
		holder, next, third := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}, limitRun{"third", "x", ""}
		tr := newHoldTracker(e, holder, next, third)
		tr.failFirst(holder.name, "a", 1)
		tr.gated(third.name)
		wf := holdFlow(e, workflow.HoldWhileRunning, 1, holdStep(tr, 2*e.tm.unit))
		e.up(wf)
		h := e.enqueue(holdJob(wf.Name, holder))
		e.eventually("the holder's first attempt", 0, func() bool { return tr.attempts(holder.name) == 1 })
		n := e.enqueue(holdJob(wf.Name, next))
		e.waitStatus(n, workflow.StatusSucceeded, 0)
		e.doCrash() // holder is parked: nothing of it runs
		t3 := e.enqueue(holdJob(wf.Name, third))
		e.eventually("third to start on the free key", 0, func() bool { return tr.attempts(third.name) == 1 })
		e.pass(3 * e.tm.unit) // holder's backoff ends while third holds the key
		e.quiet()
		tr.assertAttempts(holder.name, 1, "while third held the only slot after the crash")
		tr.open(third.name)
		e.waitStatus(h, workflow.StatusSucceeded, e.tm.unit)
		e.waitStatus(t3, workflow.StatusSucceeded, 0)
		tr.assertPeak("a=x", 1)
	}},
	{"while_running: a worker crash right after a parked run took its slot back keeps the slot through the lease reclaim", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapHoldWhileRunning, workflow.CapDurable))
		if e.crash == nil {
			e.t.Skip("Instance.Crash is nil")
		}
		holder, next, third := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}, limitRun{"third", "x", ""}
		tr := newHoldTracker(e, holder, next, third)
		tr.failFirst(holder.name, "a", 1)
		tr.gated(holder.name, third.name) // holder's second attempt blocks
		wf := holdFlow(e, workflow.HoldWhileRunning, 1, holdStep(tr, e.tm.unit))
		e.up(wf)
		h := e.enqueue(holdJob(wf.Name, holder))
		e.eventually("the holder's first attempt", 0, func() bool { return tr.attempts(holder.name) == 1 })
		n := e.enqueue(holdJob(wf.Name, next))
		e.waitStatus(n, workflow.StatusSucceeded, 0)
		e.eventually("the holder to take its slot back and start its second attempt", e.tm.unit/4, func() bool { return tr.attempts(holder.name) == 2 })
		e.doCrash() // the second attempt's worker dies; its lease must be reclaimed
		t3 := e.enqueue(holdJob(wf.Name, third))
		e.eventually("the holder's attempt to be reclaimed by the new worker", 0, func() bool { return tr.attempts(holder.name) >= 3 })
		tr.assertAttempts(third.name, 0, "while the holder's reclaimed attempt held the slot")
		tr.open(holder.name)
		e.waitStatus(h, workflow.StatusSucceeded, 0)
		tr.open(third.name)
		e.waitStatus(t3, workflow.StatusSucceeded, 0)
		tr.assertPeak("a=x", 1)
	}},
}

// holdCancelParked checks that a run cancelled while parked does not give
// back again the slot it gave back when it parked: a third run must still
// wait for the run that took that slot.
func holdCancelParked(e *env, shape holdShape) {
	e.need(workflow.CapsOf(workflow.CapHoldWhileRunning, workflow.CapCancel))
	holder, next, third := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}, limitRun{"third", "x", ""}
	tr := newHoldTracker(e, holder, next, third)
	tr.failFirst(holder.name, "a", 3)
	tr.gated(next.name, third.name)
	wf := holdFlowOn(e, shape, holdStep(tr, time.Hour))
	e.up(wf)
	h := e.enqueue(holdJob(wf.Name, holder))
	e.eventually("the holder's first attempt", 0, func() bool { return tr.attempts(holder.name) == 1 })
	n := e.enqueue(holdJob(wf.Name, next))
	e.eventually("next to start", 0, func() bool { return tr.attempts(next.name) == 1 })
	e.cancelRun(h)
	t3 := e.enqueue(holdJob(wf.Name, third))
	e.quiet()
	e.quiet()
	tr.assertAttempts(third.name, 0, "while next held the only slot (a parked run's end must not free a slot it gave back)")
	tr.open(next.name)
	e.waitStatus(n, workflow.StatusSucceeded, 0)
	tr.open(third.name)
	e.waitStatus(t3, workflow.StatusSucceeded, 0)
	tr.assertPeak("a=x", 1)
}

// holdFailParked is holdCancelParked for a run that fails while parked (its
// wait times out).
func holdFailParked(e *env, shape holdShape) {
	e.need(workflow.CapsOf(workflow.CapHoldWhileRunning, workflow.CapWaitForEvent))
	holder, next, third := limitRun{"holder", "x", "h"}, limitRun{"next", "x", "n"}, limitRun{"third", "x", "t"}
	tr := newHoldTracker(e, holder, next, third)
	tr.gated(next.name, third.name)
	wf := holdFlowOn(e, shape,
		workflow.Step{Name: "a", Run: tr.handler("a")},
		workflow.Step{Name: "w", After: []string{"a"}, Wait: &workflow.WaitSpec{Event: e.name("ev"), KeyFrom: e.h.waitKeyFromLabel(limitLabelB), Timeout: e.tm.unit}})
	for _, r := range []limitRun{holder, next, third} {
		tr.runs[r.name] = limitRun{r.name, "x", ""}
	}
	e.up(wf)
	h := e.enqueue(holdJob(wf.Name, holder))
	e.eventually("the holder to reach its wait", 0, func() bool { return tr.attempts(holder.name) == 1 })
	n := e.enqueue(holdJob(wf.Name, next))
	e.eventually("next to start", 0, func() bool { return tr.attempts(next.name) == 1 })
	e.waitStatus(h, workflow.StatusFailed, e.tm.unit/4) // the wait times out
	t3 := e.enqueue(holdJob(wf.Name, third))
	e.quiet()
	e.quiet()
	tr.assertAttempts(third.name, 0, "while next held the only slot (a parked run's failure must not free a slot it gave back)")
	tr.open(next.name)
	tr.open(third.name)
	e.eventually("third to start once next parked in its own wait", e.tm.unit/4, func() bool { return tr.attempts(third.name) == 1 })
	tr.assertPeak("a=x", 1)
	e.waitTerminal(n, e.tm.unit/4)
	e.waitTerminal(t3, e.tm.unit/4)
}
