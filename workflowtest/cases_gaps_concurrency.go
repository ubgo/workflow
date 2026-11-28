package workflowtest

import (
	"context"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// How schedule idempotency keys, step checkpoints, CompleteInTx and
// coalescing hold up on workflows with several concurrency keys
// (Workflow.ConcurrencyLimits) and hold policies (HoldWhileRunning). Each
// slice joins its feature's group in gapGroups.

// coalesceConcurrencyCases: a run pending behind a coalesced key is
// admitted like any run once its holder ends.
var coalesceConcurrencyCases = []testCase{
	{"the pending run goes through concurrency limits when its holder ends", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapCoalesce, workflow.CapConcurrencyLimits))
		// holder runs and holds key K and bucket a=x; blocker waits for
		// a=x under another key; next is stored pending behind holder.
		// When holder ends, next is promoted, but blocker came first and
		// takes a=x: next must wait for it, never running beside it.
		holder, blocker, next := limitRun{"holder", "x", "h"}, limitRun{"blocker", "x", "k"}, limitRun{"next", "x", "n"}
		tr := newLimitTracker(e, holder, blocker, next)
		wf := limitsWorkflow(e, tr, 1, 1)
		wf.Coalesce = &workflow.Coalesce{}
		e.up(wf)
		key := e.name("sync")
		withKey := func(r limitRun, k string) workflow.Job {
			j := limitJob(wf.Name, r)
			j.IdempotencyKey = k
			return j
		}
		h := e.enqueue(withKey(holder, key))
		e.eventually("the holder to start", 0, func() bool { return tr.started(holder.name) })
		b := e.enqueue(withKey(blocker, e.name("other")))
		n := e.enqueue(withKey(next, key))
		if sameRef(n, h) {
			e.t.Fatalf("an enqueue while the holder runs returned the holder; want a pending run")
		}
		e.quiet()
		assertNotStarted(e, tr, "while the holder held a=x", blocker.name, next.name)
		tr.open(holder.name)
		e.waitStatus(h, workflow.StatusSucceeded, 0)
		e.eventually("the blocker to take a=x", 0, func() bool { return tr.started(blocker.name) })
		e.quiet()
		assertNotStarted(e, tr, "while the blocker held a=x", next.name)
		if st := e.status(n); st.IsTerminal() {
			e.t.Fatalf("the promoted run ended %q while it waited for a=x", st)
		}
		tr.open(blocker.name)
		tr.open(next.name)
		e.waitStatus(b, workflow.StatusSucceeded, 0)
		e.waitStatus(n, workflow.StatusSucceeded, 0)
		if p := tr.peakOf("a=x"); p > 1 {
			e.t.Fatalf("bucket a=x had %d runs at once, want 1 (starts: %v)", p, tr.startOrder())
		}
	}},
	{"CoalesceKeepLast admits the pending run under the keys of its newest payload (concurrency key)", func(e *env) {
		coalesceKeepLastKeys(e, holdShapes[0])
	}},
	{"CoalesceKeepLast admits the pending run under the keys of its newest payload (concurrency limit)", func(e *env) {
		coalesceKeepLastKeys(e, holdShapes[1])
	}},
}

// coalesceKeepLastKeys keys a workflow's single slot on the first byte of
// the payload: x or y. holder ("x-holder") runs under key K; occupant
// ("y-occupant") holds bucket y under another key. The run pending
// behind K is enqueued with a y payload and then folded to an x payload:
// once holder ends it must run on the free bucket x while occupant still
// holds y.
func coalesceKeepLastKeys(e *env, shape holdShape) {
	e.t.Helper()
	e.need(workflow.CapsOf(workflow.CapCoalesce, workflow.CapConcurrencyKey))
	byFirst := workflow.KeyFunc(func(j workflow.Job) string { return string(j.Payload[:1]) })
	gates := map[string]*gate{"x-holder": newGate(e.t), "y-occupant": newGate(e.t)}
	var mu sync.Mutex
	var ran []string
	wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
		mu.Lock()
		ran = append(ran, string(in))
		mu.Unlock()
		if g := gates[string(in)]; g != nil {
			if err := g.wait(ctx); err != nil {
				return nil, err
			}
		}
		return in, nil
	})
	wf.Retry = *workflow.RetriesNone()
	wf.Coalesce = &workflow.Coalesce{Payload: workflow.CoalesceKeepLast}
	if shape.limit {
		e.need(workflow.CapConcurrencyLimits)
		wf.ConcurrencyLimits = []workflow.ConcurrencyLimit{{Key: byFirst, MaxRuns: 1}}
	} else {
		wf.Concurrency = &workflow.Concurrency{Key: byFirst, MaxRuns: 1}
	}
	e.up(wf)
	key := e.name("k")
	h := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key, Payload: []byte("x-holder")})
	o := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: e.name("o"), Payload: []byte("y-occupant")})
	started := func(p string) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range ran {
			if r == p {
				return true
			}
		}
		return false
	}
	e.eventually("the holder and the occupant to start", 0, func() bool { return started("x-holder") && started("y-occupant") })
	n := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key, Payload: []byte("y-first")})
	if folded := e.enqueue(workflow.Job{Workflow: wf.Name, IdempotencyKey: key, Payload: []byte("x-last")}); !sameRef(folded, n) {
		e.t.Fatalf("the second enqueue behind the holder returned %+v, want the pending run %+v", folded, n)
	}
	gates["x-holder"].open()
	e.waitStatus(h, workflow.StatusSucceeded, 0)
	e.waitStatus(n, workflow.StatusSucceeded, 0) // bucket x is free; y is still the occupant's
	if st := e.status(o); st.IsTerminal() {
		e.t.Fatalf("the occupant ended %q before its gate opened", st)
	}
	gates["y-occupant"].open()
	e.waitStatus(o, workflow.StatusSucceeded, 0)
	mu.Lock()
	defer mu.Unlock()
	for _, r := range ran {
		if r == "y-first" {
			e.t.Fatalf("the pending run ran its first payload; runs: %q", ran)
		}
	}
}

// checkpointConcurrencyCases: a checkpoint survives a run that parks
// under HoldWhileRunning.
var checkpointConcurrencyCases = []testCase{
	{"a checkpoint saved before a HoldWhileRunning park is read back after the run takes its slot again (concurrency key)", func(e *env) {
		checkpointAcrossPark(e, holdShapes[0])
	}},
	{"a checkpoint saved before a HoldWhileRunning park is read back after the run takes its slot again (concurrency limit)", func(e *env) {
		checkpointAcrossPark(e, holdShapes[1])
	}},
}

// checkpointAcrossPark: holder's first attempt saves a checkpoint and
// fails into a retry backoff, so the run parks and gives its slot back;
// next takes the slot and runs; holder's retry then takes the slot again
// and must load what its first attempt saved.
func checkpointAcrossPark(e *env, shape holdShape) {
	e.t.Helper()
	e.need(workflow.CapsOf(workflow.CapCheckpoint, workflow.CapHoldWhileRunning))
	holder, next := limitRun{"holder", "x", ""}, limitRun{"next", "x", ""}
	tr := newHoldTracker(e, holder, next)
	tr.gated(next.name)
	var l cpLog
	var mu sync.Mutex
	var order []string
	inner := tr.handler("a")
	step := workflow.Step{Name: "a", Retry: &workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: e.tm.unit, Factor: 1}},
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			mu.Lock()
			order = append(order, string(in))
			mu.Unlock()
			if string(in) != holder.name {
				return inner(ctx, in)
			}
			l.load(ctx)
			if tr.attempts(holder.name) == 0 {
				if err := workflow.SaveCheckpoint(ctx, []byte("page-7")); err != nil {
					return nil, workflow.Permanent(err)
				}
				_, _ = inner(ctx, in) // counted as an attempt
				return nil, errBoom
			}
			return inner(ctx, in)
		}}
	wf := holdFlowOn(e, shape, step)
	e.up(wf)
	h := e.enqueue(holdJob(wf.Name, holder))
	e.eventually("the holder's first attempt", 0, func() bool { return tr.attempts(holder.name) == 1 })
	n := e.enqueue(holdJob(wf.Name, next))
	e.eventually("next to start on the slot the parked holder gave back", 0, func() bool { return tr.attempts(next.name) == 1 })
	tr.open(next.name)
	e.waitStatus(n, workflow.StatusSucceeded, 0)
	e.waitStatus(h, workflow.StatusSucceeded, e.tm.unit)
	if got := l.got(); len(got) != 2 || got[0] != "" || got[1] != "page-7" {
		e.t.Fatalf("the holder's attempts loaded %q, want [\"\" \"page-7\"]", got)
	}
	mu.Lock()
	got := order
	mu.Unlock()
	if len(got) != 3 || got[0] != holder.name || got[1] != next.name || got[2] != holder.name {
		e.t.Fatalf("order of attempts %q, want holder, next, holder: the holder must park between its attempts", got)
	}
	tr.assertPeak("a=x", 1)
}

// completeInTxConcurrencyCases: a run completed inside the caller's unit of
// work gives back every concurrency slot it holds.
var completeInTxConcurrencyCases = []testCase{
	{"a run completed by CompleteInTx gives back its concurrency key and every limit", func(e *env) {
		completeInTxReleases(e, workflow.HoldWholeRun)
	}},
	{"a run completed by CompleteInTx gives back its HoldWhileRunning key and every limit", func(e *env) {
		e.need(workflow.CapHoldWhileRunning)
		completeInTxReleases(e, workflow.HoldWhileRunning)
	}},
}

// completeInTxReleases: holder (a=x, b=y) completes its only step with
// CompleteInTx; waitA needs a=x and waitB needs b=y. Both wait while the
// holder runs and both start once its completion committed.
func completeInTxReleases(e *env, hold workflow.ConcurrencyHold) {
	e.t.Helper()
	tc := e.txCompleter()
	e.need(workflow.CapsOf(workflow.CapConcurrencyKey, workflow.CapConcurrencyLimits))
	holder, waitA, waitB := limitRun{"holder", "x", "y"}, limitRun{"waitA", "x", "p"}, limitRun{"waitB", "q", "y"}
	tr := newLimitTracker(e, holder, waitA, waitB)
	tr.open(waitA.name)
	tr.open(waitB.name)
	inner := tr.handler()
	wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
		out, err := inner(ctx, in)
		if err != nil || string(in) != holder.name {
			return out, err
		}
		if err := tc.CompleteInTx(ctx, []byte(`"done"`), func(context.Context) error { return nil }); err != nil {
			return nil, workflow.Permanent(err)
		}
		return nil, errBoom // ignored: the step is already complete
	})
	wf.Steps[0].Retry = workflow.RetriesNone()
	wf.Concurrency = &workflow.Concurrency{Key: e.h.keyFromLabel(limitLabelA), MaxRuns: 1, Hold: hold}
	wf.ConcurrencyLimits = []workflow.ConcurrencyLimit{{Key: e.h.keyFromLabel(limitLabelB), MaxRuns: 1, Hold: hold}}
	e.up(wf)
	h := e.enqueue(limitJob(wf.Name, holder))
	e.eventually("the holder to start", 0, func() bool { return tr.started(holder.name) })
	a := e.enqueue(limitJob(wf.Name, waitA))
	b := e.enqueue(limitJob(wf.Name, waitB))
	e.quiet()
	assertNotStarted(e, tr, "while the holder held a=x and b=y", waitA.name, waitB.name)
	tr.open(holder.name)
	e.waitStatus(h, workflow.StatusSucceeded, 0)
	e.waitStatus(a, workflow.StatusSucceeded, 0)
	e.waitStatus(b, workflow.StatusSucceeded, 0)
	if out, ok := e.resultOf(h); ok && string(out) != `"done"` {
		e.t.Errorf("the holder's output = %s, want the completed output", out)
	}
}

// scheduleKeyConcurrencyCases: a keyed schedule on a workflow with several
// concurrency keys.
var scheduleKeyConcurrencyCases = []testCase{
	{"a keyed fire waiting for a concurrency limit is the one run its key names", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapCron, workflow.CapScheduleTrigger, workflow.CapScheduleKey, workflow.CapConcurrencyLimits))
		// blocker holds a=x; the schedule's fires carry a=x too, so the
		// first trigger's run waits for a=x. A second trigger and a hand
		// enqueue under the key name that waiting run; it runs once, after
		// blocker, never beside it.
		blocker, fired := limitRun{"blocker", "x", "k"}, limitRun{"fired", "x", "f"}
		tr := newLimitTracker(e, blocker, fired)
		tr.open(fired.name)
		wf := limitsWorkflow(e, tr, 1, 1)
		e.up(wf)
		b := e.enqueue(limitJob(wf.Name, blocker))
		e.eventually("the blocker to start", 0, func() bool { return tr.started(blocker.name) })
		key := e.name("sync")
		id := e.schedule(workflow.Schedule{Name: e.name("s"), Workflow: wf.Name, Cron: farCron, Location: time.UTC,
			Input: []byte(fired.name), Labels: map[string]string{limitLabelA: fired.a, limitLabelB: fired.b}, IdempotencyKey: key})
		first := e.mustTrigger(id)
		again, out, err := e.triggerWithOutcome(id)
		if err != nil {
			e.t.Fatalf("a trigger while the key's run waits for a=x: %v, want that run", err)
		}
		if !sameRef(again, first) || (out.Reported && !out.Duplicate) {
			e.t.Fatalf("a second trigger returned %+v (%+v), want the waiting run %+v as a duplicate", again, out, first)
		}
		byHand := limitJob(wf.Name, fired)
		byHand.IdempotencyKey = key
		if ref := e.enqueue(byHand); !sameRef(ref, first) {
			e.t.Fatalf("a hand enqueue under the schedule's key returned %+v, want the waiting run %+v", ref, first)
		}
		e.quiet()
		assertNotStarted(e, tr, "while the blocker held a=x", fired.name)
		tr.open(blocker.name)
		e.waitStatus(b, workflow.StatusSucceeded, 0)
		e.waitStatus(first, workflow.StatusSucceeded, 0)
		e.quiet()
		if n := tr.starts(fired.name); n != 1 {
			e.t.Fatalf("the keyed run started %d times, want 1 (starts: %v)", n, tr.startOrder())
		}
		if p := tr.peakOf("a=x"); p > 1 {
			e.t.Fatalf("bucket a=x had %d runs at once, want 1 (starts: %v)", p, tr.startOrder())
		}
	}},
}
