package workflowtest

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/ubgo/workflow"
)

// collector records RunEvents, plus the Status the observer read back
// while handling each one.
type collector struct {
	mu     sync.Mutex
	c      workflow.Client
	events []workflow.RunEvent
	seen   []workflow.Status // Status(ev.Ref) read inside the observer
	errs   []error
}

func (col *collector) observe(ctx context.Context, ev workflow.RunEvent) {
	st, err := col.c.Status(ctx, ev.Ref)
	col.mu.Lock()
	defer col.mu.Unlock()
	col.events = append(col.events, ev)
	col.seen = append(col.seen, st)
	col.errs = append(col.errs, err)
}

// of returns the events for ref, matched on RunID or ScheduledID, with
// the Status read back for each. Delivery is at least once, so there may
// be several; every one must agree.
func (col *collector) of(ref workflow.RunRef) (evs []workflow.RunEvent, seen []workflow.Status, errs []error) {
	col.mu.Lock()
	defer col.mu.Unlock()
	for i, ev := range col.events {
		if (!ref.RunID.IsZero() && ev.Ref.RunID == ref.RunID) || (!ref.ScheduledID.IsZero() && ev.Ref.ScheduledID == ref.ScheduledID) {
			evs = append(evs, ev)
			seen = append(seen, col.seen[i])
			errs = append(errs, col.errs[i])
		}
	}
	return evs, seen, errs
}

// notifierOf skips unless the client implements workflow.RunNotifier, and
// fails if it claims CapRunFinished without implementing it.
func notifierOf(e *env) workflow.RunNotifier {
	e.t.Helper()
	rn, ok := e.c.(workflow.RunNotifier)
	if !ok {
		if e.c.Capabilities().Has(workflow.CapRunFinished) {
			e.t.Fatalf("driver claims CapRunFinished but %T does not implement workflow.RunNotifier", e.c)
		}
		e.t.Skip("driver does not implement workflow.RunNotifier")
	}
	return rn
}

// observeWith registers a collector on rn, or fails the subtest.
func observeWith(e *env, rn workflow.RunNotifier) *collector {
	e.t.Helper()
	col := &collector{c: e.c}
	if err := rn.OnRunFinished(col.observe); err != nil {
		e.t.Fatalf("OnRunFinished: %v", err)
	}
	return col
}

// expectEnd waits for ref's event and checks every delivery of it: the
// terminal status want, the workflow name, the labels, a non-zero
// FinishedAt, an error that check accepts, and a Status read inside the
// observer that already equals want.
func expectEnd(e *env, col *collector, ref workflow.RunRef, wf string, want workflow.Status, labels map[string]string, check func(error) string) {
	e.t.Helper()
	e.eventually("the run-finished event of a "+string(want)+" run", 0, func() bool {
		evs, _, _ := col.of(ref)
		return len(evs) > 0
	})
	evs, seen, errs := col.of(ref)
	for i, ev := range evs {
		if ev.Status != want {
			e.t.Errorf("event %d: Status = %q, want %q", i, ev.Status, want)
		}
		if ev.Workflow != wf {
			e.t.Errorf("event %d: Workflow = %q, want %q", i, ev.Workflow, wf)
		}
		if !ref.RunID.IsZero() && ev.Ref.RunID != ref.RunID {
			e.t.Errorf("event %d: Ref = %+v, want RunID %q", i, ev.Ref, ref.RunID)
		}
		if !ref.ScheduledID.IsZero() && ev.Ref.ScheduledID != ref.ScheduledID {
			e.t.Errorf("event %d: Ref = %+v, want ScheduledID %q", i, ev.Ref, ref.ScheduledID)
		}
		for k, v := range labels {
			if ev.Labels[k] != v {
				e.t.Errorf("event %d: Labels = %v, want %s=%s", i, ev.Labels, k, v)
			}
		}
		if ev.FinishedAt.IsZero() {
			e.t.Errorf("event %d: FinishedAt is zero", i)
		}
		if msg := check(ev.Err); msg != "" {
			e.t.Errorf("event %d: Err = %v: %s", i, ev.Err, msg)
		}
		if errs[i] != nil || seen[i] != want {
			e.t.Errorf("event %d: Status inside the observer = %q, %v; want %q", i, seen[i], errs[i], want)
		}
	}
}

func noErr(err error) string {
	if err != nil {
		return "want nil for a succeeded run"
	}
	return ""
}

func boomErr(err error) string {
	if err == nil || !strings.Contains(err.Error(), errBoom.Error()) {
		return "want the step's error"
	}
	return ""
}

func cancelledErr(err error) string {
	if !errors.Is(err, workflow.ErrCancelled) {
		return "want it to match workflow.ErrCancelled"
	}
	return ""
}

var hookCases = []testCase{
	{"succeeded, failed and cancelled runs are reported with status, labels and error", func(e *env) {
		col := observeWith(e, notifierOf(e))
		var ok, bad, slow, ended counter
		wfOK := oneStep(e.name("ok"), ok.echo())
		wfBad := oneStep(e.name("bad"), bad.fail(errBoom))
		wfSlow := oneStep(e.name("slow"), blockUntilDone(&slow, &ended))
		e.up(wfOK, wfBad, wfSlow)
		labels := map[string]string{"tenant": "t1"}
		okRef := e.enqueue(workflow.Job{Workflow: wfOK.Name, Labels: labels})
		badRef := e.enqueue(workflow.Job{Workflow: wfBad.Name, Labels: labels})
		expectEnd(e, col, okRef, wfOK.Name, workflow.StatusSucceeded, labels, noErr)
		expectEnd(e, col, badRef, wfBad.Name, workflow.StatusFailed, labels, boomErr)
		if !e.c.Capabilities().Has(workflow.CapCancel) {
			return
		}
		slowRef := e.enqueue(workflow.Job{Workflow: wfSlow.Name, Labels: labels})
		e.eventually("the step to start", 0, func() bool { return slow.count() > 0 })
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Cancel(ctx, slowRef); err != nil {
			e.t.Fatalf("Cancel: %v", err)
		}
		expectEnd(e, col, slowRef, wfSlow.Name, workflow.StatusCancelled, labels, cancelledErr)
	}},
	{"OnRunFinished after Start is ErrAlreadyStarted", func(e *env) {
		rn := notifierOf(e)
		var c counter
		e.up(oneStep(e.name("w"), c.echo()))
		if err := rn.OnRunFinished(func(context.Context, workflow.RunEvent) {}); !errors.Is(err, workflow.ErrAlreadyStarted) {
			e.t.Fatalf("OnRunFinished after Start: err = %v, want ErrAlreadyStarted", err)
		}
	}},
	{"every observer sees every event, even after one panics", func(e *env) {
		rn := notifierOf(e)
		if err := rn.OnRunFinished(func(context.Context, workflow.RunEvent) { panic("workflowtest: observer bug") }); err != nil {
			e.t.Fatalf("OnRunFinished: %v", err)
		}
		first, second := observeWith(e, rn), observeWith(e, rn)
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		for _, col := range []*collector{first, second} {
			expectEnd(e, col, ref, wf.Name, workflow.StatusSucceeded, nil, noErr)
		}
		e.quiet()
		if st := e.status(ref); st != workflow.StatusSucceeded {
			e.t.Fatalf("a panicking observer changed the run's status to %q", st)
		}
	}},
	{"a scheduled run cancelled before it fires is reported", func(e *env) {
		e.need(workflow.CapRunFinished | workflow.CapDelayedStart | workflow.CapCancel)
		col := observeWith(e, notifierOf(e))
		var c counter
		wf := oneStep(e.name("w"), c.echo())
		e.up(wf)
		labels := map[string]string{"tenant": "t2"}
		ref := e.enqueue(workflow.Job{Workflow: wf.Name, Labels: labels, RunAt: e.now().Add(e.tm.unit)})
		ctx, cancel := e.callCtx()
		defer cancel()
		if err := e.c.Cancel(ctx, ref); err != nil {
			e.t.Fatalf("Cancel of a scheduled run: %v", err)
		}
		expectEnd(e, col, ref, wf.Name, workflow.StatusCancelled, labels, cancelledErr)
	}},
	{"a run dropped or cancelled by its concurrency strategy is reported", func(e *env) {
		e.need(workflow.CapRunFinished | workflow.CapConcurrencyKey)
		col := observeWith(e, notifierOf(e))
		var wfs []workflow.Workflow
		trackers := map[string]*tracker{}
		g := newGate(e.t)
		for _, s := range []workflow.ConcurrencyStrategy{workflow.ConcurrencyDropNewest, workflow.ConcurrencyCancelNewest} {
			tr := newTracker()
			wf := oneStep(e.name(string(s)), tr.handler(g))
			wf.Concurrency = &workflow.Concurrency{Key: e.h.keyFromLabel(concurrencyLabel), MaxRuns: 1, Strategy: s}
			wfs = append(wfs, wf)
			trackers[wf.Name] = tr
		}
		e.up(wfs...)
		for _, wf := range wfs {
			tr := trackers[wf.Name]
			e.enqueue(job(wf.Name, "x", "a"))
			e.eventually("run a to start", 0, func() bool { return tr.hasStarted("x:a") })
			b := e.enqueue(job(wf.Name, "x", "b"))
			expectEnd(e, col, b, wf.Name, workflow.StatusCancelled, map[string]string{concurrencyLabel: "x"}, cancelledErr)
		}
	}},
}
