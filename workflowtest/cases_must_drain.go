package workflowtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
)

// Drain report conformance (ReportingDrainer, DrainReport, CapDrainReport).
//
// A drain has two clocks, the grace the caller gives and the speed of the
// steps, and three possible fates for a running attempt: it finishes inside
// the grace, it is still running at the deadline and is cut, or it was never
// running. The matrix crosses the number of steps that finish after the
// drain began (K) with the number that never will (M, "stuck": they only
// return when the client cancels them), then separate cases cover the grace
// itself (zero, negative, short, long), repeated and concurrent drains, a
// drain before Start, the ShutdownHook adapter, and what happens to a cut
// attempt afterwards: handed back to the store without being charged an
// attempt (CapShutdownRequeue) or cancelled (nothing outlives the client).

// Inputs of the drain fleet's steps.
const (
	inputQuick = "quick" // finishes when the gate opens
	inputStuck = "stuck" // returns only when the client cancels it
)

// drainFleet is the workflow of the drain cases: quick and stuck steps that
// record every attempt they start.
type drainFleet struct {
	wf       workflow.Workflow
	gate     *gate
	started  atomic.Int64
	released atomic.Bool // set after the drain: stuck steps then return at once

	mu       sync.Mutex
	attempts map[workflow.RunID][]int // StepInfo.Attempt of every start, per run
}

func (e *env) newDrainFleet() *drainFleet {
	f := &drainFleet{gate: newGate(e.t), attempts: map[workflow.RunID][]int{}}
	f.wf = oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		f.mu.Lock()
		f.attempts[info.RunID] = append(f.attempts[info.RunID], info.Attempt)
		f.mu.Unlock()
		f.started.Add(1)
		if string(in) == inputStuck && !f.released.Load() {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return in, f.gate.wait(ctx)
	})
	// A cut attempt that was charged would fail the run: no retries.
	f.wf.Retry = *workflow.RetriesNone()
	return f
}

func (f *drainFleet) attemptsOf(id workflow.RunID) []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.attempts[id])
}

// enqueueN enqueues n runs of input and returns their refs.
func (e *env) enqueueN(f *drainFleet, n int, input string) []workflow.RunRef {
	var refs []workflow.RunRef
	for range n {
		refs = append(refs, e.enqueue(workflow.Job{Workflow: f.wf.Name, Payload: []byte(input)}))
	}
	return refs
}

// drainResult is what a DrainWithReport call returned.
type drainResult struct {
	report workflow.DrainReport
	err    error
}

// goDrain runs rd.DrainWithReport(ctx) on its own goroutine.
func goDrain(rd workflow.ReportingDrainer, ctx context.Context) <-chan drainResult {
	ch := make(chan drainResult, 1)
	go func() {
		rep, err := rd.DrainWithReport(ctx)
		ch <- drainResult{rep, err}
	}()
	return ch
}

// await returns the result or fails after Wait.
func (e *env) await(ch <-chan drainResult, what string) drainResult {
	e.t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(e.tm.wait):
		e.t.Fatalf("%s did not return within %s", what, e.tm.wait)
		return drainResult{}
	}
}

// done fails unless the client's Done channel is closed.
func (e *env) done(d workflow.Drainer) {
	e.t.Helper()
	select {
	case <-d.Done():
	case <-time.After(e.tm.wait):
		e.t.Fatal("Done was not closed after the drain")
	}
}

// disposition reports whether got is what a driver with this capability set
// may say about a cut attempt: requeued exactly when it declares
// CapShutdownRequeue, otherwise cancelled or left to the engine.
func (e *env) okDisposition(got workflow.DrainDisposition) bool {
	if e.c.Capabilities().Has(workflow.CapShutdownRequeue) {
		return got == workflow.DrainRequeued
	}
	return got == workflow.DrainCancelled || got == workflow.DrainEngineOwned
}

// needDrain skips unless the driver drains with a report.
func (e *env) needDrain() workflow.ReportingDrainer {
	e.t.Helper()
	e.need(workflow.CapDrain | workflow.CapDrainReport)
	return as[workflow.ReportingDrainer](e, "workflow.ReportingDrainer")
}

type drainRow struct{ finish, stuck int }

var drainMatrix = []drainRow{{1, 0}, {3, 0}, {0, 1}, {0, 3}, {2, 1}, {1, 2}, {3, 3}}

var drainReportCases = func() []testCase {
	var out []testCase
	for _, row := range drainMatrix {
		out = append(out, testCase{fmt.Sprintf("%d step(s) finish after the drain began, %d never do: report and fate of the cut", row.finish, row.stuck),
			func(e *env) { e.runDrainRow(row) }})
	}
	for _, grace := range []time.Duration{0, -time.Second} {
		out = append(out, testCase{fmt.Sprintf("a grace of %s cuts the running step at once and never waits", grace), func(e *env) {
			rd := e.needDrain()
			f := e.newDrainFleet()
			e.up(f.wf)
			e.enqueueN(f, 1, inputStuck)
			e.eventually("the step to start", 0, func() bool { return f.started.Load() == 1 })
			begin := time.Now()
			rep, detailed, err := workflow.DrainTimeoutReport(context.Background(), rd, grace)
			if took := time.Since(begin); took > e.tm.wait {
				e.t.Fatalf("a grace of %s took %s", grace, took)
			}
			if !detailed || !errors.Is(err, context.Canceled) || len(rep.Cut) != 1 {
				e.t.Fatalf("DrainTimeoutReport(grace %s) = detailed %t, err %v, cut %d; want detailed, Canceled, 1 cut", grace, detailed, err, len(rep.Cut))
			}
			e.done(rd)
		}})
	}
	return append(out,
		testCase{"a grace of zero on an idle client is a clean, nil drain", func(e *env) {
			rd := e.needDrain()
			f := e.newDrainFleet()
			e.up(f.wf)
			rep, detailed, err := workflow.DrainTimeoutReport(context.Background(), rd, 0)
			if !detailed || err != nil || !rep.Clean() || rep.FinishedSteps != 0 || rep.FinishedRuns != 0 {
				e.t.Fatalf("idle drain = %+v, detailed %t, err %v; want an empty clean report and nil", rep, detailed, err)
			}
			e.done(rd)
		}},
		testCase{"a short grace cuts at the deadline, not before", func(e *env) {
			rd := e.needDrain()
			f := e.newDrainFleet()
			e.up(f.wf)
			e.enqueueN(f, 1, inputStuck)
			e.eventually("the step to start", 0, func() bool { return f.started.Load() == 1 })
			grace := 4 * e.tm.quiet
			begin := time.Now()
			rep, _, err := workflow.DrainTimeoutReport(context.Background(), rd, grace)
			took := time.Since(begin)
			if !errors.Is(err, context.DeadlineExceeded) || len(rep.Cut) != 1 {
				e.t.Fatalf("drain past a %s grace = err %v, cut %d; want DeadlineExceeded and 1 cut", grace, err, len(rep.Cut))
			}
			if took < grace {
				e.t.Fatalf("the drain cut a running step after %s, before its %s grace", took, grace)
			}
		}},
		testCase{"a step that finishes inside a long grace is not cut and the drain returns early", func(e *env) {
			rd := e.needDrain()
			f := e.newDrainFleet()
			e.up(f.wf)
			e.enqueueN(f, 1, inputQuick)
			e.eventually("the step to start", 0, func() bool { return f.started.Load() == 1 })
			grace := 20 * e.tm.quiet
			res := make(chan drainResult, 1)
			begin := time.Now()
			go func() {
				rep, _, err := workflow.DrainTimeoutReport(context.Background(), rd, grace)
				res <- drainResult{rep, err}
			}()
			e.quiet()
			f.gate.open()
			r := e.await(res, "the drain")
			if took := time.Since(begin); took >= grace {
				e.t.Fatalf("the drain took %s, the whole %s grace: it waited instead of returning when the step finished", took, grace)
			}
			if r.err != nil || !r.report.Clean() || r.report.FinishedSteps != 1 || r.report.FinishedRuns != 1 {
				e.t.Fatalf("drain = %+v, err %v; want clean with 1 finished step and 1 finished run", r.report, r.err)
			}
			e.done(rd)
		}},
		testCase{"a second and a concurrent Drain return the first drain's report", func(e *env) {
			rd := e.needDrain()
			f := e.newDrainFleet()
			e.up(f.wf)
			e.enqueueN(f, 2, inputStuck)
			e.eventually("both steps to start", 0, func() bool { return f.started.Load() == 2 })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a, b := goDrain(rd, ctx), goDrain(rd, ctx)
			e.quiet()
			cancel()
			ra, rb := e.await(a, "the first Drain"), e.await(b, "the concurrent Drain")
			if len(ra.report.Cut) != 2 || !reflect.DeepEqual(ra.report, rb.report) {
				e.t.Fatalf("concurrent drains reported %+v and %+v; want the same report with 2 cut", ra.report, rb.report)
			}
			e.done(rd)
			// After Done: a fresh call neither hangs nor reports a different drain.
			later := e.await(goDrain(rd, context.Background()), "Drain after Done")
			if !reflect.DeepEqual(later.report, ra.report) {
				e.t.Fatalf("Drain after Done reported %+v, want the first drain's %+v", later.report, ra.report)
			}
			if later.err != nil && !errors.Is(later.err, context.Canceled) {
				e.t.Fatalf("Drain after Done returned %v, want nil or the first drain's context error", later.err)
			}
			again, detailed, _ := workflow.DrainTimeoutReport(context.Background(), rd, time.Minute)
			if !detailed || !reflect.DeepEqual(again, ra.report) {
				e.t.Fatalf("DrainTimeoutReport after Done = %+v, want the first drain's report", again)
			}
		}},
		testCase{"Drain before Start is ErrNotStarted", func(e *env) {
			rd := e.needDrain()
			e.register(oneStep(e.name("w"), (&counter{}).echo()))
			if _, err := rd.DrainWithReport(context.Background()); !errors.Is(err, workflow.ErrNotStarted) {
				e.t.Fatalf("DrainWithReport before Start: %v, want ErrNotStarted", err)
			}
		}},
		testCase{"ShutdownHook obeys the tighter of its grace and the phase deadline", func(e *env) {
			rd := e.needDrain()
			f := e.newDrainFleet()
			e.up(f.wf)
			e.enqueueN(f, 1, inputStuck)
			e.eventually("the step to start", 0, func() bool { return f.started.Load() == 1 })
			phase, cancel := context.WithTimeout(context.Background(), 2*e.tm.quiet)
			defer cancel()
			begin := time.Now()
			err := workflow.ShutdownHook(rd, time.Hour)(phase)
			if !errors.Is(err, context.DeadlineExceeded) {
				e.t.Fatalf("hook past its phase deadline: %v, want DeadlineExceeded", err)
			}
			if took := time.Since(begin); took > e.tm.wait {
				e.t.Fatalf("the hook took %s, past the phase deadline", took)
			}
			e.done(rd)
		}},
	)
}()

func (e *env) runDrainRow(row drainRow) {
	e.t.Helper()
	rd := e.needDrain()
	f := e.newDrainFleet()
	e.up(f.wf)
	quick := e.enqueueN(f, row.finish, inputQuick)
	stuck := e.enqueueN(f, row.stuck, inputStuck)
	total := int64(row.finish + row.stuck)
	e.eventually("every step to start", 0, func() bool { return f.started.Load() == total })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := goDrain(rd, ctx)
	e.quiet() // the drain has begun; only now do the quick steps finish
	f.gate.open()
	for _, ref := range quick {
		e.waitSucceededOrDrained(ref)
	}
	if row.stuck > 0 {
		e.quiet()
		select {
		case r := <-res:
			e.t.Fatalf("Drain returned (%+v, %v) while %d step(s) were still running and its ctx was open", r.report, r.err, row.stuck)
		default:
		}
		cancel() // the deadline
	}
	r := e.await(res, "Drain")
	rep := r.report
	if row.stuck == 0 {
		if r.err != nil || !rep.Clean() {
			e.t.Fatalf("every step finished but Drain = %+v, %v; want clean and nil", rep, r.err)
		}
	} else if !errors.Is(r.err, context.Canceled) {
		e.t.Fatalf("Drain cut %d step(s) but returned %v, want the ctx error", row.stuck, r.err)
	}
	if rep.FinishedSteps != row.finish || rep.FinishedRuns != row.finish {
		e.t.Errorf("FinishedSteps = %d, FinishedRuns = %d; want %d and %d", rep.FinishedSteps, rep.FinishedRuns, row.finish, row.finish)
	}
	if len(rep.Cut) != row.stuck || rep.CutRuns() != row.stuck || rep.Clean() != (row.stuck == 0) {
		e.t.Fatalf("report = %+v; want %d cut attempts of %d distinct runs", rep, row.stuck, row.stuck)
	}
	cutIDs := map[workflow.RunID]bool{}
	for _, c := range rep.Cut {
		cutIDs[c.RunID] = true
		if c.Workflow != f.wf.Name || c.Step != "only" || c.Attempt != 1 || !e.okDisposition(c.Disposition) {
			e.t.Errorf("cut %+v; want workflow %q, step %q, attempt 1 and a disposition this driver may report", c, f.wf.Name, "only")
		}
	}
	for _, ref := range stuck {
		if !cutIDs[ref.RunID] {
			e.t.Errorf("stuck run %v is missing from the report's Cut", ref.RunID)
		}
	}
	for _, ref := range quick {
		if cutIDs[ref.RunID] {
			e.t.Errorf("run %v finished before the deadline but is listed as cut", ref.RunID)
		}
	}
	e.done(rd)
	if row.stuck == 0 {
		return
	}

	// What became of the cut attempts.
	f.released.Store(true)
	if e.c.Capabilities().Has(workflow.CapShutdownRequeue) {
		if e.restart == nil {
			e.t.Log("Instance.Restart is nil: the requeued attempts are not followed to completion")
			return
		}
		e.doRestart()
		for _, ref := range stuck {
			e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
			// The cut attempt was not charged: the rerun is attempt 1 again,
			// and with no retries declared the run could not have survived a
			// charged one.
			if got := f.attemptsOf(ref.RunID); !slices.Equal(got, []int{1, 1}) {
				e.t.Errorf("run %v started attempts %v, want [1 1]: the cut one, then the same number again", ref.RunID, got)
			}
		}
		return
	}
	// Without requeue the fate is the driver's to state. A cut it reports as
	// DrainCancelled must read Cancelled; one it reports as DrainEngineOwned
	// belongs to the engine (and the drained client cannot be asked, see
	// waitSucceededOrDrained), so there is nothing here to check.
	for _, c := range rep.Cut {
		if c.Disposition != workflow.DrainCancelled {
			return
		}
	}
	for _, ref := range stuck {
		e.waitStatus(ref, workflow.StatusCancelled, 0)
	}
}

// waitSucceededOrDrained waits for ref to succeed, but stops waiting once the
// client refuses the question with ErrNotStarted. A driver may answer
// ErrNotStarted for every call after its drain finished (Hatchet's does: it
// has stopped its workers and tells the caller to ask another client), and
// a drain with no stuck step finishes the moment the last handler returns,
// possibly before this poll sees the run's final status. The handler having
// returned is what FinishedSteps asserts; the status is checked here only
// while the client can still be asked, and a wrong terminal status still
// fails.
func (e *env) waitSucceededOrDrained(ref workflow.RunRef) {
	e.t.Helper()
	e.eventually("status "+string(workflow.StatusSucceeded)+" or a finished drain", 0, func() bool {
		ctx, cancel := e.callCtx()
		defer cancel()
		st, err := e.c.Status(ctx, ref)
		if errors.Is(err, workflow.ErrNotStarted) {
			return true
		}
		if err != nil {
			e.t.Fatalf("Status(%+v): %v", ref, err)
		}
		if st != workflow.StatusSucceeded && st.IsTerminal() {
			e.t.Fatalf("run %+v ended %q, want %q", ref, st, workflow.StatusSucceeded)
		}
		return st == workflow.StatusSucceeded
	})
}
