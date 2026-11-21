package workflowtest

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/ubgo/workflow"
)

// The cases below hold every driver to what a handler, a trigger rule and
// a run-finished observer learn about an attempt that ran out of time:
//
//   - inside the handler, context.Cause of an attempt cut off by
//     Step.Timeout or Step.HeartbeatTimeout matches workflow.ErrStepTimeout
//     or workflow.ErrHeartbeatTimeout (and ErrTimeout), as the attempt's
//     recorded error does;
//   - a step tolerated by WithRules records its own Step.Timeout as a
//     FailureTimeout, so RuleOneFailedOf(FailureTimeout) routes it (the
//     interrupting form workflow.Boundary's doc points at);
//   - a step that ignores its context and returns success after the
//     timeout still fails with the timeout: only a tolerated step turns
//     the timeout into an outcome of its own;
//   - the error a run-finished observer gets for a run that failed on a
//     deadline still matches that deadline's sentinel after the run has
//     ended, on a durable driver that rebuilds it from what it stored.
//
// Hatchet claims neither CapRunTimeout nor CapHeartbeat (the engine times
// tasks out with its own text), so it skips them all.

// causeProbe records what a handler saw as its context's cause.
type causeProbe struct {
	mu    sync.Mutex
	cause []error
}

// handler blocks until the attempt's context ends, records its cause and
// returns ctx.Err(), as a handler that honours cancellation does.
func (p *causeProbe) handler() workflow.Handler {
	return func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		p.mu.Lock()
		p.cause = append(p.cause, context.Cause(ctx))
		p.mu.Unlock()
		return nil, ctx.Err()
	}
}

// seen returns the causes recorded so far.
func (p *causeProbe) seen() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.cause)
}

// failureProbe records the ParentFailures a ruled step saw.
type failureProbe struct {
	mu   sync.Mutex
	runs int
	got  []workflow.StepFailure
}

func (p *failureProbe) handler() workflow.Handler {
	return func(ctx context.Context, _ []byte) ([]byte, error) {
		p.mu.Lock()
		p.runs++
		p.got = append(p.got, workflow.ParentFailures(ctx)...)
		p.mu.Unlock()
		return []byte(`"routed"`), nil
	}
}

func (p *failureProbe) state() (int, []workflow.StepFailure) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.runs, slices.Clone(p.got)
}

// timeoutErrCheck returns an expectEnd check that wants the run's error
// to match kind and workflow.ErrTimeout.
func timeoutErrCheck(kind error) func(error) string {
	return func(err error) string {
		if !errors.Is(err, kind) || !workflow.IsTimeout(err) {
			return "want it to match " + kind.Error() + " and workflow.ErrTimeout after the run ended"
		}
		return ""
	}
}

var attemptCauseCases = []testCase{
	{"a step-timed-out handler sees ErrStepTimeout as its context's cause", func(e *env) {
		e.need(workflow.CapRunTimeout)
		var p causeProbe
		wf := oneStep(e.name("w"), p.handler())
		wf.Steps[0].Timeout = e.tm.unit
		wf.Retry = *workflow.RetriesNone()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		got := p.seen()
		if len(got) != 1 {
			e.t.Fatalf("the handler ended %d times, want 1", len(got))
		}
		if !errors.Is(got[0], workflow.ErrStepTimeout) || !workflow.IsTimeout(got[0]) {
			e.t.Errorf("context.Cause in the handler = %v, want it to match workflow.ErrStepTimeout and ErrTimeout", got[0])
		}
		if errors.Is(got[0], workflow.ErrHeartbeatTimeout) || errors.Is(got[0], workflow.ErrRunTimeout) {
			e.t.Errorf("context.Cause in the handler = %v matches another deadline's sentinel", got[0])
		}
	}},
	{"a heartbeat-timed-out handler sees ErrHeartbeatTimeout as its context's cause", func(e *env) {
		e.need(workflow.CapHeartbeat)
		var p causeProbe
		wf := oneStep(e.name("w"), p.handler())
		wf.Steps[0].HeartbeatTimeout = 2 * e.tm.unit
		wf.Retry = *workflow.RetriesNone()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		got := p.seen()
		if len(got) != 1 {
			e.t.Fatalf("the handler ended %d times, want 1", len(got))
		}
		if !errors.Is(got[0], workflow.ErrHeartbeatTimeout) || !workflow.IsTimeout(got[0]) {
			e.t.Errorf("context.Cause in the handler = %v, want it to match workflow.ErrHeartbeatTimeout and ErrTimeout", got[0])
		}
		if errors.Is(got[0], workflow.ErrStepTimeout) {
			e.t.Errorf("context.Cause in the handler = %v matches ErrStepTimeout for a missed heartbeat", got[0])
		}
	}},
}

var toleratedTimeoutCases = []testCase{
	{"a tolerated step's Step.Timeout reaches RuleOneFailedOf(FailureTimeout)", func(e *env) {
		e.need(workflow.CapRunTimeout)
		var slow causeProbe
		var routed, other failureProbe
		wf, err := workflow.WithRules(workflow.Workflow{Name: e.name("w"), Retry: *workflow.RetriesNone(), Steps: []workflow.Step{
			{Name: "slow", Timeout: e.tm.unit, Run: slow.handler()},
			{Name: "on-timeout", After: []string{"slow"}, Run: routed.handler()},
			{Name: "on-error", After: []string{"slow"}, Run: other.handler()},
		}}, workflow.RulesSpec{
			Tolerate: []string{"slow"},
			Rules: map[string]workflow.TriggerRule{
				"on-timeout": workflow.RuleOneFailedOf(workflow.FailureTimeout),
				"on-error":   workflow.RuleOneFailedOf(workflow.FailureError),
			},
		})
		if err != nil {
			e.t.Fatal(err)
		}
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusSucceeded, e.tm.unit)
		runs, got := routed.state()
		if runs != 1 || len(got) != 1 || got[0].Step != "slow" || got[0].Kind != workflow.FailureTimeout {
			e.t.Errorf("on-timeout ran %d times with ParentFailures %+v, want once with slow's %q failure", runs, got, workflow.FailureTimeout)
		}
		if n, _ := other.state(); n != 0 {
			e.t.Errorf("on-error ran %d times for a timeout, want 0", n)
		}
	}},
	{"a handler that returns success after its Step.Timeout still fails with ErrStepTimeout", func(e *env) {
		e.need(workflow.CapRunTimeout)
		col := (*collector)(nil)
		if e.c.Capabilities().Has(workflow.CapRunFinished) {
			col = observeWith(e, notifierOf(e))
		}
		wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
			<-ctx.Done()
			return []byte(`"late"`), nil // ignores the cancellation
		})
		wf.Steps[0].Timeout = e.tm.unit
		wf.Retry = *workflow.RetriesNone()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		if col != nil {
			expectEnd(e, col, ref, wf.Name, workflow.StatusFailed, nil, timeoutErrCheck(workflow.ErrStepTimeout))
		}
	}},
}

var storedTimeoutCases = []testCase{
	{"a step-timed-out run's error matches ErrStepTimeout after the run ended", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapRunTimeout, workflow.CapRunFinished))
		col := observeWith(e, notifierOf(e))
		var p causeProbe
		wf := oneStep(e.name("w"), p.handler())
		wf.Steps[0].Timeout = e.tm.unit
		wf.Retry = *workflow.RetriesNone()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		expectEnd(e, col, ref, wf.Name, workflow.StatusFailed, nil, timeoutErrCheck(workflow.ErrStepTimeout))
	}},
	{"a heartbeat-timed-out run's error matches ErrHeartbeatTimeout after the run ended", func(e *env) {
		e.need(workflow.CapsOf(workflow.CapHeartbeat, workflow.CapRunFinished))
		col := observeWith(e, notifierOf(e))
		var p causeProbe
		wf := oneStep(e.name("w"), p.handler())
		wf.Steps[0].HeartbeatTimeout = 2 * e.tm.unit
		wf.Retry = *workflow.RetriesNone()
		e.up(wf)
		ref := e.enqueue(workflow.Job{Workflow: wf.Name})
		e.waitStatus(ref, workflow.StatusFailed, e.tm.unit)
		expectEnd(e, col, ref, wf.Name, workflow.StatusFailed, nil, timeoutErrCheck(workflow.ErrHeartbeatTimeout))
	}},
}

// attemptErrorGroups are the groups above, in Run order.
var attemptErrorGroups = []suiteGroup{
	{"attemptcause", attemptCauseCases},
	{"toleratedtimeout", toleratedTimeoutCases},
	{"storedtimeout", storedTimeoutCases},
}
