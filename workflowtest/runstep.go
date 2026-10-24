package workflowtest

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// StepOutcome is what one call of a step handler returned, for asserting
// how the attempt ended (released, failed, permanent, discarded) without
// a client or a driver.
type StepOutcome struct {
	Output []byte
	Err    error

	// Chain is the run's chain after this attempt: StepChain's jobs with
	// the attempt's PrependToChain and AppendToChain edits applied when
	// it succeeded, unchanged when it failed (a driver drops a failed
	// attempt's edits).
	Chain []workflow.Job
}

// StepOption configures RunStep.
type StepOption func(*stepRun)

type stepRun struct {
	ctx   context.Context
	info  workflow.StepInfo
	mw    []workflow.Middleware
	chain []workflow.Job
	caps  workflow.Caps
}

// StepContext sets the parent ctx of the call; the default is
// context.Background.
func StepContext(ctx context.Context) StepOption { return func(s *stepRun) { s.ctx = ctx } }

// StepInfoOf sets the workflow.StepInfo the handler sees, for a handler
// that reads its attempt, run or labels. The default is workflow "test",
// step "step", run "runstep", attempt 1.
func StepInfoOf(info workflow.StepInfo) StepOption { return func(s *stepRun) { s.info = info } }

// StepChain sets the chain the run already holds (its Job.Chain), which
// the step's PrependToChain and AppendToChain edit; see StepOutcome.Chain.
func StepChain(jobs ...workflow.Job) StepOption {
	return func(s *stepRun) { s.chain = append(s.chain, jobs...) }
}

// StepCaps sets the capabilities the step's chain edits are checked
// against, as the production driver's would be. The default is the
// Fake's default set, which includes CapChain. Without
// CapChain the step's PrependToChain and AppendToChain are refused, as on
// a driver that cannot edit chains.
func StepCaps(c workflow.Caps) StepOption { return func(s *stepRun) { s.caps = c } }

// StepMiddleware wraps the handler in mw, outermost first, as a driver
// built with the same middleware would.
func StepMiddleware(mw ...workflow.Middleware) StepOption {
	return func(s *stepRun) { s.mw = append(s.mw, mw...) }
}

// RunStep calls h once with in, on a ctx carrying StepInfo, and returns
// the outcome. It runs no retries, so it shows what one attempt asks for:
// a release (workflow.RetryAfter), a permanent failure, a discard
// (workflow.Discard), an ordinary failure the retry policy would retry,
// or success. A panic in h is not
// recovered, so it fails the test with its stack.
func RunStep(h workflow.Handler, in []byte, opts ...StepOption) StepOutcome {
	s := stepRun{ctx: context.Background(), info: workflow.StepInfo{Workflow: "test", Step: "step", RunID: "runstep", Attempt: 1},
		caps: fakeDefaultCaps}
	for _, o := range opts {
		o(&s)
	}
	for i := len(s.mw) - 1; i >= 0; i-- {
		h = s.mw[i](h)
	}
	ctx := s.ctx
	var edits *workflow.ChainEdits
	if s.caps.Has(workflow.CapChain) {
		edits = workflow.NewChainEdits(s.caps, s.chain)
		ctx = workflow.WithChainEditor(ctx, edits)
	}
	out, err := h(workflow.WithStepInfo(ctx, s.info), in)
	o := StepOutcome{Output: out, Err: err, Chain: s.chain}
	if err == nil && edits != nil {
		o.Chain = edits.Apply(s.chain)
	}
	return o
}

// ChainNames is the order the jobs of o.Chain would run in, nested
// chains flattened as a driver runs them.
func (o StepOutcome) ChainNames() []string { return chainNames(o.Chain) }

// Succeeded reports whether the attempt returned no error.
func (o StepOutcome) Succeeded() bool { return o.Err == nil }

// Failed reports whether the attempt returned an error of any kind: a
// release, a permanent failure or an ordinary one.
func (o StepOutcome) Failed() bool { return o.Err != nil }

// Permanent reports whether the error forbids any retry
// (workflow.Permanent).
func (o StepOutcome) Permanent() bool { return workflow.IsPermanent(o.Err) }

// Discarded reports whether the step asked to drop the run
// (workflow.Discard): no retry, and the run ends discarded (CapRunStates)
// or cancelled (CapDiscard) rather than failed; see workflow.EndStatus.
func (o StepOutcome) Discarded() bool { return workflow.IsDiscarded(o.Err) }

// Released reports whether the error asks for a retry after a delay
// (workflow.RetryAfter, a release) and that delay. A policy whose
// DelayFor is workflow.RetryAfterDelay honours it; other policies retry on
// their Backoff. Permanent and Discard win over a release, as on every
// driver.
func (o StepOutcome) Released() (time.Duration, bool) {
	if o.Err == nil || o.Permanent() || o.Discarded() {
		return 0, false
	}
	return workflow.RetryAfterDelay(o.Err, 1)
}

// AssertSucceeded fails t unless the attempt succeeded.
func (o StepOutcome) AssertSucceeded(t testing.TB) {
	t.Helper()
	if o.Err != nil {
		t.Errorf("step failed: %v", o.Err)
	}
}

// AssertFailed fails t unless the attempt returned an error that is
// neither permanent, discarded nor a release: one the retry policy
// retries.
func (o StepOutcome) AssertFailed(t testing.TB) {
	t.Helper()
	_, released := o.Released()
	if o.Err == nil || o.Permanent() || o.Discarded() || released {
		t.Errorf("step outcome is %s, want an ordinary failure", o.describe())
	}
}

// AssertPermanent fails t unless the attempt failed permanently.
func (o StepOutcome) AssertPermanent(t testing.TB) {
	t.Helper()
	if !o.Permanent() {
		t.Errorf("step outcome is %s, want a permanent failure", o.describe())
	}
}

// AssertDiscarded fails t unless the attempt asked to drop the run.
func (o StepOutcome) AssertDiscarded(t testing.TB) {
	t.Helper()
	if !o.Discarded() {
		t.Errorf("step outcome is %s, want discarded", o.describe())
	}
}

// AssertReleased fails t unless the attempt asked to be retried after
// exactly d.
func (o StepOutcome) AssertReleased(t testing.TB, d time.Duration) {
	t.Helper()
	if got, ok := o.Released(); !ok || got != d {
		t.Errorf("step outcome is %s, want released for %s", o.describe(), d)
	}
}

// describe names the outcome for a failure message.
func (o StepOutcome) describe() string {
	if o.Err == nil {
		return "success"
	}
	if o.Permanent() {
		return "a permanent failure (" + o.Err.Error() + ")"
	}
	if o.Discarded() {
		return "discarded (" + o.Err.Error() + ")"
	}
	if d, ok := o.Released(); ok {
		return "released for " + d.String() + " (" + o.Err.Error() + ")"
	}
	return "an ordinary failure (" + o.Err.Error() + ")"
}
