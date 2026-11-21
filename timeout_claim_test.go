package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
)

// TestAttemptTimeoutOnlyTheAttemptsOwn: attemptTimeout names a step or
// heartbeat timeout the attempt's ctx was cancelled with, and nothing
// else: not while it runs, not a run timeout, a cancel, or a failing
// run's *RunError around another step's timeout.
func TestAttemptTimeoutOnlyTheAttemptsOwn(t *testing.T) {
	step := workflow.TimeoutError(workflow.ErrStepTimeout, "s")
	hb := workflow.TimeoutError(workflow.ErrHeartbeatTimeout, "h")
	for _, tc := range []struct {
		name  string
		cause error // nil: the ctx is still live
		want  error
	}{
		{"running", nil, nil},
		{"step timeout", step, step},
		{"heartbeat timeout", hb, hb},
		{"run timeout", workflow.TimeoutError(workflow.ErrRunTimeout, "r"), nil},
		{"cancel", workflow.ErrCancelled, nil},
		{"another step's timeout failing the run", &workflow.RunError{RunID: "r1", Step: "other", Err: step}, nil},
		{"a wrapped step timeout", errors.Join(step), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			if tc.cause != nil {
				cancel(tc.cause)
			}
			if got := workflow.AttemptTimeoutForTest(ctx); got != tc.want {
				t.Fatalf("attemptTimeout = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestClaimTimeout: a claim holds only for a timed-out attempt whose
// driver offers it (WithTimeoutClaim), and the driver reads it back.
func TestClaimTimeout(t *testing.T) {
	timedOut := func(ctx context.Context) context.Context {
		ctx, cancel := context.WithCancelCause(ctx)
		cancel(workflow.TimeoutError(workflow.ErrStepTimeout, ""))
		return ctx
	}
	ctx, claimed := workflow.WithTimeoutClaim(context.Background())
	if workflow.ClaimTimeout(ctx) || claimed() {
		t.Fatal("a claim on an attempt that has not timed out held")
	}
	if workflow.ClaimTimeout(timedOut(context.Background())) {
		t.Fatal("a claim without WithTimeoutClaim held")
	}
	if !workflow.ClaimTimeout(timedOut(ctx)) || !claimed() {
		t.Fatal("a claim on a timed-out attempt under WithTimeoutClaim did not hold")
	}
}
