package workflow

import (
	"context"
	"sync/atomic"
)

// A driver fails an attempt that ran past Step.Timeout (or the workflow's
// Timeout) or Step.HeartbeatTimeout with the timeout error, whatever the
// handler returned after its context was cancelled: a late success is not
// a success. Step middleware that turns the attempt's own timeout into an
// outcome (WithRules' tolerated steps record it as a FailureTimeout for
// RuleOneFailedOf) claims the timeout with ClaimTimeout, and the driver
// then keeps the handler's result. The handshake goes through ctx, so it
// passes any stack of middleware unchanged, like EnqueueOutcome.
//
// Driver authors: cancel the attempt's context with the error
// TimeoutError(ErrStepTimeout, ...) or TimeoutError(ErrHeartbeatTimeout,
// ...) returns, not wrapped, so context.Cause inside the handler matches
// it and ClaimTimeout recognises it, and run the handler under
// WithTimeoutClaim:
//
//	ctx, claimed := workflow.WithTimeoutClaim(ctx)
//	out, err := handler(ctx, in)
//	if context.Cause(ctx) == myTimeout && !claimed() {
//		return nil, myTimeoutError
//	}
//	return out, err

type timeoutClaimKey struct{}

// WithTimeoutClaim returns ctx carrying the slot ClaimTimeout fills, and a
// func that reports whether something in the handler chain claimed the
// attempt's timeout. A driver calls it once per attempt, before it runs
// the handler.
//
//ds:def id=withtimeoutclaim-qeyxp3kw owner=@khanakia stability=stable
func WithTimeoutClaim(ctx context.Context) (context.Context, func() bool) {
	var claimed atomic.Bool
	return context.WithValue(ctx, timeoutClaimKey{}, &claimed), claimed.Load
}

// ClaimTimeout tells the driver running ctx's attempt that the caller has
// turned the attempt's timeout into the outcome it returns, so the driver
// must not replace that outcome with the timeout error. It reports false,
// and claims nothing, when ctx's attempt has not timed out (its cause does
// not match ErrStepTimeout or ErrHeartbeatTimeout) or when its driver does
// not offer the claim (no WithTimeoutClaim); the driver's timeout error
// then stands.
//
//ds:def id=claimtimeout-62xdkgef owner=@khanakia stability=stable
func ClaimTimeout(ctx context.Context) bool {
	if attemptTimeout(ctx) == nil {
		return false
	}
	claimed, ok := ctx.Value(timeoutClaimKey{}).(*atomic.Bool)
	if !ok {
		return false
	}
	claimed.Store(true)
	return true
}

// attemptTimeout returns the error ctx's step attempt was cancelled with
// when it ran past Step.Timeout (matching ErrStepTimeout) or
// Step.HeartbeatTimeout (matching ErrHeartbeatTimeout), and nil otherwise:
// while the attempt still runs, or when it ended for another reason (a run
// timeout, a cancel, a shutdown, or the run failing because another step
// timed out: that cause is a *RunError around the other step's timeout,
// not this attempt's own). Both match ErrTimeout.
func attemptTimeout(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	te, ok := context.Cause(ctx).(*timeoutError)
	if !ok || (te.kind != ErrStepTimeout && te.kind != ErrHeartbeatTimeout) {
		return nil
	}
	return te
}
