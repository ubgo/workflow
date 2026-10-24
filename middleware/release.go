package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/ubgo/workflow"
)

// ErrReleased is wrapped by the error Release returns for a step it did not
// run, so a retry policy, a log filter or a test can tell a release from a
// real failure with errors.Is.
var ErrReleased = errors.New("workflow/middleware: step released")

// Release does not run the step when when reports true: the attempt ends
// with workflow.RetryAfter(ErrReleased, delay), so the driver retries the
// step after delay: a release decided before the step runs. To release
// unless a condition holds, negate the condition. A nil when is a no-op; a negative delay counts as
// zero.
//
// Two rules come from the retry policy, not from this middleware, and hold
// on every driver:
//
//   - The delay is honoured only under a policy whose DelayFor reads it
//     (workflow.RetryAfterDelay); under any other policy a released
//     attempt waits the policy's normal Backoff.
//   - A release is an attempt: it counts against
//     RetryPolicy.Max, and a step released on its last attempt fails its
//     run with an error wrapping ErrReleased. A step that may be released
//     often needs a Max to match.
//
// To release from inside a handler, after it has started, return
// workflow.RetryAfter(err, d) there; this middleware is for a condition
// checked before the handler, across every workflow it wraps.
func Release(when func(ctx context.Context, in []byte) bool, delay time.Duration) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		if when == nil {
			return next
		}
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if when(ctx, in) {
				return nil, workflow.RetryAfter(ErrReleased, delay)
			}
			return next(ctx, in)
		}
	}
}
