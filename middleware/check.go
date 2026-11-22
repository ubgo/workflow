package middleware

import (
	"context"
	"errors"
	"fmt"

	"github.com/ubgo/workflow"
)

// Check is step middleware that states a step's contract: pre runs on
// the step's input before the step, post on its input and output after a
// successful step. An error from either fails the attempt with a
// workflow.Permanent error wrapping workflow.ErrConstraint and the
// condition's own error, so a breach stops the run at once (retrying the
// same data cannot fix it) and names the rule that broke; a post breach
// also drops the step's output, so no child step consumes it. An error
// already permanent and matching ErrConstraint (workflow.Invariant) is
// passed on as it is. A nil pre or post is skipped.
//
// Apply it to one step's handler (Run: middleware.Check(pre, post)(run))
// or through the driver's step middleware for every step. It runs on
// every driver.
//
//ds:def id=mw-check-4cs4nmby owner=@khanakia stability=stable
func Check(pre func(ctx context.Context, in []byte) error, post func(ctx context.Context, in, out []byte) error) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if pre != nil {
				if err := pre(ctx, in); err != nil {
					return nil, constraint("precondition", err)
				}
			}
			out, err := next(ctx, in)
			if err != nil || post == nil {
				return out, err
			}
			if err := post(ctx, in, out); err != nil {
				return nil, constraint("postcondition", err)
			}
			return out, nil
		}
	}
}

// constraint makes err a permanent constraint breach, unless it is one.
func constraint(what string, err error) error {
	if workflow.IsPermanent(err) && errors.Is(err, workflow.ErrConstraint) {
		return err
	}
	return workflow.Permanent(fmt.Errorf("%w: %s: %w", workflow.ErrConstraint, what, err))
}
