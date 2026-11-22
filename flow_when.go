package workflow

import (
	"context"
	"fmt"
)

// When makes a step conditional (CM-10): the step runs h only when cond
// reports true for its input, and otherwise returns its input unchanged
// without calling h. Children of the step therefore still run, and they
// receive what the step received rather than an empty input, which is
// what a skipped node of a pipeline means: the data flows past it. A step
// with one parent passes that parent's output through; a step with
// several passes the JSON object keyed by parent name that StepInput
// built, so a child sees the same shape whether the step ran or not only
// when the step has one parent. For an either-or between several paths
// use Choice; to drop a step's output instead of passing it on, use
// middleware.Skip.
//
// cond runs on every attempt, before h, and must decide from the input and
// the ctx alone (not from the wall clock) so a retry decides the same
// way. A cond error is returned as the step's error, so the retry policy
// applies. A nil cond or h makes every call fail with a Permanent error
// wrapping ErrInvalid.
//
//ds:def id=when-z8ecm2uw owner=@khanakia stability=stable
func When(cond func(ctx context.Context, in []byte) (bool, error), h Handler) Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		if cond == nil || h == nil {
			return nil, Permanent(invalidf("When needs a condition and a handler"))
		}
		ok, err := cond(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("workflow: When condition: %w", err)
		}
		if !ok {
			return in, nil
		}
		return h(ctx, in)
	}
}
