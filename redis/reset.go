package redis

import (
	"context"
	"fmt"

	"github.com/ubgo/workflow"
)

var _ workflow.RunResetter = (*Client)(nil)

// ResetRun re-runs a failed run from step: step and every step downstream
// of it start again with the steps that had not succeeded, the other steps
// keep their outputs (workflow.RunResetter). The client must have the
// run's workflow registered to know which steps are downstream; otherwise
// it returns an error wrapping ErrInvalid.
func (c *Client) ResetRun(ctx context.Context, ref workflow.RunRef, step string) (workflow.RunRef, error) {
	if step == "" {
		return workflow.RunRef{}, fmt.Errorf("%w: ResetRun needs a step", workflow.ErrInvalid)
	}
	return c.retryFrom(ctx, ref, step)
}
