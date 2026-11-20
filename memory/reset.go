package memory

import (
	"context"
	"fmt"

	"github.com/ubgo/workflow"
)

var _ workflow.RunResetter = (*Client)(nil)

// ResetRun re-runs a failed run from step: step and every step downstream
// of it run again, the other steps that succeeded keep their outputs. See
// workflow.RunResetter.
func (c *Client) ResetRun(ctx context.Context, ref workflow.RunRef, step string) (workflow.RunRef, error) {
	if step == "" {
		return workflow.RunRef{}, fmt.Errorf("%w: ResetRun needs a step", workflow.ErrInvalid)
	}
	return c.retryFrom(ctx, ref, step)
}
