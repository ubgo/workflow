package sqlstore

import (
	"context"
	"fmt"

	"github.com/ubgo/workflow"
)

var _ workflow.RunResetter = (*Client)(nil)

// ResetRun re-runs a failed run from step: step and every step downstream
// of it go back to pending with the steps that had not succeeded, the
// other steps keep their outputs (workflow.RunResetter). The steps table
// does not store the graph, so the client must have the run's workflow
// registered to know which steps are downstream; otherwise it returns an
// error wrapping ErrInvalid.
func (c *Client) ResetRun(ctx context.Context, ref workflow.RunRef, step string) (workflow.RunRef, error) {
	if step == "" {
		return workflow.RunRef{}, fmt.Errorf("%w: ResetRun needs a step", workflow.ErrInvalid)
	}
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	reg := c.reg(r.workflow)
	if reg == nil {
		return workflow.RunRef{}, fmt.Errorf("%w: ResetRun: workflow %s is not registered on this client, so its steps are unknown", workflow.ErrInvalid, r.workflow)
	}
	rerun := workflow.StepsFrom(reg.wf, step)
	if rerun == nil {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %s has no step %q", workflow.ErrInvalid, r.workflow, step)
	}
	return c.retryFrom(ctx, ref, rerun)
}
