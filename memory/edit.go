package memory

import (
	"context"
	"fmt"
	"slices"

	"github.com/ubgo/workflow"
)

var _ workflow.RunEditor = (*Client)(nil)

// EditRun changes a delayed run that has not started: payload, priority,
// start (see workflow.RunEditor). The payload limit of WithPayloadLimit
// applies to a new payload; the concurrency and rate keys are computed
// from it when the run is admitted, as for any run.
//
//ds:def id=memory-client-editrun-3afh5vbv owner=@khanakia stability=stable
func (c *Client) EditRun(ctx context.Context, ref workflow.RunRef, edit workflow.JobEdit) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJobEdit(edit); err != nil {
		return workflow.RunRef{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return workflow.RunRef{}, err
	}
	r, err := c.lookupLocked(ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if r.status != workflow.StatusScheduled {
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s, not scheduled", workflow.ErrWrongState, ref, r.statusLocked())
	}
	job := r.job
	if edit.Payload != nil {
		job.Payload = slices.Clone(*edit.Payload)
		if err := workflow.CheckPayload(job, c.payloadLimit, "memory.WithPayloadLimit"); err != nil {
			return workflow.RunRef{}, err
		}
	}
	if edit.Priority != nil {
		job.Priority = *edit.Priority
	}
	r.job = job
	if edit.RunAt != nil {
		now := c.clk.Now()
		at := *edit.RunAt
		if at.IsZero() || !at.After(now) {
			at = now
		}
		r.runAt = at
		c.armDelayLocked(r, at.Sub(now))
	}
	return r.ref(), nil
}
