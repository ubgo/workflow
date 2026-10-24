package memory

import (
	"context"
	"fmt"

	"github.com/ubgo/workflow"
)

// ForceComplete ends a run as succeeded without running its remaining
// steps; see workflow.RunForcer. Attempts in progress see their context
// cancelled with cause workflow.ErrForced and their results are dropped.
// A failed run is turned into a success: its run-finished event is
// delivered again and its chain continues.
func (c *Client) ForceComplete(ctx context.Context, ref workflow.RunRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.lookupLocked(ref)
	if err != nil {
		return err
	}
	st := r.statusLocked()
	if !workflow.CanForceComplete(st) {
		return fmt.Errorf("%w: run %s is %s; only an unfinished or failed run can be force-completed", workflow.ErrWrongState, r.ref().RunID, st)
	}
	if st == workflow.StatusFailed {
		// Already finished: its key, slot and context are released, so
		// only the outcome changes.
		r.status, r.err, r.failedStep = workflow.StatusSucceeded, nil, ""
		r.finishedAt = c.clk.Now()
		c.queueEndedLocked(r)
	} else {
		r.cancel(workflow.ErrForced) // first cause wins: attempts see ErrForced
		c.endLocked(r, workflow.StatusSucceeded)
	}
	c.continueChainLocked(r)
	return nil
}

// ForceFail ends an unfinished run as failed with
// workflow.ForcedFailure(reason); see workflow.RunForcer. No retry is
// made and OnFailure does not run.
func (c *Client) ForceFail(ctx context.Context, ref workflow.RunRef, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := c.lookupLocked(ref)
	if err != nil {
		return err
	}
	if st := r.statusLocked(); !workflow.CanForceFail(st) {
		return fmt.Errorf("%w: run %s is %s; only an unfinished run can be force-failed", workflow.ErrWrongState, r.ref().RunID, st)
	}
	r.cancel(workflow.ErrForced)
	r.err = workflow.ForcedFailure(reason)
	c.endLocked(r, workflow.StatusFailed)
	return nil
}
