package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// tblStepCancel is migration 008's table of running steps CancelStep asked
// to stop: the node running one sees its row at its next lease renewal.
const tblStepCancel = "step_cancel"

// CancelStep stops one step of a run (workflow.StepCanceller). A running
// attempt has its context cancelled with cause workflow.ErrStepCancelled
// (at once on this client, at the next lease renewal on another), and its
// handler's return decides: an output completes the step and the run goes
// on; an error fails the step for good, without retry, matching
// ErrStepCancelled, and the run fails. A step waiting on its retry backoff,
// a snooze, a signal or a sleep fails that way at once. A step not started
// yet or already ended, or a run that is not running, returns
// ErrWrongState; an unknown run or step ErrNotFound.
func (c *Client) CancelStep(ctx context.Context, ref workflow.RunRef, step string) error {
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return err
	}
	var o txOut
	var running bool
	err = c.st.tx(ctx, func(tx *sql.Tx) error {
		o, running = txOut{}, false
		row, err := c.touch(ctx, tx, r.id)
		if err != nil {
			return err
		}
		var id, st string
		var failures int64
		var in []byte
		err = c.st.one(ctx, tx, "SELECT id, status, failures, in_data FROM {steps} WHERE run_id = ? AND name = ?", []any{row.id, step}, &id, &st, &failures, &in)
		if errors.Is(err, sqlb.ErrNoRows) {
			if row.status != runActive {
				return fmt.Errorf("%w: run %s is %s", workflow.ErrWrongState, row.id, runStatus(row.status, nil))
			}
			return fmt.Errorf("%w: run %s has no step %q", workflow.ErrNotFound, row.id, step)
		}
		if err != nil {
			return err
		}
		if row.status != runActive {
			return fmt.Errorf("%w: run %s is %s", workflow.ErrWrongState, row.id, runStatus(row.status, nil))
		}
		switch {
		case st == stepRunning:
			running = true
			if _, err := c.st.exec(ctx, tx, "DELETE FROM {step_cancel} WHERE step_id = ?", id); err != nil {
				return err
			}
			_, err := c.st.exec(ctx, tx, "INSERT INTO {step_cancel} (step_id) VALUES (?)", id)
			return err
		case st == stepSnoozed, st == stepWaiting, st == stepSleeping, st == stepReady && failures > 0:
			msg := fmt.Sprintf("sqlstore: step %q: %v", step, workflow.ErrStepCancelled)
			return c.markFailing(ctx, tx, row, step, in, msg, &o)
		}
		return fmt.Errorf("%w: step %q of run %s is %s", workflow.ErrWrongState, step, row.id, st)
	})
	if err != nil {
		return err
	}
	if running {
		c.cancelLocalStep(r.id, step, workflow.ErrStepCancelled)
	}
	c.afterTx(ctx, o)
	return nil
}

// cancelLocalStep cancels the attempts of step of run runID executing on
// this client with cause.
func (c *Client) cancelLocalStep(runID, step string, cause error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for a := range c.local[runID] {
		if a.name == step {
			a.cancel(cause)
		}
	}
}

// stepCancelRequested reports whether CancelStep asked a's step to stop.
func (c *Client) stepCancelRequested(ctx context.Context, a *attempt) (bool, error) {
	var n int64
	err := c.st.one(ctx, c.st.db, "SELECT COUNT(*) FROM {step_cancel} WHERE step_id = ?", []any{a.stepID}, &n)
	return n > 0, err
}

// cancelledOutcome is what a step CancelStep stopped returned: an output
// completes it; an error fails it for good, matching ErrStepCancelled.
func cancelledOutcome(herr error) error {
	if herr == nil {
		return nil
	}
	if !errors.Is(herr, workflow.ErrStepCancelled) {
		herr = fmt.Errorf("%w: %w", workflow.ErrStepCancelled, herr)
	}
	return workflow.Permanent(herr)
}
