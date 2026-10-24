package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/ubgo/workflow"
)

// ForceComplete ends a run as succeeded from outside (workflow.RunForcer):
// an unfinished run, or a failed one. Steps still executing are cancelled
// with cause workflow.ErrForced and their results are ignored. It counts as
// a success: the idempotency key is released, Job.Chain hands on to its next
// job, and run-finished observers hear StatusSucceeded, a second time for a
// run that had failed. Any other state returns ErrWrongState; an unknown run
// ErrNotFound.
func (c *Client) ForceComplete(ctx context.Context, ref workflow.RunRef) error {
	return c.force(ctx, ref, func(tx *sql.Tx, row *runRow, o *txOut) error {
		if !workflow.CanForceComplete(runStatus(row.status, nil)) {
			return fmt.Errorf("%w: run %s is %s; only an unfinished or failed run can be force-completed", workflow.ErrWrongState, row.id, runStatus(row.status, nil))
		}
		// notified and notify_until are reset so a failed run's new end is
		// delivered again.
		n, err := c.st.execN(ctx, tx, `UPDATE {runs} SET status = ?, finished_at = ?, idem_active = NULL, err = NULL, fail_token = NULL,
			notified = 0, notify_until = 0 WHERE id = ? AND status IN (?, ?, ?, ?, ?)`,
			runSucceeded, c.nowNanos(), row.id, runScheduled, runPending, runActive, runFailing, runFailed)
		if err != nil || n == 0 {
			return err
		}
		if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_forced} WHERE run_id = ?", row.id); err != nil {
			return err
		}
		if err := c.cancelSteps(ctx, tx, row.id); err != nil {
			return err
		}
		row.status = runSucceeded
		o.ended = append(o.ended, row.id)
		return c.handoffChain(ctx, tx, row, o)
	})
}

// ForceFail ends an unfinished run as failed from outside
// (workflow.RunForcer), with workflow.ForcedFailure(reason) as its error:
// no retry, and OnFailure does not run (a run already failing has its
// OnFailure claim dropped). RunRetrier can run it again. A finished run
// returns ErrWrongState; an unknown run ErrNotFound.
func (c *Client) ForceFail(ctx context.Context, ref workflow.RunRef, reason string) error {
	return c.force(ctx, ref, func(tx *sql.Tx, row *runRow, o *txOut) error {
		if !workflow.CanForceFail(runStatus(row.status, nil)) {
			return fmt.Errorf("%w: run %s is %s; only an unfinished run can be force-failed", workflow.ErrWrongState, row.id, runStatus(row.status, nil))
		}
		changed, err := c.finishRun(ctx, tx, row, runFailed, workflow.ForcedFailure(reason).Error(), o)
		if err != nil || !changed {
			return err
		}
		// The flag, not the message, is what makes the run's error match
		// workflow.ErrForced (runError): a step's own error may begin with
		// the same words.
		if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_forced} WHERE run_id = ?", row.id); err != nil {
			return err
		}
		_, err = c.st.exec(ctx, tx, "INSERT INTO {run_forced} (run_id) VALUES (?)", row.id)
		return err
	})
}

// force runs fn on the touched run in one transaction, then cancels the
// run's local attempts with cause ErrForced and wakes the loops.
func (c *Client) force(ctx context.Context, ref workflow.RunRef, fn func(tx *sql.Tx, row *runRow, o *txOut) error) error {
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return err
	}
	var out txOut
	err = c.st.tx(ctx, func(tx *sql.Tx) error {
		out = txOut{cause: workflow.ErrForced}
		row, err := c.touch(ctx, tx, r.id)
		if err != nil {
			return err
		}
		return fn(tx, row, &out)
	})
	if err != nil {
		return err
	}
	c.afterTx(ctx, out)
	return nil
}

// forcedError rebuilds the error of a run ForceFail ended (run_forced
// holds its flag) from its stored message, so it matches workflow.ErrForced
// again. The message is ForcedFailure's text; only the reason is kept.
func forcedError(msg string) error {
	rest := strings.TrimPrefix(msg, workflow.ErrForced.Error())
	return workflow.ForcedFailure(strings.TrimPrefix(rest, ": "))
}
