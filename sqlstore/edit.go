package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ubgo/workflow"
)

var _ workflow.RunEditor = (*Client)(nil)

// EditRun changes a scheduled run that has not started: payload, priority
// and start (workflow.RunEditor), in one transaction that only touches a
// row still in the scheduled state, so an edit racing the run's admission
// on another node either lands before it or returns ErrWrongState. The
// payload limit (WithPayloadLimit) applies to a new payload, and the run's
// concurrency and rate keys are computed again from it (left for the
// admitting worker when this client does not register the workflow, as
// an enqueue-only client's runs are). A RunAt of now or earlier promotes
// the row to pending in the same statement, so the returned ref carries
// the RunID.
//
//ds:def id=sql-client-editrun-fwr9u4yr owner=@khanakia stability=stable
func (c *Client) EditRun(ctx context.Context, ref workflow.RunRef, edit workflow.JobEdit) (workflow.RunRef, error) {
	if err := workflow.CheckJobEdit(edit); err != nil {
		return workflow.RunRef{}, err
	}
	if err := c.usable(); err != nil {
		return workflow.RunRef{}, err
	}
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if r.status != runScheduled {
		return workflow.RunRef{}, fmt.Errorf("%w: run %s is %s, not scheduled", workflow.ErrWrongState, r.id, r.status)
	}
	job, err := r.job()
	if err != nil {
		return workflow.RunRef{}, err
	}
	if edit.Payload != nil {
		job.Payload = *edit.Payload
		if err := workflow.CheckPayload(job, c.payloadLimit, "driver"); err != nil {
			return workflow.RunRef{}, err
		}
	}
	if edit.Priority != nil {
		job.Priority = *edit.Priority
	}
	bucket, rk := r.bucket, r.rateKeys
	if edit.Payload != nil {
		bucket, rk = "", unevaluatedKeys
		if reg := c.reg(r.workflow); reg != nil {
			if bucket, rk, err = runKeys(reg, job); err != nil {
				return workflow.RunRef{}, err
			}
		}
	}
	stmt := "UPDATE {runs} SET payload = ?, priority = ?, bucket = ?, rate_keys = ?"
	args := []any{job.Payload, job.Priority, bucket, rk}
	due := false // the edit moves the start to now: promote the row here
	if edit.RunAt != nil {
		now := c.nowNanos()
		runAt := edit.RunAt.UnixNano()
		if edit.RunAt.IsZero() || runAt <= now {
			runAt, due = now, true
		}
		stmt += ", run_at = ?"
		args = append(args, runAt)
		if due {
			// What promote does for a row whose RunAt has come, done in the
			// same guarded UPDATE, so the returned ref carries the RunID
			// (the workflow.RunEditor promise) and a worker admits it.
			stmt += ", status = ?"
			args = append(args, runPending)
		}
	}
	args = append(args, r.id, runScheduled)
	err = c.st.tx(ctx, func(tx *sql.Tx) error {
		n, err := c.st.execN(ctx, tx, stmt+" WHERE id = ? AND status = ?", args...)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: run %s is no longer scheduled", workflow.ErrWrongState, r.id)
		}
		return nil
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	c.nudge()
	if due {
		r.status = runPending
	}
	return r.ref(), nil
}
