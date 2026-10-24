package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ubgo/workflow"
)

// Timeout kinds as stored in run_info.timeout: which deadline failed the
// run, so OnTimeout is chosen over OnFailure (workflow.FailureHandler) and
// the run's error matches its sentinel again after a restart. Empty: the
// run did not fail on a deadline.
const (
	timeoutNone      = ""
	timeoutRun       = "run"
	timeoutStep      = "step"
	timeoutWait      = "wait"
	timeoutHeartbeat = "heartbeat"
)

// timeoutKinds maps each stored kind to its sentinel, in the order
// timeoutKindOf checks them (the specific sentinels before the general).
var timeoutKinds = []struct {
	kind     string
	sentinel error
}{
	{timeoutRun, workflow.ErrRunTimeout},
	{timeoutHeartbeat, workflow.ErrHeartbeatTimeout},
	{timeoutWait, workflow.ErrWaitTimeout},
	{timeoutStep, workflow.ErrStepTimeout},
}

// timeoutKindOf is the stored kind of err: the first sentinel it matches,
// "step" for any other timeout, empty when it is not one.
func timeoutKindOf(err error) string {
	if !workflow.IsTimeout(err) {
		return timeoutNone
	}
	for _, k := range timeoutKinds {
		if errors.Is(err, k.sentinel) {
			return k.kind
		}
	}
	return timeoutStep
}

// storedTimeout is a failed run's error rebuilt from its stored message and
// timeout kind: the same text, matching workflow.ErrTimeout and the kind's
// sentinel.
type storedTimeout struct {
	msg  string
	kind error
}

func (e storedTimeout) Error() string { return e.msg }

// Is matches ErrTimeout and the specific sentinel.
func (e storedTimeout) Is(target error) bool {
	return target == workflow.ErrTimeout || target == e.kind
}

// failureCause is the error a failed run's message and timeout kind stand
// for.
func failureCause(msg, kind string) error {
	for _, k := range timeoutKinds {
		if k.kind == kind {
			return storedTimeout{msg: msg, kind: k.sentinel}
		}
	}
	return storedError(msg)
}

// setTimeoutKind records in tx which deadline failed run id.
func (c *Client) setTimeoutKind(ctx context.Context, tx *sql.Tx, id, kind string) error {
	if kind == timeoutNone {
		return nil
	}
	_, err := c.st.exec(ctx, tx, "UPDATE {run_info} SET timeout = ? WHERE run_id = ?", kind, id)
	return err
}

// armRunTimeout starts reg's Workflow.RunTimeout for row at its admission.
func (c *Client) armRunTimeout(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow) error {
	if reg.wf.RunTimeout <= 0 {
		return nil
	}
	_, err := c.st.exec(ctx, tx, "UPDATE {run_info} SET deadline = ? WHERE run_id = ?", c.nowNanos()+int64(reg.wf.RunTimeout), row.id)
	return err
}

// expireRuns fails the active runs of this client's workflows whose
// Workflow.RunTimeout passed: the error matches workflow.ErrRunTimeout,
// nothing is retried, and the attempts running here are cancelled with
// that cause (elsewhere at their next lease renewal).
func (c *Client) expireRuns(ctx context.Context) error {
	var ids []string
	var id string
	err := c.st.each(ctx, c.st.db, "SELECT r.id FROM {runs} r JOIN {run_info} i ON i.run_id = r.id WHERE r."+c.inNames()+
		" AND r.status = ? AND i.deadline > 0 AND i.deadline <= ? LIMIT ?",
		c.nameArgs(runActive, c.nowNanos(), c.fetch), func() error { ids = append(ids, id); return nil }, &id)
	if err != nil {
		return err
	}
	for _, id := range ids {
		var o txOut
		err := c.st.tx(ctx, func(tx *sql.Tx) error {
			o = txOut{cause: workflow.ErrRunTimeout}
			row, err := c.touch(ctx, tx, id)
			if err != nil || row.status != runActive {
				return err
			}
			reg := c.reg(row.workflow)
			msg := workflow.TimeoutError(workflow.ErrRunTimeout, fmt.Sprintf("run %s of workflow %q passed its RunTimeout of %s", row.id, row.workflow, reg.wf.RunTimeout)).Error()
			if err := c.markFailing(ctx, tx, row, "", nil, msg, &o); err != nil {
				return err
			}
			return c.setTimeoutKind(ctx, tx, row.id, timeoutRun)
		})
		if err != nil {
			return err
		}
		c.afterTx(ctx, o)
	}
	return nil
}
