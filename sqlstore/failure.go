package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// recordFailure decides what a failed attempt a of step means, inside the
// record transaction: retry it after a delay, or fail the run. The policy
// is the run's Job.Retry when it was enqueued with one, else the step's or
// the workflow's (workflow.EffectiveRetriesFor), and the decision is
// workflow.RetryPolicy.Next over the step's stored attempt counts, so Max,
// MaxErrors and Until are judged the way every driver judges them.
//
// steps.failures is Attempts.N (failed attempts, releases included; an
// attempt cut off by a stop or a dead worker is not one), step_ext holds
// Errors and First. First is the start of the step's first failed
// attempt: an earlier attempt whose worker died before it could record
// anything left no trace, so a takeover's start stands in for it.
func (c *Client) recordFailure(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow, step workflow.Step, a *attempt, herr error, o *txOut) error {
	override, err := c.runRetry(ctx, tx, row.id)
	if err != nil {
		return err
	}
	policy := workflow.EffectiveRetriesFor(step, reg.wf, workflow.Job{Retry: override})
	var errs, first int64
	err = c.st.one(ctx, tx, "SELECT errors, first_at FROM {step_ext} WHERE step_id = ?", []any{a.stepID}, &errs, &first)
	if err != nil && !errors.Is(err, sqlb.ErrNoRows) {
		return fmt.Errorf("sqlstore: read attempt counts: %w", err)
	}
	if first == 0 {
		first = a.started
	}
	if err := c.addAttemptError(ctx, tx, a, herr); err != nil {
		return err
	}
	att := workflow.Attempts{N: int(a.failures), Errors: int(errs), First: unixTime(first)}.Count(herr)
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {step_ext} WHERE step_id = ?", a.stepID); err != nil {
		return err
	}
	if _, err := c.st.exec(ctx, tx, "INSERT INTO {step_ext} (step_id, errors, first_at) VALUES (?, ?, ?)", a.stepID, att.Errors, first); err != nil {
		return err
	}
	now := c.clk.Now()
	if delay, retry := policy.Next(herr, att, now, c.rnd); retry {
		o.changed = true
		o.events = append(o.events, workflow.LifecycleEvent{Kind: workflow.LifecycleRetrying, Ref: workflow.RunRef{RunID: workflow.RunID(a.runID)},
			Workflow: a.workflow, Queue: a.queue, Tenant: a.tenant, Step: a.name, Attempt: int(a.attempts), Status: workflow.StatusRetrying, Err: herr})
		if _, err := c.st.exec(ctx, tx, "DELETE FROM {step_err} WHERE step_id = ?", a.stepID); err != nil {
			return err
		}
		if _, err := c.st.exec(ctx, tx, "INSERT INTO {step_err} (step_id, err) VALUES (?, ?)", a.stepID, herr.Error()); err != nil {
			return err
		}
		_, err := c.st.exec(ctx, tx, `UPDATE {steps} SET status = ?, due = ?, lease_token = NULL, lease_until = 0, failures = ?, reclaims = 0
			WHERE id = ?`, stepReady, now.Add(delay).UnixNano(), att.N, a.stepID)
		return err
	}
	if _, err := c.st.exec(ctx, tx, "UPDATE {steps} SET failures = ? WHERE id = ?", att.N, a.stepID); err != nil {
		return err
	}
	if derr := policy.ExhaustedError(att.N, herr); derr != nil {
		return c.discardRun(ctx, tx, row, a.name, derr, o)
	}
	msg := fmt.Sprintf("sqlstore: step %q failed after %d attempt(s): %v", a.name, att.N, herr)
	if err := c.markFailing(ctx, tx, row, a.name, a.in, msg, o); err != nil {
		return err
	}
	return c.setTimeoutKind(ctx, tx, row.id, timeoutKindOf(herr))
}

// runRetry reads run id's Job.Retry override; nil when it has none.
func (c *Client) runRetry(ctx context.Context, x sqlb.DBTX, id string) (*workflow.RetryPolicy, error) {
	var raw sql.NullString
	err := c.st.one(ctx, x, "SELECT retry FROM {run_ext} WHERE run_id = ?", []any{id}, &raw)
	if errors.Is(err, sqlb.ErrNoRows) {
		return nil, nil // a run from before migration 003
	}
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read the run's retry policy: %w", err)
	}
	return decodeRetry(raw)
}

// discardRun ends row discarded because step returned workflow.Discard:
// no retry, no OnFailure, its key released, and an Err (in RunEvent and
// RunInfo) matching workflow.ErrDiscarded and workflow.ErrCancelled. The
// stored status is workflow.EndStatus's answer for this driver's
// capabilities (StatusDiscarded under CapRunStates), so every driver maps
// a discard the same way. run_ext.discarded is still set, for readers of
// rows written before migration 004.
func (c *Client) discardRun(ctx context.Context, tx *sql.Tx, row *runRow, step string, herr error, o *txOut) error {
	changed, err := c.finishRun(ctx, tx, row, string(workflow.EndStatus(caps, herr)), workflow.DiscardedError(step, herr).Error(), o)
	if err != nil || !changed {
		return err
	}
	_, err = c.st.exec(ctx, tx, "UPDATE {run_ext} SET discarded = 1 WHERE run_id = ?", row.id)
	return err
}

// discardedError is a discarded run's Err rebuilt from its stored message:
// it matches workflow.ErrDiscarded and workflow.ErrCancelled, as
// workflow.DiscardedError does before it is stored.
type discardedError string

func (e discardedError) Error() string { return string(e) }

// Is makes errors.Is match both sentinels a discard carries.
func (e discardedError) Is(target error) bool {
	return target == workflow.ErrDiscarded || target == workflow.ErrCancelled
}

// runError is the Err of a finished run as observers see it, rebuilt from
// what the runs table stores; forced is the run_forced flag. A failed run's
// is a *workflow.RunError naming the run, its workflow and the failing step
// around the stored message.
func runError(id, wf, step, status, msg, kind string, discarded, forced bool) error {
	switch {
	case status == runFailed && forced:
		return failedRunError(id, wf, step, forcedError(msg))
	case status == runFailed:
		return failedRunError(id, wf, step, failureCause(msg, kind))
	case status == runDiscarded, status == runCancelled && discarded:
		return discardedError(msg)
	case status == runCancelled:
		return fmt.Errorf("%w: %s", workflow.ErrCancelled, msg)
	}
	return nil
}

// failedRunError wraps a failed run's cause in a *workflow.RunError, so
// the error (RunEvent.Err) and its text (RunInfo.Error) name the run.
func failedRunError(id, wf, step string, cause error) error {
	return &workflow.RunError{RunID: workflow.RunID(id), Workflow: wf, Step: step, Err: cause}
}

// snoozeStep puts a's step back for d in tx (workflow.Snooze): claimable
// again at now+d as the same attempt (the attempt is refunded), not
// counted against the retry policy and not recorded as an error; the run
// reports Waiting meanwhile.
func (c *Client) snoozeStep(ctx context.Context, tx *sql.Tx, a *attempt, d time.Duration, herr error, o *txOut) error {
	due := c.clk.Now().Add(d)
	if _, err := c.st.exec(ctx, tx, `UPDATE {steps} SET status = ?, due = ?, lease_token = NULL, lease_until = 0, attempts = attempts - 1, reclaims = 0
		WHERE id = ?`, stepSnoozed, due.UnixNano(), a.stepID); err != nil {
		return err
	}
	o.changed = true
	o.events = append(o.events, workflow.LifecycleEvent{Kind: workflow.LifecycleSnoozed, Ref: workflow.RunRef{RunID: workflow.RunID(a.runID)},
		Workflow: a.workflow, Queue: a.queue, Tenant: a.tenant, Step: a.name, Attempt: int(a.attempts), Status: workflow.StatusWaiting, Err: herr})
	return nil
}
