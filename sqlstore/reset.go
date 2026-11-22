package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunResetter     = (*Client)(nil)
	_ workflow.RunResetterWith = (*Client)(nil)
	_ workflow.RunForker       = (*Client)(nil)
)

// ResetRun re-runs a failed run from step: step and every step downstream
// of it go back to pending with the steps that had not succeeded, the
// other steps keep their outputs (workflow.RunResetter). The steps table
// does not store the graph, so the client must have the run's workflow
// registered to know which steps are downstream; otherwise it returns an
// error wrapping ErrInvalid.
//
//ds:def id=sql-client-resetrun-prngumah owner=@khanakia stability=stable
func (c *Client) ResetRun(ctx context.Context, ref workflow.RunRef, step string) (workflow.RunRef, error) {
	return c.ResetRunWith(ctx, ref, step, workflow.ResetOptions{})
}

// ResetRunWith is ResetRun that, with opts.Succeeded, also resets a
// succeeded run in place (workflow.RunResetterWith): it gets
// workflow.LabelRedo, in its labels and in the label index, and its chain
// is not handed on again. A succeeded run whose chain still waits for a
// client that registers the next workflow (a pending handoff) returns
// ErrWrongState, since its next success would hand that chain on twice.
//
//ds:def id=sql-client-resetrunwith-hrw54cxc owner=@khanakia stability=stable
func (c *Client) ResetRunWith(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ResetOptions) (workflow.RunRef, error) {
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
	return c.retryFrom(ctx, ref, rerun, opts)
}

// markRedo stamps workflow.LabelRedo on succeeded run row in tx, once,
// after checking that no chain handoff is pending.
//
//ds:def id=sql-client-markredo-yt3wcuhc owner=@khanakia stability=stable
func (c *Client) markRedo(ctx context.Context, tx *sql.Tx, row *runRow) error {
	var n int64
	if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {run_chain} WHERE run_id = ?", []any{row.id}, &n); err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("%w: run %s succeeded but its chain is not handed on yet (no client registers the next workflow); reset it once the chain has moved on", workflow.ErrWrongState, row.id)
	}
	labels, err := row.labelMap()
	if err != nil {
		return err
	}
	if labels[workflow.LabelRedo] == "true" {
		return nil
	}
	if labels == nil {
		labels = map[string]string{}
	}
	labels[workflow.LabelRedo] = "true"
	b, err := json.Marshal(labels)
	if err != nil {
		return fmt.Errorf("sqlstore: encode labels: %w", err)
	}
	if _, err := c.st.exec(ctx, tx, "UPDATE {runs} SET labels = ? WHERE id = ?", string(b), row.id); err != nil {
		return err
	}
	return c.insertLabels(ctx, tx, row.id, map[string]string{workflow.LabelRedo: "true"})
}

// ForkRun starts a new run from a finished one at step
// (workflow.RunForker): one transaction inserts the fork as a pending run
// with a row per step, those the original succeeded in and that step does
// not reach marked succeeded with the original's outputs, so admission
// resumes it from there as it resumes a retried run. The client must
// register the run's workflow at the original's version (the steps table
// does not store the graph, and the kept outputs came from that
// version's steps); otherwise it returns an error wrapping ErrInvalid. A
// fork key held by an unfinished run returns that run's RunRef and starts
// nothing, as Enqueue does.
//
//ds:def id=sql-client-forkrun-dax2n4ug owner=@khanakia stability=stable
func (c *Client) ForkRun(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ForkOptions) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckFork(step, opts); err != nil {
		return workflow.RunRef{}, err
	}
	if err := c.usable(); err != nil {
		return workflow.RunRef{}, err
	}
	orig, err := c.lookup(ctx, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if !isTerminalRun(orig.status) {
		return workflow.RunRef{}, fmt.Errorf("%w: run %s is %s; only a finished run can be forked", workflow.ErrWrongState, orig.id, orig.status)
	}
	reg, err := c.runsRun(ctx, c.st.db, orig)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if reg == nil {
		return workflow.RunRef{}, fmt.Errorf("%w: ForkRun: workflow %s at the run's version is not registered on this client, so its steps are unknown", workflow.ErrInvalid, orig.workflow)
	}
	rerun := workflow.StepsFrom(reg.wf, step)
	if rerun == nil {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %s has no step %q", workflow.ErrInvalid, orig.workflow, step)
	}
	job, kept, err := c.forkJob(ctx, orig, rerun, step, opts)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if err := c.checkJob(reg, job); err != nil {
		return workflow.RunRef{}, err
	}
	key, err := idemKey(reg, job)
	if err != nil {
		return workflow.RunRef{}, err
	}
	for range maxIdemAttempts {
		var row *runRow
		var held string
		err := c.st.tx(ctx, func(tx *sql.Tx) error {
			var err error
			if row, err = c.insertRun(ctx, tx, reg, job, key, ""); err != nil {
				return err
			}
			held = row.id
			c.holdEvents(held)
			return c.insertForkSteps(ctx, tx, reg, row, kept)
		})
		if err == nil {
			c.releaseEvents(ctx, held, c.insertedEvent(row, job, runQueue(reg, job)))
			if err := c.admit(ctx, reg); err != nil {
				return row.ref(), err
			}
			c.nudge()
			workflow.MarkInserted(ctx)
			return row.ref(), nil
		}
		if held != "" {
			c.releaseEvents(ctx, held, nil)
		}
		if key == "" || !c.st.isUnique(err) {
			return workflow.RunRef{}, err
		}
		var holder runRow
		err = c.st.one(ctx, c.st.db, "SELECT "+runCols+" FROM {runs} WHERE workflow = ? AND idem_active = ?",
			[]any{reg.wf.Name, key}, holder.targets()...)
		if err == nil {
			workflow.MarkDuplicate(ctx)
			return holder.ref(), nil
		}
		if !errors.Is(err, sqlb.ErrNoRows) {
			return workflow.RunRef{}, fmt.Errorf("sqlstore: read idempotency holder: %w", err)
		}
	}
	return workflow.RunRef{}, fmt.Errorf("%w: key %q of workflow %q kept changing hands", workflow.ErrDuplicate, key, reg.wf.Name)
}

// forkJob rebuilds orig's job for its fork (payload, labels through
// workflow.ForkLabels, priority, queue, job retry policy, tenant, the
// fork's own key, no chain) and reads the outputs of the steps the fork
// keeps: those orig succeeded in that are not in rerun.
func (c *Client) forkJob(ctx context.Context, orig *runRow, rerun []string, step string, opts workflow.ForkOptions) (workflow.Job, map[string][]byte, error) {
	labels, err := orig.labelMap()
	if err != nil {
		return workflow.Job{}, nil, err
	}
	var queue string
	var retry sql.NullString
	if err := c.st.one(ctx, c.st.db, "SELECT queue, retry FROM {run_ext} WHERE run_id = ?", []any{orig.id}, &queue, &retry); err != nil && !isNoRows(err) {
		return workflow.Job{}, nil, fmt.Errorf("sqlstore: read the run's queue: %w", err)
	}
	policy, err := decodeRetry(retry)
	if err != nil {
		return workflow.Job{}, nil, err
	}
	var tenant sql.NullString
	if err := c.st.one(ctx, c.st.db, "SELECT tenant FROM {run_info} WHERE run_id = ?", []any{orig.id}, &tenant); err != nil && !isNoRows(err) {
		return workflow.Job{}, nil, fmt.Errorf("sqlstore: read the run's tenant: %w", err)
	}
	st := runStatus(orig.status, nil) // terminal: the step counts are not read
	job := workflow.Job{
		Workflow:       orig.workflow,
		Payload:        orig.payload,
		Labels:         workflow.ForkLabels(labels, opts.Labels, workflow.RunID(orig.id), step, workflow.ForkRedo(st, labels)),
		Priority:       int(orig.priority),
		Queue:          queue,
		Retry:          policy,
		Tenant:         tenant.String,
		IdempotencyKey: opts.IdempotencyKey,
	}
	kept := map[string][]byte{}
	var name, status string
	var out []byte
	err = c.st.each(ctx, c.st.db, "SELECT name, status, out_data FROM {steps} WHERE run_id = ?", []any{orig.id},
		func() error {
			if status == stepSucceeded && !slices.Contains(rerun, name) {
				kept[name] = append([]byte(nil), out...)
			}
			return nil
		}, &name, &status, &out)
	if err != nil {
		return workflow.Job{}, nil, fmt.Errorf("sqlstore: read the run's steps: %w", err)
	}
	return job, kept, nil
}

// insertForkSteps writes a row per step of the fork row in tx: succeeded
// with its output for the steps in kept, pending for the rest. Admission
// then finds rows and resumes the run (activate).
func (c *Client) insertForkSteps(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow, kept map[string][]byte) error {
	for _, s := range reg.wf.Steps {
		stepID, err := c.st.newID(IDStep)
		if err != nil {
			return err
		}
		status, out := stepPending, []byte(nil)
		if o, ok := kept[s.Name]; ok {
			status, out = stepSucceeded, o
		}
		if _, err := c.st.exec(ctx, tx, `INSERT INTO {steps} (id, run_id, run_seq, workflow, name, status, out_data, attempts, failures, reclaims,
			due, lease_until, priority, wait_event, wait_key) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, ?, '', '')`,
			stepID, row.id, row.seq, row.workflow, s.Name, status, out, row.priority); err != nil {
			return err
		}
	}
	return nil
}
