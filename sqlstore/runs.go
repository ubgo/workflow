package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// runCols is the column list runRow.targets scans, in order.
const runCols = "seq, id, scheduled_id, workflow, payload, labels, priority, bucket, rate_keys, status, sig_floor, finished_at, err"

// runRow is one row of the runs table, as the driver reads it.
type runRow struct {
	seq        int64
	id         string
	sid        sql.NullString
	workflow   string
	payload    []byte
	labels     string
	priority   int64
	bucket     string
	rateKeys   string
	status     string
	sigFloor   int64
	finishedAt int64
	errMsg     sql.NullString
}

func (r *runRow) targets() []any {
	return []any{&r.seq, &r.id, &r.sid, &r.workflow, &r.payload, &r.labels, &r.priority, &r.bucket, &r.rateKeys,
		&r.status, &r.sigFloor, &r.finishedAt, &r.errMsg}
}

// ref is the RunRef a caller sees: only the ScheduledID while a delayed
// run waits for RunAt, both IDs once it has started, as workflow.RunRef
// documents.
func (r *runRow) ref() workflow.RunRef {
	ref := workflow.RunRef{ScheduledID: workflow.ScheduledID(r.sid.String)}
	if r.status != runScheduled {
		ref.RunID = workflow.RunID(r.id)
	}
	return ref
}

// labelMap decodes the stored labels. They were encoded by this package
// from a map[string]string, so a decode error means the row was edited by
// hand; the run then proceeds without labels and the error is reported.
func (r *runRow) labelMap() (map[string]string, error) {
	var m map[string]string
	if err := json.Unmarshal([]byte(r.labels), &m); err != nil {
		return nil, fmt.Errorf("sqlstore: run %s labels: %w", r.id, err)
	}
	return m, nil
}

// job rebuilds the Job a run was enqueued with, for key functions
// (WaitSpec.KeyFrom) evaluated after the enqueue.
func (r *runRow) job() (workflow.Job, error) {
	labels, err := r.labelMap()
	return workflow.Job{Workflow: r.workflow, Payload: r.payload, Labels: labels, Priority: int(r.priority)}, err
}

func isTerminalRun(status string) bool {
	return status == runSucceeded || status == runFailed || status == runCancelled || status == runDiscarded
}

// loadRun reads the run whose column col equals v.
func (c *Client) loadRun(ctx context.Context, x sqlb.DBTX, col string, v any) (*runRow, error) {
	var r runRow
	err := c.st.one(ctx, x, "SELECT "+runCols+" FROM {runs} WHERE "+col+" = ?", []any{v}, r.targets()...)
	if errors.Is(err, sqlb.ErrNoRows) {
		return nil, fmt.Errorf("%w: run %s=%v", workflow.ErrNotFound, col, v)
	}
	if err != nil {
		return nil, fmt.Errorf("sqlstore: load run: %w", err)
	}
	return &r, nil
}

// touch locks run id for the rest of tx and reads it. Every transaction
// that changes a run's steps or status starts here, so two of them on one
// run are serialised: Postgres and MySQL hold the row lock until commit,
// and SQLite has one writer. Without it, two parents finishing at once on
// two workers could each miss the other and never start their child.
func (c *Client) touch(ctx context.Context, tx *sql.Tx, id string) (*runRow, error) {
	n, err := c.st.execN(ctx, tx, "UPDATE {runs} SET ver = ver + 1 WHERE id = ?", id)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, fmt.Errorf("%w: run %s", workflow.ErrNotFound, id)
	}
	return c.loadRun(ctx, tx, "id", id)
}

// lookup finds the run ref points at, by RunID first and then by
// ScheduledID, so a caller keeps passing the RunRef Enqueue returned.
func (c *Client) lookup(ctx context.Context, ref workflow.RunRef) (*runRow, error) {
	if !ref.RunID.IsZero() {
		r, err := c.loadRun(ctx, c.st.db, "id", string(ref.RunID))
		if err == nil || !errors.Is(err, workflow.ErrNotFound) {
			return r, err
		}
	}
	if !ref.ScheduledID.IsZero() {
		return c.loadRun(ctx, c.st.db, "scheduled_id", string(ref.ScheduledID))
	}
	return nil, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
}

// insertRun writes a new run in tx: scheduled when job.RunAt is after now,
// pending otherwise. key is the idempotency key after the workflow's
// policy (empty: none); a key held by an unfinished run of the workflow
// makes the insert fail with a unique violation. id is the RunID; empty
// asks the ID generator for one.
func (c *Client) insertRun(ctx context.Context, tx *sql.Tx, reg *registered, job workflow.Job, key, id string) (*runRow, error) {
	if id == "" {
		var err error
		if id, err = c.st.newID(IDRun); err != nil {
			return nil, err
		}
	}
	now := c.nowNanos()
	status, runAt := runPending, now
	var sid, idem any // NULL unless set
	if !job.RunAt.IsZero() && job.RunAt.UnixNano() > now {
		scheduled, err := c.st.newID(IDScheduled)
		if err != nil {
			return nil, err
		}
		status, runAt, sid = runScheduled, job.RunAt.UnixNano(), scheduled
	}
	if key != "" {
		idem = key
	}
	labels, err := json.Marshal(nonNil(job.Labels))
	if err != nil {
		return nil, fmt.Errorf("sqlstore: encode labels: %w", err)
	}
	bucket, rk := "", unevaluatedKeys // a stand-in's keys are the admitting worker's (evaluateKeys)
	if !reg.stub {
		if bucket, rk, err = runKeys(reg, job); err != nil {
			return nil, err
		}
	}
	// The signal floor is read by the INSERT itself, so the statement that
	// opens the transaction is a write: SQLite then takes its write lock
	// up front (waiting out busy_timeout) instead of failing a read-to-write
	// upgrade with SQLITE_BUSY.
	_, err = c.st.exec(ctx, tx, `INSERT INTO {runs} (id, scheduled_id, workflow, payload, labels, idem_active, priority, bucket,
		rate_keys, status, run_at, sig_floor, created_at, finished_at, fail_until, notified, notify_until, ver)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(id), 0) FROM {signals}), ?, 0, 0, 0, 0, 0)`,
		id, sid, reg.wf.Name, job.Payload, string(labels), idem, job.Priority, bucket, rk, status, runAt, now)
	if err != nil {
		return nil, err
	}
	if err := c.insertRunExt(ctx, tx, reg, job, key, id); err != nil {
		return nil, err
	}
	if err := c.insertRunInfo(ctx, tx, job, id); err != nil {
		return nil, err
	}
	if err := c.insertLabels(ctx, tx, id, job.Labels); err != nil {
		return nil, err
	}
	if len(job.Chain) > 0 {
		if err := c.saveChain(ctx, tx, id, job.Chain); err != nil {
			return nil, err
		}
	}
	return c.loadRun(ctx, tx, "id", id)
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// maxIdemAttempts bounds the enqueue loop that races a finishing holder of
// the same idempotency key: insert fails, the holder ends before it is
// read, insert again.
const maxIdemAttempts = 5

// Enqueue starts a run now, or at job.RunAt. A key held by an unfinished
// run returns that run's RunRef and starts nothing; the unique index on
// (workflow, idem_active) makes that atomic across nodes.
func (c *Client) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := c.usable(); err != nil {
		return workflow.RunRef{}, err
	}
	reg, err := c.enqueueReg(job.Workflow)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJobLabels(ctx, job); err != nil {
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
		var out txOut
		var held string // the run whose lifecycle events wait for its Inserted
		err := c.st.tx(ctx, func(tx *sql.Tx) error {
			var err error
			out = txOut{}
			if row, err = c.insertRun(ctx, tx, reg, job, key, ""); err != nil {
				return err
			}
			held = row.id
			c.holdEvents(held)
			return c.admitInline(ctx, tx, reg, row, &out)
		})
		if err == nil {
			c.releaseEvents(ctx, held, c.insertedEvent(row, job, runQueue(reg, job)))
		} else if held != "" {
			c.releaseEvents(ctx, held, nil)
		}
		if err == nil {
			if row.status == runPending {
				if err := c.admitEnqueued(ctx, reg); err != nil {
					return workflow.RunRef{}, err
				}
			}
			c.afterTx(ctx, out)
			c.nudge()
			workflow.MarkInserted(ctx)
			return row.ref(), nil
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
		// The holder finished between the insert and the read: try again.
	}
	return workflow.RunRef{}, fmt.Errorf("%w: key %q of workflow %q kept changing hands", workflow.ErrDuplicate, key, reg.wf.Name)
}

// idemKey is job's idempotency key after the workflow's policy: empty when
// the policy is off or no key was given, an error when the policy requires
// one and none was given.
func idemKey(reg *registered, job workflow.Job) (string, error) {
	switch reg.wf.Idempotency {
	case workflow.IdempotencyOff:
		return "", nil
	case workflow.IdempotencyRequired:
		if job.IdempotencyKey == "" {
			return "", fmt.Errorf("%w: workflow %q requires an IdempotencyKey", workflow.ErrInvalid, reg.wf.Name)
		}
	}
	return job.IdempotencyKey, nil
}

// Cancel stops a run or a delayed start. Finished runs are left alone.
// Steps running on this client stop at once; on another client they stop
// at its next lease renewal (a third of the lease).
func (c *Client) Cancel(ctx context.Context, ref workflow.RunRef) error {
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return err
	}
	var out txOut
	err = c.st.tx(ctx, func(tx *sql.Tx) error {
		out = txOut{}
		row, err := c.touch(ctx, tx, r.id)
		if err != nil {
			return err
		}
		_, err = c.finishRun(ctx, tx, row, runCancelled, workflow.ErrCancelled.Error(), &out)
		return err
	})
	if err != nil {
		return err
	}
	c.afterTx(ctx, out)
	return nil
}

// Status reports where a run is. An admitted run is Running while a step
// is claimed, Retrying while a step waits on its retry backoff, Queued
// while a step waits for a worker, and Waiting while only waits and sleeps
// are in progress (stepStatus).
func (c *Client) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return "", err
	}
	if r.status != runActive {
		return runStatus(r.status, nil), nil
	}
	counts := map[string]int64{}
	var st string
	var backoff, n int64
	err = c.st.each(ctx, c.st.db, "SELECT "+stepCountCols+", COUNT(*) FROM {steps} WHERE run_id = ? GROUP BY 1, 2", []any{c.nowNanos(), r.id},
		func() error { counts[countKey(st, backoff)] += n; return nil }, &st, &backoff, &n)
	if err != nil {
		return "", fmt.Errorf("sqlstore: step status: %w", err)
	}
	return stepStatus(counts), nil
}

// Signal stores ev and resumes every run of a workflow registered here
// that waits on its name and key. Runs of workflows registered on other
// clients take it on those clients' next poll; a run that reaches its wait
// later takes it then (the look-back Client.Signal documents).
func (c *Client) Signal(ctx context.Context, ev workflow.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.usable(); err != nil {
		return err
	}
	if _, err := c.st.exec(ctx, c.st.db, "INSERT INTO {signals} (name, ev_key, payload, created_at) VALUES (?, ?, ?, ?)",
		ev.Name, ev.Key, ev.Payload, c.nowNanos()); err != nil {
		return err
	}
	if c.enqueueOnly {
		c.nudge() // the workers deliver it
		return nil
	}
	if err := c.deliver(ctx); err != nil {
		return err
	}
	c.nudge()
	return nil
}

// txOut collects what a transaction decided that must happen after it
// commits: local attempts to cancel, failures whose OnFailure must run.
type txOut struct {
	ended []string // runs that left the active state
	// finished are the runs this transaction made terminal, and events
	// the attempt events it decided, both reported after the commit.
	finished []ended
	events   []workflow.LifecycleEvent
	failing  []failClaim
	changed  bool // something became due; wake the loops
	// cause is the context cause of the ended runs' local attempts; nil
	// means workflow.ErrCancelled.
	cause error
}

// failClaim is a failing run this client holds the right to finish.
type failClaim struct {
	runID, token string
}

// afterTx carries out a committed transaction's decisions.
func (c *Client) afterTx(ctx context.Context, out txOut) {
	for _, id := range out.ended {
		c.cancelLocal(id, out.cause)
	}
	for _, ev := range out.events {
		c.emit(ctx, ev)
	}
	for _, e := range out.finished {
		if e.schedule != "" {
			c.keepAfterEnd(ctx, e.schedule)
		}
	}
	for _, e := range out.finished {
		c.drainTrack.RunFinished(workflow.RunID(e.id))
	}
	c.emitFinished(ctx, out.finished)
	for _, f := range out.failing {
		c.finalizeFailure(ctx, f)
	}
	if out.changed || len(out.ended) > 0 || len(out.failing) > 0 {
		c.nudge()
	}
}

// finishRun makes row terminal with status, once: it releases the
// idempotency key and cancels every unfinished step. It reports whether it
// changed anything; a run already terminal is left alone, which is what
// makes Cancel idempotent.
func (c *Client) finishRun(ctx context.Context, tx *sql.Tx, row *runRow, status, msg string, out *txOut) (bool, error) {
	var errMsg any
	if status != runSucceeded {
		errMsg = msg
	}
	n, err := c.st.execN(ctx, tx, `UPDATE {runs} SET status = ?, finished_at = ?, idem_active = NULL, err = ?, fail_token = NULL
		WHERE id = ? AND status IN (?, ?, ?, ?)`,
		status, c.nowNanos(), errMsg, row.id, runScheduled, runPending, runActive, runFailing)
	if err != nil || n == 0 {
		return false, err
	}
	if err := c.cancelSteps(ctx, tx, row.id); err != nil {
		return false, err
	}
	row.status = status
	out.ended = append(out.ended, row.id)
	out.finished = append(out.finished, ended{id: row.id, workflow: row.workflow, status: status, msg: msg, schedule: row.scheduleLabel()})
	return true, nil
}

// cancelSteps marks every unfinished step of run cancelled, which also
// ends its lease: a worker holding one sees that at its next renewal.
func (c *Client) cancelSteps(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := c.st.exec(ctx, tx, `UPDATE {steps} SET status = ?, lease_token = NULL
		WHERE run_id = ? AND status IN (?, ?, ?, ?, ?, ?)`,
		stepCancelled, runID, stepPending, stepReady, stepRunning, stepSleeping, stepWaiting, stepSnoozed)
	return err
}

// markFailing records that step failed for good: the run becomes failing,
// its other steps are cancelled, and this client holds the right to run
// OnFailure and then mark it failed (finalizeFailure). The right is a
// lease, so a crash before the end hands it to another client.
func (c *Client) markFailing(ctx context.Context, tx *sql.Tx, row *runRow, step string, in []byte, msg string, out *txOut) error {
	token := randomID("fail")
	n, err := c.st.execN(ctx, tx, `UPDATE {runs} SET status = ?, err = ?, fail_step = ?, fail_input = ?, fail_token = ?, fail_until = ?
		WHERE id = ? AND status = ?`,
		runFailing, msg, step, in, token, c.leaseNanos()+int64(c.lease), row.id, runActive)
	if err != nil || n == 0 {
		return err
	}
	if _, err := c.st.exec(ctx, tx, "UPDATE {steps} SET status = ?, lease_token = NULL WHERE run_id = ? AND name = ?",
		stepFailed, row.id, step); err != nil {
		return err
	}
	if err := c.cancelSteps(ctx, tx, row.id); err != nil {
		return err
	}
	row.status = runFailing
	out.ended = append(out.ended, row.id)
	out.failing = append(out.failing, failClaim{runID: row.id, token: token})
	return nil
}

// finalizeFailure runs the workflow's OnFailure once with the failing
// step's input, then marks the run failed, unless a Cancel got there first
// or another client took the claim over after it expired.
func (c *Client) finalizeFailure(ctx context.Context, f failClaim) {
	var wfName string
	var input []byte
	var msg, step, kind sql.NullString
	err := c.st.one(ctx, c.st.db, "SELECT r.workflow, r.fail_input, r.err, r.fail_step, i.timeout FROM {runs} r LEFT JOIN {run_info} i ON i.run_id = r.id"+
		" WHERE r.id = ? AND r.status = ? AND r.fail_token = ?",
		[]any{f.runID, runFailing, f.token}, &wfName, &input, &msg, &step, &kind)
	if err != nil {
		if !errors.Is(err, sqlb.ErrNoRows) {
			c.log.Error("sqlstore: read failing run", "run", f.runID, "error", err)
		}
		return
	}
	if reg := c.reg(wfName); reg != nil {
		// OnTimeout replaces OnFailure for a run that failed on a deadline
		// (workflow.FailureHandler).
		h := reg.onFailure
		if reg.onTimeout != nil && workflow.IsTimeout(failureCause(msg.String, kind.String)) {
			h = reg.onTimeout
		}
		if h != nil {
			if _, err := safeCall(h, ctx, input); err != nil {
				c.log.Error("sqlstore: OnFailure handler failed", "workflow", wfName, "run", f.runID, "error", err)
			}
		}
	}
	if _, err := c.st.exec(ctx, c.st.db, `UPDATE {runs} SET status = ?, finished_at = ?, idem_active = NULL, fail_token = NULL
		WHERE id = ? AND status = ? AND fail_token = ?`,
		runFailed, c.nowNanos(), f.runID, runFailing, f.token); err != nil {
		// Left failing: another client reruns OnFailure once the claim
		// expires, which is the at-least-once the doc promises.
		c.log.Error("sqlstore: mark run failed", "run", f.runID, "error", err)
		return
	}
	c.emitFinished(ctx, []ended{{id: f.runID, workflow: wfName, status: runFailed, msg: msg.String, step: step.String, kind: kind.String}})
	c.nudge()
}

// stepRow is one step's state, as completeStep needs it.
type stepRow struct {
	status string
	out    []byte
}

// completeStep marks step succeeded with out and starts every child whose
// parents have now all succeeded; when every step has succeeded the run
// succeeds. Callers have touched the run.
func (c *Client) completeStep(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow, step string, out []byte, o *txOut) error {
	if _, err := c.st.exec(ctx, tx, "UPDATE {steps} SET status = ?, out_data = ?, lease_token = NULL WHERE run_id = ? AND name = ?",
		stepSucceeded, out, row.id, step); err != nil {
		return err
	}
	o.changed = true
	steps := map[string]stepRow{}
	var name, st string
	var data []byte
	err := c.st.each(ctx, tx, "SELECT name, status, out_data FROM {steps} WHERE run_id = ?", []any{row.id},
		func() error { steps[name] = stepRow{status: st, out: append([]byte(nil), data...)}; return nil }, &name, &st, &data)
	if err != nil {
		return fmt.Errorf("sqlstore: read steps: %w", err)
	}
	done := 0
	for _, s := range steps {
		if s.status == stepSucceeded {
			done++
		}
	}
	if done == len(reg.wf.Steps) {
		changed, err := c.finishRun(ctx, tx, row, runSucceeded, "", o)
		if err != nil || !changed {
			return err
		}
		outs := make(map[string][]byte, len(steps))
		for name, s := range steps {
			outs[name] = s.out
		}
		if err := c.storeOutput(ctx, tx, row.id, workflow.RunOutput(reg.wf, outs)); err != nil {
			return err
		}
		return c.handoffChain(ctx, tx, row, o)
	}
	for _, child := range reg.children[step] {
		if steps[child].status != stepPending {
			continue
		}
		cs := reg.steps[child]
		outs := make(map[string][]byte, len(cs.After))
		ready := true
		for _, p := range cs.After {
			if steps[p].status != stepSucceeded {
				ready = false
				break
			}
			outs[p] = steps[p].out
		}
		if !ready {
			continue
		}
		if err := c.startStep(ctx, tx, reg, row, cs, workflow.StepInput(row.payload, cs.After, outs), o); err != nil {
			return err
		}
	}
	return nil
}

// startStep begins step s of row with input in, by its kind: a Run step
// becomes claimable, a sleep step sleeps until its end, a wait step waits
// and takes at once a kept signal it may take.
func (c *Client) startStep(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow, s workflow.Step, in []byte, o *txOut) error {
	now := c.nowNanos()
	o.changed = true
	switch s.Kind() {
	case workflow.StepRun:
		_, err := c.st.exec(ctx, tx, "UPDATE {steps} SET status = ?, in_data = ?, due = ? WHERE run_id = ? AND name = ?",
			stepReady, in, now, row.id, s.Name)
		return err
	case workflow.StepSleep:
		_, err := c.st.exec(ctx, tx, "UPDATE {steps} SET status = ?, in_data = ?, due = ? WHERE run_id = ? AND name = ?",
			stepSleeping, in, now+int64(s.Sleep), row.id, s.Name)
		return err
	}
	job, err := row.job()
	if err != nil {
		c.log.Error("sqlstore: wait key reads a run without labels", "run", row.id, "error", err)
	}
	key, err := s.Wait.ResolveKey(job) // Start refused Expr keys, so this is a Func or the fixed Key
	if err != nil {
		return c.markFailing(ctx, tx, row, s.Name, in, err.Error(), o)
	}
	var due int64 // 0: wait forever
	switch {
	case !s.Wait.Deadline.IsZero():
		due = max(s.Wait.Deadline.UnixNano(), now) // already past: expire at once
	case s.Wait.Timeout > 0:
		due = now + int64(s.Wait.Timeout)
	}
	if _, err := c.st.exec(ctx, tx, `UPDATE {steps} SET status = ?, in_data = ?, due = ?, wait_event = ?, wait_key = ?
		WHERE run_id = ? AND name = ?`, stepWaiting, in, due, s.Wait.Event, key, row.id, s.Name); err != nil {
		return err
	}
	var sigID int64
	var payload []byte
	err = c.st.one(ctx, tx, `SELECT id, payload FROM {signals} WHERE name = ? AND ev_key = ? AND id > ?
		AND id NOT IN (SELECT signal_id FROM {consumed} WHERE run_id = ?) ORDER BY id LIMIT 1`,
		[]any{s.Wait.Event, key, row.sigFloor, row.id}, &sigID, &payload)
	if errors.Is(err, sqlb.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sqlstore: look back for a signal: %w", err)
	}
	return c.takeSignal(ctx, tx, reg, row, s.Name, sigID, payload, o)
}

// takeSignal records that run row took signal sigID and completes its wait
// step with the payload. The consumed row's primary key is the guard that
// one run never takes one signal twice.
func (c *Client) takeSignal(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow, step string, sigID int64, payload []byte, o *txOut) error {
	if _, err := c.st.exec(ctx, tx, "INSERT INTO {consumed} (run_id, signal_id) VALUES (?, ?)", row.id, sigID); err != nil {
		return err
	}
	return c.completeStep(ctx, tx, reg, row, step, payload, o)
}

// activate admits row: it writes a row per step and starts the root steps.
// Callers have moved the run to active in the same transaction. A run that
// already has step rows was re-queued by Retry: it resumes instead.
func (c *Client) activate(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow, o *txOut) error {
	var have int64
	if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {steps} WHERE run_id = ?", []any{row.id}, &have); err != nil {
		return fmt.Errorf("sqlstore: read the run's steps: %w", err)
	}
	if have > 0 {
		if err := c.armRunTimeout(ctx, tx, reg, row); err != nil {
			return err
		}
		return c.resume(ctx, tx, reg, row, o)
	}
	for _, s := range reg.wf.Steps {
		stepID, err := c.st.newID(IDStep)
		if err != nil {
			return err
		}
		if _, err := c.st.exec(ctx, tx, `INSERT INTO {steps} (id, run_id, run_seq, workflow, name, status, attempts, failures, reclaims,
			due, lease_until, priority, wait_event, wait_key) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, ?, '', '')`,
			stepID, row.id, row.seq, row.workflow, s.Name, stepPending, row.priority); err != nil {
			return err
		}
	}
	row.status = runActive
	if err := c.armRunTimeout(ctx, tx, reg, row); err != nil {
		return err
	}
	for _, s := range reg.wf.Steps {
		if len(s.After) == 0 {
			if err := c.startStep(ctx, tx, reg, row, s, row.payload, o); err != nil {
				return err
			}
			if row.status != runActive { // a wait failed to resolve its key
				return nil
			}
		}
	}
	return nil
}

// resume starts, for a run Retry re-queued, every pending step whose
// parents all succeeded (a root step has none), with the stored outputs of
// those parents as its input. Steps that succeeded before keep their
// outputs and do not run again.
func (c *Client) resume(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow, o *txOut) error {
	steps := map[string]stepRow{}
	var name, st string
	var data []byte
	err := c.st.each(ctx, tx, "SELECT name, status, out_data FROM {steps} WHERE run_id = ?", []any{row.id},
		func() error { steps[name] = stepRow{status: st, out: append([]byte(nil), data...)}; return nil }, &name, &st, &data)
	if err != nil {
		return fmt.Errorf("sqlstore: read steps: %w", err)
	}
	row.status = runActive
	for _, s := range reg.wf.Steps {
		if steps[s.Name].status != stepPending {
			continue
		}
		outs := make(map[string][]byte, len(s.After))
		ready := true
		for _, p := range s.After {
			if steps[p].status != stepSucceeded {
				ready = false
				break
			}
			outs[p] = steps[p].out
		}
		if !ready {
			continue
		}
		in := row.payload
		if len(s.After) > 0 {
			in = workflow.StepInput(row.payload, s.After, outs)
		}
		if err := c.startStep(ctx, tx, reg, row, s, in, o); err != nil {
			return err
		}
		if row.status != runActive { // a wait failed to resolve its key
			return nil
		}
	}
	return nil
}

// safeCall runs h and turns a panic into an error, so one bad handler
// cannot crash the process that hosts every other run.
func safeCall(h workflow.Handler, ctx context.Context, in []byte) (out []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			out, err = nil, fmt.Errorf("sqlstore: handler panicked: %v", p)
		}
	}()
	out, err = h(ctx, in)
	if err != nil && errors.Is(context.Cause(ctx), workflow.ErrCancelled) {
		err = fmt.Errorf("%w: %w", workflow.ErrCancelled, err)
	}
	return out, err
}

// unixTime turns stored Unix nanoseconds back into a time.
func unixTime(n int64) time.Time { return time.Unix(0, n).UTC() }
