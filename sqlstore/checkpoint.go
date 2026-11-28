package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// gapCaps are the capabilities of migration 014's tables: step checkpoints,
// coalescing workflows and schedule idempotency keys. Each is rows the
// claiming, firing or enqueueing node reads and writes in its own
// transaction, so all three engines honour them the same way.
var gapCaps = workflow.CapsOf(workflow.CapCheckpoint, workflow.CapCoalesce, workflow.CapScheduleKey)

// The tables of migration 014.
const (
	// tblCheckpoints holds each step's saved checkpoint (SaveCheckpoint).
	tblCheckpoints = "checkpoints"
	// tblRunNext holds the run pending behind each held key of a
	// Workflow.Coalesce workflow.
	tblRunNext = "run_next"
	// tblScheduleKey holds Schedule.IdempotencyKey and Schedule.OnHeld.
	tblScheduleKey = "schedule_key"
)

// gapDefs is migration 014: side tables, never a new column on runs or
// schedules, for the reason scheduleTenantDefs gives (an ALTER TABLE ADD
// COLUMN cannot be written to run twice on MySQL).
func gapDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblCheckpoints, lines: []string{
		"run_id " + t.id + " NOT NULL",
		"step " + t.name + " NOT NULL",
		"data " + t.blob,
		// saved_at and expires_at are Unix nanoseconds on the scheduling
		// clock; expires_at 0: kept until the run ends.
		"saved_at " + t.int + " NOT NULL",
		"expires_at " + t.int + " NOT NULL",
		// One checkpoint per step of a run: a save replaces it.
		"PRIMARY KEY (run_id, step)",
	}}, {stem: tblRunNext, lines: []string{
		"workflow " + t.name + " NOT NULL",
		"ikey " + t.name + " NOT NULL",
		// run_id is the pending run: scheduled, run_at parkedRunAt, no
		// idem_active until its holder ends (promoteNext).
		"run_id " + t.id + " NOT NULL",
		// run_at is the pending run's own Job.RunAt (0: none), the
		// earliest it may start once promoted.
		"run_at " + t.int + " NOT NULL",
		// At most one run waits behind a key: the guard that makes a
		// concurrent enqueue fold into the one already stored.
		"PRIMARY KEY (workflow, ikey)",
	}, indexes: []index{{"run", "run_id"}}}, {stem: tblScheduleKey, lines: []string{
		"schedule_id " + t.id + " NOT NULL PRIMARY KEY",
		// template is Schedule.IdempotencyKey; never empty here.
		"template " + t.text + " NOT NULL",
		// on_held is Schedule.OnHeld ("" for the default).
		"on_held " + t.short + " NOT NULL",
	}}}
}

// WithMaxCheckpoint sets the largest checkpoint a step may save
// (workflow.SaveCheckpoint), in bytes; a bigger one is refused with a
// *workflow.CheckpointTooLargeError before anything is written. Zero or
// less means workflow.DefaultMaxCheckpoint, the default.
func WithMaxCheckpoint(n int) Option {
	return func(cl *Client) { cl.maxCheckpoint = max(n, 0) }
}

// checkpointer is the workflow.Checkpointer of one attempt: its writes are
// fenced by the attempt's lease token.
type checkpointer struct {
	c *Client
	a *attempt
}

var _ workflow.Checkpointer = checkpointer{}

// lost is the error of a write the attempt's lease no longer covers; it
// matches ErrLeaseLost and workflow.ErrAttemptLost.
func (p checkpointer) lost() error {
	return fmt.Errorf("sqlstore: checkpoint of step %q of run %s: the step is no longer this attempt's: %w", p.a.name, p.a.runID, ErrLeaseLost)
}

// fence fails in tx unless the step is still running under the attempt's
// lease token. The read takes a shared row lock on Postgres and MySQL, so a
// takeover waits until tx ends; on SQLite the write before it already
// holds the database's writer lock.
func (p checkpointer) fence(ctx context.Context, tx *sql.Tx) error {
	var id string
	err := p.c.st.one(ctx, tx, "SELECT id FROM {steps} WHERE id = ? AND lease_token = ? AND status = ?"+p.c.st.lockingRead(),
		[]any{p.a.stepID, p.a.token, stepRunning}, &id)
	if isNoRows(err) {
		return p.lost()
	}
	if err != nil {
		return fmt.Errorf("sqlstore: read step %q of run %s: %w", p.a.name, p.a.runID, err)
	}
	return nil
}

// SaveCheckpoint implements workflow.Checkpointer: the row is replaced and
// the lease checked in one transaction, so a stale attempt's save never
// lands.
func (p checkpointer) SaveCheckpoint(ctx context.Context, data []byte, ttl time.Duration) error {
	if err := workflow.CheckCheckpointSize(p.a.name, data, p.c.maxCheckpoint, "sqlstore.WithMaxCheckpoint"); err != nil {
		return err
	}
	if errors.Is(context.Cause(ctx), ErrLeaseLost) {
		return p.lost()
	}
	now := p.c.nowNanos()
	var exp int64
	if ttl > 0 {
		exp = now + int64(ttl)
	}
	if data == nil {
		data = []byte{}
	}
	// The lease, not the handler's ctx, decides whether the write lands: a
	// handler whose ctx was cancelled but whose attempt still owns the step
	// saves, and one whose lease is gone is refused by the fence.
	ctx = context.WithoutCancel(ctx)
	return p.c.st.tx(ctx, func(tx *sql.Tx) error {
		if _, err := p.c.st.exec(ctx, tx, "DELETE FROM {checkpoints} WHERE run_id = ? AND step = ?", p.a.runID, p.a.name); err != nil {
			return err
		}
		if _, err := p.c.st.exec(ctx, tx, "INSERT INTO {checkpoints} (run_id, step, data, saved_at, expires_at) VALUES (?, ?, ?, ?, ?)",
			p.a.runID, p.a.name, data, now, exp); err != nil {
			return err
		}
		return p.fence(ctx, tx)
	})
}

// LoadCheckpoint implements workflow.Checkpointer.
func (p checkpointer) LoadCheckpoint(ctx context.Context) ([]byte, bool, error) {
	var data []byte
	var exp int64
	err := p.c.st.one(ctx, p.c.st.db, "SELECT data, expires_at FROM {checkpoints} WHERE run_id = ? AND step = ?",
		[]any{p.a.runID, p.a.name}, &data, &exp)
	if isNoRows(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlstore: read checkpoint of step %q of run %s: %w", p.a.name, p.a.runID, err)
	}
	if exp != 0 && p.c.nowNanos() >= exp {
		return nil, false, nil
	}
	return bytes.Clone(data), true, nil
}

// ClearCheckpoint implements workflow.Checkpointer, fenced like a save.
func (p checkpointer) ClearCheckpoint(ctx context.Context) error {
	if errors.Is(context.Cause(ctx), ErrLeaseLost) {
		return p.lost()
	}
	ctx = context.WithoutCancel(ctx) // fenced by the lease, as SaveCheckpoint
	return p.c.st.tx(ctx, func(tx *sql.Tx) error {
		if _, err := p.c.st.exec(ctx, tx, "DELETE FROM {checkpoints} WHERE run_id = ? AND step = ?", p.a.runID, p.a.name); err != nil {
			return err
		}
		return p.fence(ctx, tx)
	})
}

// dropCheckpoints deletes every checkpoint of run in tx; callers do it in
// the transaction that ends the run.
func (c *Client) dropCheckpoints(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := c.st.exec(ctx, tx, "DELETE FROM {checkpoints} WHERE run_id = ?", runID)
	return err
}

var _ workflow.TxCompleter = (*Client)(nil)

// CompleteInTx implements workflow.TxCompleter: it begins a transaction on
// the client's *sql.DB, calls fn with a ctx carrying it (read it with
// workflow.CompletionTx[*sql.Tx]), completes the step in it with output
// exactly as CompleteTx does, and commits. When fn or the completion fails
// the transaction is rolled back and the error returned: nothing of fn's
// writes or the completion remains, and the handler's own return value is
// recorded as usual. A lost lease is an error matching ErrLeaseLost and
// workflow.ErrAttemptLost. Use CompleteTx instead when the app opens the
// transaction itself.
func (c *Client) CompleteInTx(ctx context.Context, output []byte, fn func(ctx context.Context) error) error {
	if fn == nil {
		return fmt.Errorf("%w: CompleteInTx needs a function", workflow.ErrInvalid)
	}
	v, ok := ctx.Value(attemptKey{}).(attemptCtx)
	if !ok || v.c != c {
		return fmt.Errorf("%w: CompleteInTx must be called with the context of a step this client is running", workflow.ErrInvalid)
	}
	if v.a.completion.get() != nil {
		// A completion of this attempt was staged before: refuse a second
		// one when it committed (the step is succeeded under this
		// attempt's token), whatever the run has done since.
		var n int64
		if err := c.st.one(ctx, c.st.db, "SELECT COUNT(*) FROM {steps} WHERE id = ? AND lease_token = ? AND status = ?",
			[]any{v.a.stepID, v.a.token, stepSucceeded}, &n); err != nil {
			return fmt.Errorf("sqlstore: read step %q of run %s: %w", v.a.name, v.a.runID, err)
		}
		if n == 1 {
			return fmt.Errorf("%w: step %q of run %s was already completed by this attempt", workflow.ErrInvalid, v.a.name, v.a.runID)
		}
	}
	tx, err := c.st.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlstore: begin CompleteInTx: %w", err)
	}
	if err := fn(workflow.WithCompletionTx(ctx, tx)); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := c.CompleteTx(ctx, tx, output); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlstore: commit CompleteInTx: %w", err)
	}
	return nil
}
