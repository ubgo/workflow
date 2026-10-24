package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// idemSavepoint is the savepoint EnqueueTx sets around an insert that holds
// an idempotency key. A unique violation aborts the whole transaction on
// Postgres, so without it a duplicate key would poison the caller's own
// writes; rolling back to the savepoint undoes only the failed insert.
const idemSavepoint = "ubgo_workflow_enqueue"

// EnqueueTx writes a run inside the caller's transaction tx, so the run
// commits or rolls back with the app's own writes: a rolled-back tx leaves
// no run, a committed one is picked up by the workers of every client
// registering the workflow. It is the transactional outbox without a relay,
// for apps whose queue is this database.
//
// tx must be open on the *sql.DB this client was built with (the tables are
// read and written through it), and the workflow must be registered here
// and the client started, as for Enqueue.
//
// The run is written pending (or scheduled, for a future RunAt) and is not
// admitted inside tx: admission may take a workflow's lock row, which would
// then be held for as long as the app's transaction runs. Nothing can wake a
// worker before the commit, so a committed run is found by the dispatcher
// within WithPollInterval (default a second); call Wake after the commit to
// have this client admit it at once.
//
// Idempotency keys follow the workflow's policy, as for Enqueue: a key held
// by an unfinished run (committed, or written earlier in tx) returns that
// run's RunRef and writes nothing. A key written by another transaction that
// has not committed yet makes this insert wait for it. The insert runs under
// a savepoint, so a duplicate key never aborts the caller's transaction. On
// MySQL the holder is read with a locking read (FOR SHARE), which sees rows
// committed after tx began under REPEATABLE READ; on Postgres use READ
// COMMITTED (its default), because a REPEATABLE READ transaction cannot see a
// holder that committed after its snapshot and fails instead.
//
// Enqueue middleware does not run: it wraps workflow.Client.Enqueue, and
// what it does (a lock or counter in another store, a debounce timer, a
// trace export) would not roll back with tx. An app that needs middleware on
// a transactional enqueue writes through the outbox package instead, whose
// relay enqueues on any client, middleware included, after the commit.
//
// An error leaves tx open; the caller decides whether to roll it back.
func (c *Client) EnqueueTx(ctx context.Context, tx *sql.Tx, job workflow.Job) (workflow.RunRef, error) {
	if tx == nil {
		return workflow.RunRef{}, fmt.Errorf("%w: EnqueueTx needs a transaction", workflow.ErrInvalid)
	}
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
	if key == "" {
		row, err := c.insertRun(ctx, tx, reg, job, "", "")
		if err != nil {
			return workflow.RunRef{}, err
		}
		workflow.MarkInserted(ctx)
		return row.ref(), nil
	}
	for range maxIdemAttempts {
		ref, done, err := c.insertKeyed(ctx, tx, reg, job, key)
		if err != nil || done {
			return ref, err
		}
		// The holder finished between the insert and the read: try again.
	}
	return workflow.RunRef{}, fmt.Errorf("%w: key %q of workflow %q kept changing hands", workflow.ErrDuplicate, key, reg.wf.Name)
}

// insertKeyed is one attempt of a keyed EnqueueTx insert under the
// savepoint. done is false only when the key's holder was gone by the time
// it was read, so the caller tries the insert again.
func (c *Client) insertKeyed(ctx context.Context, tx *sql.Tx, reg *registered, job workflow.Job, key string) (ref workflow.RunRef, done bool, err error) {
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+idemSavepoint); err != nil {
		return workflow.RunRef{}, false, fmt.Errorf("sqlstore: savepoint: %w", err)
	}
	row, err := c.insertRun(ctx, tx, reg, job, key, "")
	if err == nil {
		if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+idemSavepoint); err != nil {
			return workflow.RunRef{}, false, fmt.Errorf("sqlstore: release savepoint: %w", err)
		}
		workflow.MarkInserted(ctx)
		return row.ref(), true, nil
	}
	if !c.st.isUnique(err) {
		return workflow.RunRef{}, false, err
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+idemSavepoint); err != nil {
		return workflow.RunRef{}, false, fmt.Errorf("sqlstore: roll back to savepoint: %w", err)
	}
	var holder runRow
	err = c.st.one(ctx, tx, "SELECT "+runCols+" FROM {runs} WHERE workflow = ? AND idem_active = ?"+c.st.lockingRead(),
		[]any{reg.wf.Name, key}, holder.targets()...)
	if errors.Is(err, sqlb.ErrNoRows) {
		return workflow.RunRef{}, false, nil
	}
	if err != nil {
		return workflow.RunRef{}, false, fmt.Errorf("sqlstore: read idempotency holder: %w", err)
	}
	workflow.MarkDuplicate(ctx)
	return holder.ref(), true, nil
}

// lockingRead is the suffix that makes a SELECT inside a caller's
// transaction read the latest committed row rather than the transaction's
// snapshot: FOR SHARE on Postgres and MySQL 8, nothing on SQLite, whose one
// writer means a write transaction already sees every committed row.
func (s *store) lockingRead() string {
	if s.engine == engineSQLite {
		return ""
	}
	return " FOR SHARE"
}

// Wake makes this client look for work at once instead of at its next
// poll: call it after committing a transaction that used EnqueueTx, so the
// run is admitted without waiting up to WithPollInterval. It never blocks
// and is a no-op on a client that is not running. Other clients of the
// cluster find the run at their next poll, or at once when the client has a
// Notifier (WithNotifier), which Wake signals too.
func (c *Client) Wake() { c.nudge() }
