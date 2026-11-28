package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ubgo/workflow"
)

// parkedRunAt is the run_at of a run pending behind a held key of a
// Workflow.Coalesce workflow: scheduled, never due, so the promotion of
// due runs (promote) and every admission leave it alone until its holder
// ends and promoteNext gives it a real start time.
const parkedRunAt = int64(math.MaxInt64)

// errHolderGone is placeRun's report that the key's holder ended while the
// transaction ran; the caller tries again and the key is free.
var errHolderGone = errors.New("sqlstore: the idempotency key's holder ended")

// placement is what placeRun did with a keyed job.
type placement struct {
	row *runRow // the run the caller's RunRef names
	dup bool    // row is an existing run: nothing was inserted
	// fresh is true when row is a new run holding the key (not a run
	// pending behind it): the caller admits it as any new run.
	fresh bool
}

// holderOf reads, in tx, the unfinished run of reg's workflow holding key,
// with a shared row lock on Postgres and MySQL so the holder cannot end
// before tx does; ok is false when the key is free.
func (c *Client) holderOf(ctx context.Context, tx *sql.Tx, wf, key string) (*runRow, bool, error) {
	var h runRow
	err := c.st.one(ctx, tx, "SELECT "+runCols+" FROM {runs} WHERE workflow = ? AND idem_active = ?"+c.st.lockingRead(),
		[]any{wf, key}, h.targets()...)
	if isNoRows(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlstore: read idempotency holder: %w", err)
	}
	return &h, true, nil
}

// placeRun stores job under the idempotency key key (after the workflow's
// policy, not empty) in tx, deciding against the key's holder: a free key
// inserts a run holding it; a held key answers with the holder, except on
// a Workflow.Coalesce workflow whose holder has left the queue, where the
// run pending behind the holder answers, or a new pending run is stored
// (scheduled at parkedRunAt, recorded in run_next). id is the new run's
// RunID (empty: generated). A race that inserts a holder meanwhile fails
// with a unique violation the caller retries.
func (c *Client) placeRun(ctx context.Context, tx *sql.Tx, reg *registered, job workflow.Job, key, id string) (placement, error) {
	if c.st.engine == engineSQLite {
		// Write first. The holder is read before anything is written, and
		// a SQLite transaction that reads and then writes must upgrade its
		// read lock, which fails at once with SQLITE_BUSY whenever another
		// connection committed in between (busy_timeout does not apply to
		// the upgrade), so concurrent enqueuers of one key could use up
		// every transaction attempt. A write that changes nothing takes the
		// write lock up front, waiting out the timeout instead.
		if _, err := c.st.exec(ctx, tx, "UPDATE {runs} SET status = status WHERE workflow = ? AND idem_active = ?", reg.wf.Name, key); err != nil {
			return placement{}, err
		}
	}
	holder, held, err := c.holderOf(ctx, tx, reg.wf.Name, key)
	if err != nil {
		return placement{}, err
	}
	if !held {
		row, err := c.insertRun(ctx, tx, reg, job, key, id)
		return placement{row: row, fresh: true}, err
	}
	if reg.wf.Coalesce == nil {
		return placement{row: holder, dup: true}, nil
	}
	if holder.status == runScheduled || holder.status == runPending {
		return placement{row: holder, dup: true}, c.fold(ctx, tx, reg, holder, job)
	}
	var nextID string
	err = c.st.one(ctx, tx, "SELECT run_id FROM {run_next} WHERE workflow = ? AND ikey = ?", []any{reg.wf.Name, key}, &nextID)
	switch {
	case err == nil:
		next, err := c.loadRun(ctx, tx, "id", nextID)
		if err != nil {
			return placement{}, err
		}
		return placement{row: next, dup: true}, c.fold(ctx, tx, reg, next, job)
	case !isNoRows(err):
		return placement{}, fmt.Errorf("sqlstore: read the run pending behind key %q: %w", key, err)
	}
	var own int64
	if !job.RunAt.IsZero() {
		own = job.RunAt.UnixNano()
	}
	parked := job
	parked.RunAt = time.Unix(0, parkedRunAt)
	row, err := c.insertRun(ctx, tx, reg, parked, "", id)
	if err != nil {
		return placement{}, err
	}
	// insertRun records the key on run_ext only when it holds it; the
	// pending run takes it over on promotion, so record it now.
	if _, err := c.st.exec(ctx, tx, "UPDATE {run_ext} SET idem_key = ? WHERE run_id = ?", key, row.id); err != nil {
		return placement{}, err
	}
	if _, err := c.st.exec(ctx, tx, "INSERT INTO {run_next} (workflow, ikey, run_id, run_at) VALUES (?, ?, ?, ?)",
		reg.wf.Name, key, row.id, own); err != nil {
		return placement{}, err
	}
	// The holder was read before these writes; on SQLite the read and the
	// writes are one snapshot, on Postgres and MySQL the shared lock kept
	// it. Read it again so a holder that ended in between is never left
	// with a pending run nothing will promote.
	if _, still, err := c.holderOf(ctx, tx, reg.wf.Name, key); err != nil {
		return placement{}, err
	} else if !still {
		return placement{}, errHolderGone
	}
	return placement{row: row}, nil
}

// fold applies an enqueue folding into run row of reg's workflow: with
// CoalesceKeepLast the run takes job's payload and priority while it is
// still in the queue, and its stored concurrency key and rate-limit keys
// are evaluated again from them and its own labels (its ConcurrencyLimits
// keys are evaluated from the payload at admission), so admission weighs
// the keys of the payload it will run with.
func (c *Client) fold(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow, job workflow.Job) error {
	if !reg.wf.Coalesce.KeepsLast() {
		return nil
	}
	keyed, err := row.job()
	if err != nil {
		return fmt.Errorf("sqlstore: decode run %s: %w", row.id, err)
	}
	keyed.Payload, keyed.Priority = job.Payload, job.Priority
	bucket, rk, err := runKeys(reg, keyed)
	if err != nil {
		return err
	}
	_, err = c.st.exec(ctx, tx, "UPDATE {runs} SET payload = ?, priority = ?, bucket = ?, rate_keys = ? WHERE id = ? AND status IN (?, ?)",
		job.Payload, job.Priority, bucket, rk, row.id, runScheduled, runPending)
	return err
}

// enqueueCoalesced is Enqueue for a keyed job of a Workflow.Coalesce
// workflow: placeRun in a transaction, tried again when a race moved the
// key meanwhile.
func (c *Client) enqueueCoalesced(ctx context.Context, reg *registered, job workflow.Job, key string) (workflow.RunRef, error) {
	for range maxIdemAttempts {
		var p placement
		var out txOut
		var held string // the new run whose lifecycle events wait for its Inserted
		err := c.st.tx(ctx, func(tx *sql.Tx) error {
			var err error
			out = txOut{}
			if p, err = c.placeRun(ctx, tx, reg, job, key, ""); err != nil || p.dup {
				return err
			}
			held = p.row.id
			c.holdEvents(held)
			if !p.fresh {
				return nil // pending behind the holder: nothing to admit yet
			}
			return c.admitInline(ctx, tx, reg, p.row, &out)
		})
		if err != nil {
			if held != "" {
				c.releaseEvents(ctx, held, nil)
			}
			if errors.Is(err, errHolderGone) || c.st.isUnique(err) {
				continue
			}
			return workflow.RunRef{}, err
		}
		if p.dup {
			workflow.MarkDuplicate(ctx)
			return p.row.ref(), nil
		}
		c.releaseEvents(ctx, held, c.insertedEvent(p.row, job, runQueue(reg, job)))
		if p.fresh && p.row.status == runPending {
			if err := c.admitEnqueued(ctx, reg); err != nil {
				return workflow.RunRef{}, err
			}
		}
		c.afterTx(ctx, out)
		c.nudge()
		workflow.MarkInserted(ctx)
		return p.row.ref(), nil
	}
	return workflow.RunRef{}, fmt.Errorf("%w: key %q of workflow %q kept changing hands", workflow.ErrDuplicate, key, reg.wf.Name)
}

// runEnded is what migration 014 adds to a run's end, in the transaction
// that ends it: its checkpoints are deleted, its place as a pending run is
// dropped, and the run pending behind the key it held is promoted.
func (c *Client) runEnded(ctx context.Context, tx *sql.Tx, runID string, out *txOut) error {
	if err := c.dropCheckpoints(ctx, tx, runID); err != nil {
		return err
	}
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_next} WHERE run_id = ?", runID); err != nil {
		return err
	}
	var wf string
	var key sql.NullString
	err := c.st.one(ctx, tx, "SELECT r.workflow, x.idem_key FROM {runs} r JOIN {run_ext} x ON x.run_id = r.id WHERE r.id = ?",
		[]any{runID}, &wf, &key)
	if isNoRows(err) || (err == nil && !key.Valid) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sqlstore: read the key of run %s: %w", runID, err)
	}
	return c.promoteNext(ctx, tx, wf, key.String, out)
}

// promoteNext starts the run pending behind key, once no run holds it: it
// takes the key and becomes due at the later of now and its own RunAt, so
// the next promotion pass (promote) and admission start it as any delayed
// run. A key still held leaves it pending.
func (c *Client) promoteNext(ctx context.Context, tx *sql.Tx, wf, key string, out *txOut) error {
	var nextID string
	var own int64
	err := c.st.one(ctx, tx, "SELECT run_id, run_at FROM {run_next} WHERE workflow = ? AND ikey = ?", []any{wf, key}, &nextID, &own)
	if isNoRows(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sqlstore: read the run pending behind key %q: %w", key, err)
	}
	var held int64
	if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {runs} WHERE workflow = ? AND idem_active = ?", []any{wf, key}, &held); err != nil {
		return fmt.Errorf("sqlstore: read idempotency holder: %w", err)
	}
	if held > 0 {
		return nil
	}
	at := max(c.nowNanos(), own)
	if _, err := c.st.exec(ctx, tx, "UPDATE {runs} SET idem_active = ?, run_at = ? WHERE id = ? AND status = ?",
		key, at, nextID, runScheduled); err != nil {
		return err
	}
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_next} WHERE run_id = ?", nextID); err != nil {
		return err
	}
	out.changed = true
	return nil
}

// placeKeyedTx is insertKeyed for a Workflow.Coalesce workflow: placeRun
// under the savepoint insertKeyed set, rolled back to it when a race moved
// the key so the caller's transaction stays usable and tries again.
func (c *Client) placeKeyedTx(ctx context.Context, tx *sql.Tx, reg *registered, job workflow.Job, key string) (workflow.RunRef, bool, error) {
	p, err := c.placeRun(ctx, tx, reg, job, key, "")
	if errors.Is(err, errHolderGone) || c.st.isUnique(err) {
		if _, rerr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+idemSavepoint); rerr != nil {
			return workflow.RunRef{}, false, fmt.Errorf("sqlstore: roll back to savepoint: %w", rerr)
		}
		return workflow.RunRef{}, false, nil
	}
	if err != nil {
		return workflow.RunRef{}, false, err
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+idemSavepoint); err != nil {
		return workflow.RunRef{}, false, fmt.Errorf("sqlstore: release savepoint: %w", err)
	}
	if p.dup {
		workflow.MarkDuplicate(ctx)
	} else {
		workflow.MarkInserted(ctx)
	}
	return p.row.ref(), true, nil
}

// writeScheduleKey stores s's key template and held policy as schedule
// id's schedule_key row in tx: no row without a template. The delete comes
// first so the transaction opens with a write.
func (c *Client) writeScheduleKey(ctx context.Context, tx *sql.Tx, id string, s workflow.Schedule) error {
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {schedule_key} WHERE schedule_id = ?", id); err != nil {
		return err
	}
	if s.IdempotencyKey == "" {
		return nil
	}
	_, err := c.st.exec(ctx, tx, "INSERT INTO {schedule_key} (schedule_id, template, on_held) VALUES (?, ?, ?)", id, s.IdempotencyKey, string(s.OnHeld))
	return err
}

// placeFired stores the run schedule d fires at fire (a tick, or the
// trigger time) in tx: a plain insert without a key template, placeRun
// under the key the template expands to otherwise, after the workflow's
// Idempotency policy as for any key.
func (c *Client) placeFired(ctx context.Context, tx *sql.Tx, reg *registered, d dueSchedule, job workflow.Job, fire time.Time, loc *time.Location, id string) (placement, error) {
	if d.key != "" {
		key, err := workflow.ExpandScheduleKey(d.key, fire, loc)
		if err != nil {
			return placement{}, err
		}
		job.IdempotencyKey = key
		if key, err = idemKey(reg, job); err != nil {
			return placement{}, err
		} else if key != "" {
			return c.placeRun(ctx, tx, reg, job, key, id)
		}
	}
	row, err := c.insertRun(ctx, tx, reg, job, "", id)
	return placement{row: row, fresh: true}, err
}
