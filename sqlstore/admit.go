package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/ubgo/workflow"
)

// admit starts the pending runs of reg that its concurrency and rate
// limits allow, highest priority first, then first come first served. A
// workflow with neither policy admits each run with a conditional update,
// so two clients never admit one run twice. A workflow with a policy
// admits under its lock row, so the bucket counts and rate windows a
// decision reads cannot change until it commits, on any node.
func (c *Client) admit(ctx context.Context, reg *registered) error {
	if reg.wf.Concurrency == nil && len(reg.wf.RateLimits) == 0 {
		return c.admitFree(ctx, reg)
	}
	var out txOut
	var wake int64
	err := c.st.tx(ctx, func(tx *sql.Tx) error {
		out, wake = txOut{}, 0
		var err error
		wake, err = c.admitLocked(ctx, tx, reg, &out)
		return err
	})
	if err != nil {
		return fmt.Errorf("sqlstore: admit %s: %w", reg.wf.Name, err)
	}
	if wake > 0 {
		c.mu.Lock()
		if c.rateWake.IsZero() || unixTime(wake).Before(c.rateWake) {
			c.rateWake = unixTime(wake)
		}
		c.mu.Unlock()
	}
	c.afterTx(ctx, out)
	return nil
}

// admitInline admits row, just inserted pending in tx, in the same
// transaction when nothing can hold it back: a registered workflow with no
// concurrency or rate policy (admitFree would admit it unconditionally).
// One transaction per enqueue then does what an insert followed by an
// admission sweep did in many: the sweep read every pending run of the
// workflow and admitted each in its own transaction, so concurrent
// enqueuers fought over the same rows (Postgres, 100 enqueuers and 100
// workers: 290 jobs/s with the sweep, 700 to 1,400 inline). Other runs are
// left to admit.
//
// SQLite keeps the sweep: it has one writer, so there is no row contention
// to remove, and a longer enqueue transaction holds the write lock longer
// while the other writers back off in busy_timeout sleeps (the same load:
// about 800 jobs/s with the sweep, under 400 inline).
func (c *Client) admitInline(ctx context.Context, tx *sql.Tx, reg *registered, row *runRow, out *txOut) error {
	if c.st.engine == engineSQLite {
		return nil
	}
	if row.status != runPending || reg.stub || reg.wf.Concurrency != nil || len(reg.wf.RateLimits) > 0 {
		return nil
	}
	n, err := c.st.execN(ctx, tx, "UPDATE {runs} SET status = ?, ver = ver + 1 WHERE id = ? AND status = ?", runActive, row.id, runPending)
	if err != nil || n == 0 {
		return err
	}
	return c.activate(ctx, tx, reg, row, out)
}

// pendingRuns reads up to WithFetchBatch pending runs of a workflow in
// admission order.
func (c *Client) pendingRuns(ctx context.Context, x interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, name string) ([]*runRow, error) {
	var rows []*runRow
	var r runRow
	err := c.st.each(ctx, x, "SELECT "+runCols+" FROM {runs} WHERE workflow = ? AND status = ? ORDER BY priority DESC, seq LIMIT ?",
		[]any{name, runPending, c.fetch}, func() error { cp := r; rows = append(rows, &cp); return nil }, r.targets()...)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read pending runs: %w", err)
	}
	return rows, nil
}

func (c *Client) admitFree(ctx context.Context, reg *registered) error {
	rows, err := c.pendingRuns(ctx, c.st.db, reg.wf.Name)
	if err != nil {
		return err
	}
	for _, r := range rows {
		var out txOut
		err := c.st.tx(ctx, func(tx *sql.Tx) error {
			out = txOut{}
			n, err := c.st.execN(ctx, tx, "UPDATE {runs} SET status = ?, ver = ver + 1 WHERE id = ? AND status = ?", runActive, r.id, runPending)
			if err != nil || n == 0 {
				return err
			}
			if err := c.evaluateKeys(ctx, tx, reg, r); err != nil {
				return err
			}
			return c.activate(ctx, tx, reg, r, &out)
		})
		if err != nil {
			return fmt.Errorf("sqlstore: admit run %s: %w", r.id, err)
		}
		c.afterTx(ctx, out)
	}
	return nil
}

// admitLocked is admit for a workflow with a concurrency or rate policy,
// inside the transaction that holds its lock row. It returns the earliest
// moment a rate-limited run may start (0: none is waiting on a window).
func (c *Client) admitLocked(ctx context.Context, tx *sql.Tx, reg *registered, out *txOut) (int64, error) {
	wf := reg.wf
	if _, err := c.st.exec(ctx, tx, "UPDATE {locks} SET n = n + 1 WHERE name = ?", lockName(wf.Name)); err != nil {
		return 0, err
	}
	pending, err := c.pendingRuns(ctx, tx, wf.Name)
	if err != nil || len(pending) == 0 {
		return 0, err
	}
	for _, r := range pending {
		if err := c.evaluateKeys(ctx, tx, reg, r); err != nil {
			return 0, err
		}
	}
	now := c.nowNanos()

	// The runs holding a slot, oldest first per bucket.
	active := map[string][]*runRow{}
	if wf.Concurrency != nil {
		var r runRow
		err := c.st.each(ctx, tx, "SELECT "+runCols+" FROM {runs} WHERE workflow = ? AND status IN (?, ?) ORDER BY seq",
			[]any{wf.Name, runActive, runFailing}, func() error { cp := r; active[r.bucket] = append(active[r.bucket], &cp); return nil },
			r.targets()...)
		if err != nil {
			return 0, fmt.Errorf("sqlstore: read active runs: %w", err)
		}
	}

	// The starts inside each limit's window, per key, oldest first; older
	// ones can never matter again and are deleted.
	windows := make([]map[string][]int64, len(wf.RateLimits))
	for i, rl := range wf.RateLimits {
		cut := now - int64(rl.Per)
		if _, err := c.st.exec(ctx, tx, "DELETE FROM {starts} WHERE workflow = ? AND lim = ? AND started_at <= ?", wf.Name, i, cut); err != nil {
			return 0, err
		}
		windows[i] = map[string][]int64{}
		var key string
		var at int64
		err := c.st.each(ctx, tx, "SELECT rkey, started_at FROM {starts} WHERE workflow = ? AND lim = ? ORDER BY started_at",
			[]any{wf.Name, i}, func() error { windows[i][key] = append(windows[i][key], at); return nil }, &key, &at)
		if err != nil {
			return 0, fmt.Errorf("sqlstore: read rate window: %w", err)
		}
	}

	var wake int64
	// held are the buckets of Concurrency.FIFO whose earlier run waits in
	// this pass: no later run of the bucket may start before it.
	held := map[string]bool{}
	fifo := wf.Concurrency != nil && wf.Concurrency.FIFO
	for _, r := range pending {
		if fifo && held[r.bucket] {
			continue
		}
		if conc := wf.Concurrency; conc != nil && len(active[r.bucket]) >= conc.MaxRuns {
			switch conc.Strategy {
			case workflow.ConcurrencyDropNewest, workflow.ConcurrencyCancelNewest:
				if _, err := c.finishRun(ctx, tx, r, runCancelled, "sqlstore: concurrency bucket full ("+string(conc.Strategy)+")", out); err != nil {
					return 0, err
				}
				continue
			case workflow.ConcurrencyCancelInProgress:
				for len(active[r.bucket]) >= conc.MaxRuns {
					oldest := active[r.bucket][0]
					if _, err := c.finishRun(ctx, tx, oldest, runCancelled, "sqlstore: cancelled by a newer run (cancel_in_progress)", out); err != nil {
						return 0, err
					}
					active[r.bucket] = active[r.bucket][1:]
				}
			default: // queue_newest and round_robin wait for room
				held[r.bucket] = true
				continue
			}
		}
		var keys []string
		if err := json.Unmarshal([]byte(r.rateKeys), &keys); err != nil {
			return 0, fmt.Errorf("sqlstore: run %s rate keys: %w", r.id, err)
		}
		if at, ok := rateAllows(wf.RateLimits, windows, keys); !ok {
			if wake == 0 || at < wake {
				wake = at
			}
			held[r.bucket] = true
			continue
		}
		for i := range wf.RateLimits {
			if _, err := c.st.exec(ctx, tx, "INSERT INTO {starts} (workflow, lim, rkey, started_at) VALUES (?, ?, ?, ?)",
				wf.Name, i, keys[i], now); err != nil {
				return 0, err
			}
			windows[i][keys[i]] = append(windows[i][keys[i]], now)
		}
		if _, err := c.st.exec(ctx, tx, "UPDATE {runs} SET status = ?, ver = ver + 1 WHERE id = ?", runActive, r.id); err != nil {
			return 0, err
		}
		active[r.bucket] = append(active[r.bucket], r)
		if err := c.activate(ctx, tx, reg, r, out); err != nil {
			return 0, err
		}
	}
	return wake, nil
}

// rateAllows reports whether every limit has room for a start with keys
// now; when not, it returns when the last of them frees (every limit must
// allow, so the start waits for the latest).
func rateAllows(limits []workflow.RateLimit, windows []map[string][]int64, keys []string) (int64, bool) {
	var at int64
	ok := true
	for i, rl := range limits {
		starts := windows[i][keys[i]]
		if len(starts) >= rl.Limit {
			ok = false
			if free := starts[len(starts)-rl.Limit] + int64(rl.Per); free > at {
				at = free
			}
		}
	}
	return at, ok
}
