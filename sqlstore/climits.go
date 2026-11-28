package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ubgo/workflow"
)

// Several concurrency keys per workflow (Workflow.ConcurrencyLimits,
// CapConcurrencyLimits), migration 012_concurrency_limits.
//
// The admission pass of a workflow with limits already runs under the
// workflow's lock row (admitLocked), so the counts it reads cannot change
// before it commits, on any node. When it admits a run it writes one
// <prefix>run_limits row per limit (the run, the limit's index and the
// run's key value); a bucket's count is the number of those rows whose run
// still holds a slot (active or failing), so a run that ends gives back
// every key at once with no write of its own, and a retried run's rows are
// replaced when it is admitted again. The key values of a waiting run are
// evaluated when the pass reads it, from its current payload and labels,
// like an enqueue-only client's bucket; they are fixed from its admission
// on.

// tblRunLimits holds, for each admitted run of a workflow with
// ConcurrencyLimits, its key value under each limit.
//
//ds:def id=sql-tblrunlimits-bgqvrpez owner=@khanakia stability=stable
const tblRunLimits = "run_limits"

// limitDefs are migration 012's table.
func limitDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblRunLimits, lines: []string{
		"run_id " + t.id + " NOT NULL",
		"workflow " + t.name + " NOT NULL",
		"lim " + t.int + " NOT NULL",
		"lkey " + t.name + " NOT NULL",
		// One key per limit of a run: a re-admission replaces its rows.
		"PRIMARY KEY (run_id, lim)",
	}, indexes: []index{{"bucket", "workflow, lim, lkey"}}}}
}

// hasAdmissionPolicy reports whether wf's runs are admitted under its lock
// row: it has a concurrency key, more concurrency keys or a rate limit.
func hasAdmissionPolicy(wf workflow.Workflow) bool {
	return wf.Concurrency != nil || len(wf.ConcurrencyLimits) > 0 || len(wf.RateLimits) > 0
}

// limitKeys evaluates r's key value under each of wf's ConcurrencyLimits.
func limitKeys(wf workflow.Workflow, r *runRow) ([]string, error) {
	job, err := r.job()
	if err != nil {
		return nil, fmt.Errorf("sqlstore: run %s concurrency limit keys: %w", r.id, err)
	}
	keys := make([]string, len(wf.ConcurrencyLimits))
	for i, l := range wf.ConcurrencyLimits {
		if l.Key.Func != nil {
			keys[i] = l.Key.Func(job)
		}
	}
	return keys, nil
}

// limitBucket is one bucket of one limit.
type limitBucket struct {
	lim int
	key string
}

// limitPass is one admission pass's view of a workflow's ConcurrencyLimits:
// the runs holding each bucket, and the keys an earlier waiting run of the
// pass needs (no later run may take them, so a run that needs several keys
// is never overtaken on one of them; workflow.ConcurrencyLimit).
type limitPass struct {
	wf       workflow.Workflow
	count    map[limitBucket]int
	held     map[limitBucket]bool
	heldConc map[string]bool // Concurrency buckets held the same way
	// scoped maps each Scope of wf's limits to the limit's index; the
	// counts of those limits are the scope's, across workflows (scope.go).
	scoped map[string]int
	// ahead lists, per scoped bucket, where the waiting runs of other
	// workflows stand (behind).
	ahead map[limitBucket][]scopePos
}

// limitBegin reads, in tx, how many runs hold each bucket of wf's limits.
func (c *Client) limitBegin(ctx context.Context, tx *sql.Tx, wf workflow.Workflow) (*limitPass, error) {
	lp := &limitPass{wf: wf, count: map[limitBucket]int{}, held: map[limitBucket]bool{}, heldConc: map[string]bool{}, ahead: map[limitBucket][]scopePos{}}
	if len(wf.ConcurrencyLimits) == 0 {
		return lp, nil
	}
	var b limitBucket
	var n, parked int64
	// A parked run (hold.go) does not hold a HoldWhileRunning limit's
	// bucket: its rows are counted apart and left out for those limits.
	err := c.st.each(ctx, tx, "SELECT l.lim, l.lkey, COALESCE(h.parked, 0), COUNT(*) FROM {run_limits} l JOIN {runs} r ON r.id = l.run_id"+
		" LEFT JOIN {run_hold} h ON h.run_id = l.run_id WHERE l.workflow = ? AND r.status IN (?, ?) GROUP BY l.lim, l.lkey, COALESCE(h.parked, 0)",
		[]any{wf.Name, runActive, runFailing}, func() error {
			if b.lim < 0 || b.lim >= len(wf.ConcurrencyLimits) || wf.ConcurrencyLimits[b.lim].Scope != "" {
				return nil // a scoped limit counts across workflows (scopeBegin)
			}
			if parked == 1 && wf.ConcurrencyLimits[b.lim].Hold.WhileRunning() {
				return nil
			}
			lp.count[b] += int(n)
			return nil
		}, &b.lim, &b.key, &parked, &n)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read concurrency limit counts: %w", err)
	}
	return lp, nil
}

// blocked reports whether a run with Concurrency bucket bucket (full: that
// bucket is at MaxRuns) and limit keys keys must wait: one of its keys is
// held by an earlier waiting run, or one of its buckets is full.
//
//ds:def id=sql-limitpass-blocked-kmgb9ydt owner=@khanakia stability=stable
func (lp *limitPass) blocked(bucket string, full bool, keys []string) bool {
	if lp.wf.Concurrency != nil && (full || lp.heldConc[bucket]) {
		return true
	}
	for i, l := range lp.wf.ConcurrencyLimits {
		b := limitBucket{lim: i, key: keys[i]}
		if lp.held[b] || lp.count[b] >= l.MaxRuns {
			return true
		}
	}
	return false
}

// blockedNeed is blocked for a run that needs only the keys need reports
// (-1: the Concurrency bucket): a parked run rejoining (hold.go).
func (lp *limitPass) blockedNeed(bucket string, full bool, keys []string, need func(limit int) bool) bool {
	if lp.wf.Concurrency != nil && need(-1) && (full || lp.heldConc[bucket]) {
		return true
	}
	for i, l := range lp.wf.ConcurrencyLimits {
		b := limitBucket{lim: i, key: keys[i]}
		if need(i) && (lp.held[b] || lp.count[b] >= l.MaxRuns) {
			return true
		}
	}
	return false
}

// holdNeed is hold for the keys need reports.
func (lp *limitPass) holdNeed(bucket string, keys []string, need func(limit int) bool) {
	if lp.wf.Concurrency != nil && need(-1) {
		lp.heldConc[bucket] = true
	}
	for i, k := range keys {
		if need(i) {
			lp.held[limitBucket{lim: i, key: k}] = true
		}
	}
}

// hold reserves every key of a waiting run against the runs behind it.
func (lp *limitPass) hold(bucket string, keys []string) {
	if lp.wf.Concurrency != nil {
		lp.heldConc[bucket] = true
	}
	for i, k := range keys {
		lp.held[limitBucket{lim: i, key: k}] = true
	}
}

// limitTake records run r's admission in tx: one run_limits row per limit,
// replacing any a previous admission (before a Retry) left.
func (c *Client) limitTake(ctx context.Context, tx *sql.Tx, lp *limitPass, r *runRow, keys []string) error {
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_limits} WHERE run_id = ?", r.id); err != nil {
		return err
	}
	for i, k := range keys {
		if _, err := c.st.exec(ctx, tx, "INSERT INTO {run_limits} (run_id, workflow, lim, lkey) VALUES (?, ?, ?, ?)", r.id, lp.wf.Name, i, k); err != nil {
			return err
		}
		lp.count[limitBucket{lim: i, key: k}]++
	}
	if len(lp.scoped) > 0 {
		return c.scopeWrite(ctx, tx, lp.wf, r.id, keys)
	}
	return nil
}
