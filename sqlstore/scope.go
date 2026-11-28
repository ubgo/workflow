package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/ubgo/workflow"
)

// Concurrency limits shared by several workflows (ConcurrencyLimit.Scope,
// CapConcurrencyScope), migration 015_run_scopes.
//
// A run of a workflow with a scoped limit has one <prefix>run_scopes row
// per scoped limit: the scope and the run's key value. It is written when
// the run is inserted by a client that has the workflow (the keys are
// known then), rewritten by each admission pass that reads the run while
// it waits (its payload may have changed), and replaced when it is
// admitted. Rows outlive the run like run_limits rows; every read joins
// the runs table on status, so an ended run counts nowhere.
//
// A scoped bucket's count is the number of rows of the scope and key value
// whose run holds a slot (active or failing), whatever its workflow; for a
// scope held while running, a parked run is left out, as for a limit of
// one workflow (hold.go). The admission pass of a workflow with a scoped
// limit runs under the workflow's lock row and then the lock row of each
// of its scopes, in name order, so two workflows that share a scope never
// decide on its buckets at once, on any node, and no two passes wait on
// each other's lock (each takes its own workflow's row first and the
// scope rows in one order).
//
// The no-overtake rule across workflows: a run of another workflow that
// waits on a scoped bucket (pending with a row there, or parked under a
// scope held while running with a step claimable now) is ahead of a run
// of this pass when its priority is higher, or equal and its seq lower;
// such a run holds the bucket against the run of the pass even when the
// bucket has room. The pass reads those waiting runs from their rows, so a
// worker needs no definition of the other workflow: the rows hold the key
// values its own client evaluated. A run inserted by an enqueue-only
// client has no row until a pass of a client with its workflow evaluates
// its keys; until then it neither holds back nor is held back by anything
// (it cannot start either: only such a client admits it).

// tblRunScopes holds, for each run of a workflow with a scoped limit, its
// key value under each scope.
//
//ds:def id=sql-tblrunscopes-rv2shref owner=@khanakia stability=stable
const tblRunScopes = "run_scopes"

// scopeDefs are migration 015's table.
func scopeDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblRunScopes, lines: []string{
		"run_id " + t.id + " NOT NULL",
		"scope " + t.name + " NOT NULL",
		"lkey " + t.name + " NOT NULL",
		// One key per scope of a run: a rewrite replaces it.
		"PRIMARY KEY (run_id, scope)",
	}, indexes: []index{{"bucket", "scope, lkey"}}}}
}

// scopeLockName is the lock row that serialises admission on one scope.
func scopeLockName(scope string) string { return "admit-scope:" + scope }

// scopesOf returns the scopes wf's limits name, sorted: the order every
// pass takes their lock rows in.
func scopesOf(wf workflow.Workflow) []string {
	var out []string
	for _, l := range wf.ConcurrencyLimits {
		if l.Scope != "" {
			out = append(out, l.Scope)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// scopeLock takes, in tx, the lock row of every scope of wf, in name
// order. admitLocked calls it right after the workflow's own lock row.
func (c *Client) scopeLock(ctx context.Context, tx *sql.Tx, wf workflow.Workflow) error {
	for _, s := range scopesOf(wf) {
		if _, err := c.st.exec(ctx, tx, "UPDATE {locks} SET n = n + 1 WHERE name = ?", scopeLockName(s)); err != nil {
			return err
		}
	}
	return nil
}

// scopeWrite replaces, in tx, run id's run_scopes rows with keys (one per
// limit of wf; only the scoped ones are written).
func (c *Client) scopeWrite(ctx context.Context, tx *sql.Tx, wf workflow.Workflow, id string, keys []string) error {
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_scopes} WHERE run_id = ?", id); err != nil {
		return err
	}
	for i, l := range wf.ConcurrencyLimits {
		if l.Scope == "" {
			continue
		}
		if _, err := c.st.exec(ctx, tx, "INSERT INTO {run_scopes} (run_id, scope, lkey) VALUES (?, ?, ?)", id, l.Scope, keys[i]); err != nil {
			return err
		}
	}
	return nil
}

// insertScopes writes the run_scopes rows of run id, just inserted in tx
// for job, when its workflow has a scoped limit.
func (c *Client) insertScopes(ctx context.Context, tx *sql.Tx, reg *registered, job workflow.Job, id string) error {
	if reg.stub || !workflow.ConcurrencyScoped(reg.wf) {
		return nil
	}
	keys := make([]string, len(reg.wf.ConcurrencyLimits))
	for i, l := range reg.wf.ConcurrencyLimits {
		if l.Key.Func != nil {
			keys[i] = l.Key.Func(job)
		}
	}
	return c.scopeWrite(ctx, tx, reg.wf, id, keys)
}

// scopeSync brings, in tx, the run_scopes rows of the waiting runs of wf
// this pass read in line with their keys (keys, by run ID), so the passes
// of the other workflows of each scope see where they wait. Only rows
// that differ are rewritten.
func (c *Client) scopeSync(ctx context.Context, tx *sql.Tx, wf workflow.Workflow, ids []string, keys map[string][]string) error {
	if len(ids) == 0 {
		return nil
	}
	stored := map[string]map[string]string{} // run -> scope -> key
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	var id, scope, key string
	err := c.st.each(ctx, tx, "SELECT run_id, scope, lkey FROM {run_scopes} WHERE run_id IN ("+inList(len(ids))+")", args, func() error {
		if stored[id] == nil {
			stored[id] = map[string]string{}
		}
		stored[id][scope] = key
		return nil
	}, &id, &scope, &key)
	if err != nil {
		return fmt.Errorf("sqlstore: read run scope keys: %w", err)
	}
	for _, id := range ids {
		same := true
		for i, l := range wf.ConcurrencyLimits {
			if l.Scope == "" {
				continue
			}
			if k, ok := stored[id][l.Scope]; !ok || k != keys[id][i] {
				same = false
				break
			}
		}
		if same {
			continue
		}
		if err := c.scopeWrite(ctx, tx, wf, id, keys[id]); err != nil {
			return err
		}
	}
	return nil
}

// scopePos is where a waiting run of another workflow stands in admission
// order: its priority and the lowest seq among such runs of that priority.
type scopePos struct {
	priority int64
	seq      int64
}

// scopeBegin reads, in tx, what lp needs for wf's scoped limits: the count
// of every scoped bucket across the workflows naming the scope, and where
// the runs of other workflows that wait on one of them stand (lp.ahead).
// Only the buckets of keys, the key values this pass's runs ask for (by
// limit), are read.
//
//ds:def id=sql-client-scopebegin-4cf39ag8 owner=@khanakia stability=stable
func (c *Client) scopeBegin(ctx context.Context, tx *sql.Tx, lp *limitPass, keys map[int]map[string]bool) error {
	wf := lp.wf
	idx := map[string]int{}
	var scopes []any
	var vals []any
	seen := map[string]bool{}
	for i, l := range wf.ConcurrencyLimits {
		if l.Scope == "" {
			continue
		}
		idx[l.Scope] = i
		scopes = append(scopes, l.Scope)
		for k := range keys[i] {
			if !seen[k] {
				seen[k] = true
				vals = append(vals, k)
			}
		}
	}
	lp.scoped = idx
	if len(scopes) == 0 || len(vals) == 0 {
		return nil
	}
	where := "s.scope IN (" + inList(len(scopes)) + ") AND s.lkey IN (" + inList(len(vals)) + ")"
	base := append(append([]any(nil), scopes...), vals...)

	var scope, key string
	var parked, n int64
	err := c.st.each(ctx, tx, "SELECT s.scope, s.lkey, COALESCE(h.parked, 0), COUNT(*) FROM {run_scopes} s JOIN {runs} r ON r.id = s.run_id"+
		" LEFT JOIN {run_hold} h ON h.run_id = s.run_id WHERE "+where+" AND r.status IN (?, ?) GROUP BY s.scope, s.lkey, COALESCE(h.parked, 0)",
		append(append([]any(nil), base...), runActive, runFailing), func() error {
			i := idx[scope]
			if parked == 1 && wf.ConcurrencyLimits[i].Hold.WhileRunning() {
				return nil
			}
			lp.count[limitBucket{lim: i, key: key}] += int(n)
			return nil
		}, &scope, &key, &parked, &n)
	if err != nil {
		return fmt.Errorf("sqlstore: read shared concurrency counts: %w", err)
	}

	// Waiting runs of the other workflows: pending, or parked with a step
	// claimable now (they want their slots back; only under a scope held
	// while running, since a parked run keeps its whole-run keys).
	var prio, seq int64
	collect := func(parkedRuns bool) func() error {
		return func() error {
			i := idx[scope]
			if parkedRuns && !wf.ConcurrencyLimits[i].Hold.WhileRunning() {
				return nil
			}
			b := limitBucket{lim: i, key: key}
			lp.ahead[b] = append(lp.ahead[b], scopePos{priority: prio, seq: seq})
			return nil
		}
	}
	err = c.st.each(ctx, tx, "SELECT s.scope, s.lkey, r.priority, MIN(r.seq) FROM {run_scopes} s JOIN {runs} r ON r.id = s.run_id"+
		" WHERE "+where+" AND r.workflow <> ? AND r.status = ? GROUP BY s.scope, s.lkey, r.priority",
		append(append([]any(nil), base...), wf.Name, runPending), collect(false), &scope, &key, &prio, &seq)
	if err != nil {
		return fmt.Errorf("sqlstore: read runs waiting on shared concurrency keys: %w", err)
	}
	err = c.st.each(ctx, tx, "SELECT s.scope, s.lkey, r.priority, MIN(r.seq) FROM {run_scopes} s JOIN {runs} r ON r.id = s.run_id"+
		" WHERE "+where+" AND r.workflow <> ? AND r.status = ?"+
		" AND r.id IN (SELECT run_id FROM {run_hold} WHERE parked = 1)"+
		" AND EXISTS (SELECT 1 FROM {steps} st WHERE st.run_id = r.id AND st.status IN (?, ?) AND st.due <= ?)"+
		" GROUP BY s.scope, s.lkey, r.priority",
		append(append([]any(nil), base...), wf.Name, runActive, stepReady, stepSnoozed, c.nowNanos()), collect(true), &scope, &key, &prio, &seq)
	if err != nil {
		return fmt.Errorf("sqlstore: read parked runs waiting on shared concurrency keys: %w", err)
	}
	return nil
}

// behind reports whether run r, with key values keys, has a waiting run
// of another workflow ahead of it on one of the scoped buckets need
// reports (nil: every one).
//
//ds:def id=sql-limitpass-behind-9upke76c owner=@khanakia stability=stable
func (lp *limitPass) behind(r *runRow, keys []string, need func(limit int) bool) bool {
	for _, i := range lp.scoped {
		if need != nil && !need(i) {
			continue
		}
		for _, p := range lp.ahead[limitBucket{lim: i, key: keys[i]}] {
			if p.priority > r.priority || (p.priority == r.priority && p.seq < r.seq) {
				return true
			}
		}
	}
	return false
}
