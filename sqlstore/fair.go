package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// Round robin and weighted fairness across concurrency keys (FC-05,
// FC-06; CapRoundRobin, CapKeyWeights), migration 011_fair_turns.
//
// Start-time fair queueing, the algorithm every driver shares
// (workflow.FairTurn): when the admission pass of a ConcurrencyRoundRobin
// workflow admits a run, the run gets a start tag max(key pass, virtual
// time) in <prefix>run_fair and its key's pass moves on by
// workflow.FairStride(weight) in <prefix>fair_keys. Both are written only
// inside the admission transaction, which already holds the workflow's
// lock row, so fairness adds no lock and no write to the claim.
//
// The claim keeps its order (priority, then arrival) to choose the next
// step; when that step belongs to a round-robin workflow, the turn goes to
// the claimable step of the same workflow and priority whose run has the
// smallest start tag (claimFair). Fairness so reorders which key takes the
// workflow's place in line, never the place itself, and a workflow
// without round robin pays nothing.

const (
	// tblFairKeys holds each round-robin key's pass: where its last
	// admitted run ends in virtual time.
	//ds:def id=sql-tblfairkeys-6ddtxj45 owner=@khanakia stability=stable
	tblFairKeys = "fair_keys"
	// tblRunFair holds each admitted round-robin run's start tag.
	//ds:def id=sql-tblrunfair-sgxyns7d owner=@khanakia stability=stable
	tblRunFair = "run_fair"
)

// fairDefs are migration 011's tables.
func fairDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblFairKeys, lines: []string{
		"workflow " + t.name + " NOT NULL",
		"fkey " + t.name + " NOT NULL",
		"pass " + t.int + " NOT NULL",
		// One pass per key of a workflow: the admission upserts it under
		// the workflow's lock row.
		"PRIMARY KEY (workflow, fkey)",
	}}, {stem: tblRunFair, lines: []string{
		"run_id " + t.id + " NOT NULL PRIMARY KEY",
		"workflow " + t.name + " NOT NULL",
		"fair " + t.int + " NOT NULL",
	}, indexes: []index{{"order", "workflow, fair"}}}}
}

// fairClaimLock is the lock row a SQLite claim of a client serving a
// round-robin workflow writes first (claimTurn).
const fairClaimLock = "claim:fair"

// roundRobin reports whether wf's keys take turns.
func roundRobin(wf workflow.Workflow) bool {
	return wf.Concurrency != nil && wf.Concurrency.Strategy == workflow.ConcurrencyRoundRobin
}

// fairPass is one admission pass's view of a round-robin workflow: its
// virtual time and the passes of the keys it read or wrote.
type fairPass struct {
	now  int64
	pass map[string]int64
	have map[string]bool // keys with a fair_keys row
}

// fairBegin reads the workflow's virtual time in tx: the smallest start
// tag of its runs with a step waiting for a worker or, when none waits,
// the largest pass of its keys (so keys that rejoin after the workflow
// went idle start level). Keys whose pass is behind it start at it anyway
// (FairTurn), so their rows are deleted, which keeps the table at about
// one row per key with work.
func (c *Client) fairBegin(ctx context.Context, tx *sql.Tx, name string) (*fairPass, error) {
	var now sql.NullInt64
	err := c.st.one(ctx, tx, "SELECT MIN(f.fair) FROM {run_fair} f JOIN {steps} st ON st.run_id = f.run_id WHERE f.workflow = ? AND st.workflow = ? AND st.status = ? AND st.due <= ?",
		[]any{name, name, stepReady, c.nowNanos()}, &now)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read the virtual time of %s: %w", name, err)
	}
	if !now.Valid {
		if err := c.st.one(ctx, tx, "SELECT MAX(pass) FROM {fair_keys} WHERE workflow = ?", []any{name}, &now); err != nil {
			return nil, fmt.Errorf("sqlstore: read the key passes of %s: %w", name, err)
		}
	}
	fp := &fairPass{now: now.Int64, pass: map[string]int64{}, have: map[string]bool{}}
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {fair_keys} WHERE workflow = ? AND pass < ?", name, fp.now); err != nil {
		return nil, err
	}
	return fp, nil
}

// fairAssign gives run r, being admitted in tx, its start tag and moves
// its key's pass on. A run admitted again (after Retry) gets a new tag.
//
//ds:def id=sql-client-fairassign-5q36yg5q owner=@khanakia stability=stable
func (c *Client) fairAssign(ctx context.Context, tx *sql.Tx, fp *fairPass, reg *registered, r *runRow) error {
	name := reg.wf.Name
	pass, ok := fp.pass[r.bucket]
	if !ok {
		err := c.st.one(ctx, tx, "SELECT pass FROM {fair_keys} WHERE workflow = ? AND fkey = ?", []any{name, r.bucket}, &pass)
		switch {
		case errors.Is(err, sqlb.ErrNoRows):
			pass = 0
		case err != nil:
			return fmt.Errorf("sqlstore: read the pass of key %q: %w", r.bucket, err)
		default:
			fp.have[r.bucket] = true
		}
	}
	start, next := workflow.FairTurn(pass, fp.now, workflow.FairWeight(reg.wf.Concurrency, r.bucket))
	fp.pass[r.bucket] = next
	if fp.have[r.bucket] {
		if _, err := c.st.exec(ctx, tx, "UPDATE {fair_keys} SET pass = ? WHERE workflow = ? AND fkey = ?", next, name, r.bucket); err != nil {
			return err
		}
	} else {
		if _, err := c.st.exec(ctx, tx, "INSERT INTO {fair_keys} (workflow, fkey, pass) VALUES (?, ?, ?)", name, r.bucket, next); err != nil {
			return err
		}
		fp.have[r.bucket] = true
	}
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_fair} WHERE run_id = ?", r.id); err != nil {
		return err
	}
	_, err := c.st.exec(ctx, tx, "INSERT INTO {run_fair} (run_id, workflow, fair) VALUES (?, ?, ?)", r.id, name, start)
	return err
}

// fullBuckets is the pending-run filter of a round-robin workflow: a run
// whose bucket is full cannot be admitted, so it must not take a place in
// the fetch window (WithFetchBatch) from a run of a key with room. Without
// it, a key with a backlog longer than the window would hold back every
// key behind it. A run whose keys are not evaluated yet (an enqueue-only
// client wrote it) stays in, so it gets evaluated. Arguments: the
// unevaluated marker, workflow, the two slot-holding statuses, MaxRuns.
//
//ds:def id=sql-fullbuckets-ygt6hxvr owner=@khanakia stability=stable
const fullBuckets = " AND (r.rate_keys = ? OR r.bucket NOT IN (SELECT a.bucket FROM {runs} a WHERE a.workflow = ? AND a.status IN (?, ?) GROUP BY a.bucket HAVING COUNT(*) >= ?))"

// fairArgs are fullBuckets' arguments for wf, or nil when wf is not round
// robin (no filter).
func fairArgs(wf workflow.Workflow) []any {
	if !roundRobin(wf) {
		return nil
	}
	return []any{unevaluatedKeys, wf.Name, runActive, runFailing, wf.Concurrency.MaxRuns}
}

// claimFair hands the turn of a claim inside a round-robin workflow: given
// the step the claim order chose (workflow name, priority), it returns the
// claimable step of the same workflow and priority whose run has the
// smallest start tag (then arrival) and its run_seq, locked on Postgres
// and MySQL, or "" when none is (the chosen one is then kept).
func (c *Client) claimFair(ctx context.Context, tx *sql.Tx, queues []any, name string, prio int64) (string, int64, error) {
	// Equal tags go by the run's own arrival (runs.seq): a step's run_seq
	// may be a place it took over from an earlier turn.
	q := "SELECT st.id, st.run_seq FROM {steps} st JOIN {runs} fr ON fr.id = st.run_id LEFT JOIN {run_fair} f ON f.run_id = st.run_id WHERE " + c.claimFilter(queues) +
		" AND st.workflow = ? AND st.priority = ? ORDER BY COALESCE(f.fair, 0), fr.seq, st.id LIMIT 1"
	if c.st.engine != engineSQLite {
		q += " FOR UPDATE OF st SKIP LOCKED"
	}
	var id string
	var seq int64
	err := c.st.one(ctx, tx, q, append(c.claimArgs(queues), name, prio), &id, &seq)
	if errors.Is(err, sqlb.ErrNoRows) {
		return "", 0, nil
	}
	return id, seq, err
}

// claimTurn is the claim of a client that serves a round-robin workflow:
// it chooses a step in claim order and, when the step's workflow is round
// robin, hands the turn with claimFair, then leases the step with set
// (claimFrom's UPDATE). It reports whether it claimed one. When the turn
// goes to another step, the two swap run_seq: the claimed step takes the
// chosen one's place in line and the chosen one keeps the claimed one's,
// so the workflow's places among other workflows stay where its runs'
// arrivals put them. On SQLite the lease is a conditional UPDATE that
// re-checks the step is still claimable; on Postgres and MySQL both reads
// lock their row.
func (c *Client) claimTurn(ctx context.Context, tx *sql.Tx, queues []any, set, token string, until int64) (bool, error) {
	if c.st.engine == engineSQLite {
		// Write first: a SQLite transaction that reads and then writes
		// must upgrade its read snapshot, which fails (busy) whenever
		// another claimer committed in between, so concurrent claimers
		// retry each other's transactions without end. Taking the write
		// lock first queues them instead; SQLite has one writer anyway.
		if _, err := c.st.exec(ctx, tx, "UPDATE {locks} SET n = n + 1 WHERE name = ?", fairClaimLock); err != nil {
			return false, err
		}
	}
	q := "SELECT st.id, st.workflow, st.priority, st.run_seq FROM {steps} st WHERE " + c.claimWhere(queues)
	if c.st.engine != engineSQLite {
		q += " FOR UPDATE SKIP LOCKED"
	}
	var id, name string
	var prio, seq int64
	err := c.st.one(ctx, tx, q, c.claimArgs(queues), &id, &name, &prio, &seq)
	if errors.Is(err, sqlb.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if reg := c.reg(name); reg != nil && roundRobin(reg.wf) {
		fid, fseq, err := c.claimFair(ctx, tx, queues, name, prio)
		if err != nil {
			return false, err
		}
		if fid != "" && fid != id {
			if _, err := c.st.exec(ctx, tx, "UPDATE {steps} SET run_seq = ? WHERE id = ?", fseq, id); err != nil {
				return false, err
			}
			if _, err := c.st.exec(ctx, tx, "UPDATE {steps} SET run_seq = ? WHERE id = ?", seq, fid); err != nil {
				return false, err
			}
			id = fid
		}
	}
	if c.st.engine == engineSQLite {
		args := append([]any{stepRunning, stepRunning, token, until}, c.claimArgs(queues)...)
		n, err := c.st.execN(ctx, tx, set+"? AND id IN (SELECT st.id FROM {steps} st WHERE "+c.claimFilter(queues)+")", append(append(args[:4:4], id), args[4:]...)...)
		return n > 0, err
	}
	_, err = c.st.exec(ctx, tx, set+"?", stepRunning, stepRunning, token, until, id)
	return err == nil, err
}
