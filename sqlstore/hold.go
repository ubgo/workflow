package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ubgo/workflow"
)

// Hold policies (Concurrency.Hold, ConcurrencyLimit.Hold,
// CapHoldWhileRunning), migration 013_run_hold.
//
// Every run of a workflow with a HoldWhileRunning key gets one
// <prefix>run_hold row when it is admitted, parked 0. A run parks (parked
// 1) when it has no step running and no step claimable now: every
// unfinished step is in a retry backoff, snoozed, asleep, waiting for a
// signal or waiting for a parent. A parked run does not count in the
// buckets of its HoldWhileRunning keys (admitLocked's reads skip it), and
// no worker claims its steps. Once one of its steps is claimable again
// (the backoff or snooze ended, a sleep or a wait completed and made the
// next step ready) the admission pass, under the workflow's lock row,
// treats it as a waiting run at its original place (priority, then seq)
// and unparks it only when every HoldWhileRunning key has room.
//
// The row is also the lock that keeps a parked run's step from running.
// Parking writes the row first (UPDATE ... WHERE parked = 0) and only then
// checks that nothing of the run is running or claimable, undoing itself
// when something is. A claim of a step of such a workflow writes the same
// row (UPDATE ... WHERE parked = 0) in its claim transaction and gives the
// claim up when the row is parked. Both writes take the row's lock, so a
// claim and a park of one run are serialised on every engine: either the
// claim commits first and the park sees its running step, or the park
// commits first and the claim sees parked = 1. A worker that dies while
// its run is parked leaves nothing to reclaim (nothing ran); one that dies
// after the run unparked leaves a running step with an expired lease,
// which counts as claimable, so the run keeps its slot until the step is
// reclaimed and finishes.

// tblRunHold holds, for each admitted run of a workflow with a
// HoldWhileRunning key, whether it is parked.
//
//ds:def id=sql-tblrunhold-c5w8rk2p owner=@khanakia stability=stable
const tblRunHold = "run_hold"

// holdDefs are migration 013's table. n is bumped by every claim and
// park, so each write takes the row's lock even when nothing else
// changes.
func holdDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblRunHold, lines: []string{
		"run_id " + t.id + " NOT NULL PRIMARY KEY",
		"parked " + t.int + " NOT NULL",
		"n " + t.int + " NOT NULL",
	}, indexes: []index{{"parked", "parked"}}}}
}

// notParked is the claim filter's guard: no step of a parked run.
const notParked = "NOT EXISTS (SELECT 1 FROM {run_hold} hh WHERE hh.run_id = st.run_id AND hh.parked = 1)"

// errClaimParked rolls back a claim that found its run parked.
var errClaimParked = errors.New("sqlstore: the claimed step's run is parked")

// holdWhileRunning reports whether wf has a HoldWhileRunning key.
func holdWhileRunning(wf workflow.Workflow) bool { return workflow.HoldsWhileRunning(wf) }

// holdOf is the hold policy of key limit of wf (-1: Concurrency).
func holdOf(wf workflow.Workflow, limit int) workflow.ConcurrencyHold {
	if limit < 0 {
		if wf.Concurrency == nil {
			return ""
		}
		return wf.Concurrency.Hold
	}
	return wf.ConcurrencyLimits[limit].Hold
}

// holdStart gives run id, just admitted in tx, its run_hold row, replacing
// any a previous admission (before a Retry) left.
func (c *Client) holdStart(ctx context.Context, tx *sql.Tx, id string) error {
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_hold} WHERE run_id = ?", id); err != nil {
		return err
	}
	_, err := c.st.exec(ctx, tx, "INSERT INTO {run_hold} (run_id, parked, n) VALUES (?, 0, 0)", id)
	return err
}

// holdClaim checks, in the claim transaction tx, that the claimed step's
// run is not parked, taking the run_hold row's lock (see the comment at
// the top of this file). A run without a row (admitted before its
// workflow held while running) is never parked.
func (c *Client) holdClaim(ctx context.Context, tx *sql.Tx, a *attempt) error {
	reg := c.reg(a.workflow)
	if reg == nil || !holdWhileRunning(reg.wf) {
		return nil
	}
	n, err := c.st.execN(ctx, tx, "UPDATE {run_hold} SET n = n + 1 WHERE run_id = ? AND parked = 0", a.runID)
	if err != nil || n > 0 {
		return err
	}
	var rows int64
	if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {run_hold} WHERE run_id = ?", []any{a.runID}, &rows); err != nil {
		return err
	}
	if rows > 0 {
		return errClaimParked
	}
	return nil
}

// busySQL counts the steps of a run that keep its slot: running (a lease
// that expired counts: the step waits to be reclaimed), or ready or
// snoozed and due (waiting for a worker).
const busySQL = "SELECT COUNT(*) FROM {steps} WHERE run_id = ? AND (status = ? OR (status IN (?, ?) AND due <= ?))"

func (c *Client) busyArgs(id string) []any {
	return []any{id, stepRunning, stepReady, stepSnoozed, c.nowNanos()}
}

// errStillBusy rolls back a park that found the run busy once it held the
// row.
var errStillBusy = errors.New("sqlstore: the run is busy again")

// park parks run id when it is active and has no step running and none
// claimable now. It reports whether it did. The row's lock is taken
// first, then the run is checked, in a transaction of its own (so the
// check reads what committed before the lock).
//
//ds:def id=sql-client-park-x2n6dq8v owner=@khanakia stability=stable
func (c *Client) park(ctx context.Context, id string) (bool, error) {
	var busy int64
	if err := c.st.one(ctx, c.st.db, busySQL, c.busyArgs(id), &busy); err != nil || busy > 0 {
		return false, err
	}
	parked := false
	err := c.st.tx(ctx, func(tx *sql.Tx) error {
		parked = false
		n, err := c.st.execN(ctx, tx, "UPDATE {run_hold} SET parked = 1, n = n + 1 WHERE run_id = ? AND parked = 0", id)
		if err != nil || n == 0 {
			return err
		}
		var status string
		if err := c.st.one(ctx, tx, "SELECT status FROM {runs} WHERE id = ?", []any{id}, &status); err != nil {
			return err
		}
		if err := c.st.one(ctx, tx, busySQL, c.busyArgs(id), &busy); err != nil {
			return err
		}
		if status != runActive || busy > 0 {
			return errStillBusy
		}
		parked = true
		return nil
	})
	if errors.Is(err, errStillBusy) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlstore: park run %s: %w", id, err)
	}
	return parked, nil
}

// parkAfter parks run id of reg after a transaction that may have left it
// with nothing running and nothing claimable (an attempt's result was
// recorded), and wakes admission when it did.
func (c *Client) parkAfter(ctx context.Context, reg *registered, id string) {
	if reg == nil || !holdWhileRunning(reg.wf) {
		return
	}
	ok, err := c.park(ctx, id)
	if err != nil {
		if ctx.Err() == nil {
			c.log.Error("sqlstore: park a run (the dispatcher parks it on its next pass)", "run", id, "error", err)
		}
		return
	}
	if ok {
		c.nudge()
	}
}

// parkIdle is the dispatcher's sweep: it parks every active run of a
// workflow with a HoldWhileRunning key that holds its slots with nothing
// running and nothing claimable (a run whose first step is a sleep or a
// wait, a wait that began after a sleep). Runs whose attempt this client
// just recorded were parked at once (parkAfter); the sweep catches the
// rest within one pass.
//
//ds:def id=sql-client-parkidle-m7k3vt9e owner=@khanakia stability=stable
func (c *Client) parkIdle(ctx context.Context) error {
	names := c.holdNames()
	if len(names) == 0 {
		return nil
	}
	var ids []string
	var id string
	args := append(append([]any(nil), names...), runActive, stepRunning, stepReady, stepSnoozed, c.nowNanos(), c.fetch)
	err := c.st.each(ctx, c.st.db, "SELECT r.id FROM {runs} r WHERE r.workflow IN ("+inList(len(names))+") AND r.status = ?"+
		" AND r.id IN (SELECT run_id FROM {run_hold} WHERE parked = 0)"+
		" AND NOT EXISTS (SELECT 1 FROM {steps} st WHERE st.run_id = r.id AND (st.status = ? OR (st.status IN (?, ?) AND st.due <= ?)))"+
		" ORDER BY r.seq LIMIT ?", args, func() error { ids = append(ids, id); return nil }, &id)
	if err != nil {
		return fmt.Errorf("sqlstore: read idle runs: %w", err)
	}
	parked := false
	for _, id := range ids {
		ok, err := c.park(ctx, id)
		if err != nil {
			return err
		}
		parked = parked || ok
	}
	if parked {
		c.signalFleet() // another node's dispatcher may admit into the freed slots
	}
	return nil
}

// admitHeld runs admission for the workflows with a HoldWhileRunning key
// once more after the dispatcher ended sleeps and delivered signals, so a
// parked run whose next step those made ready takes its slots back in the
// same pass.
func (c *Client) admitHeld(ctx context.Context) error {
	c.mu.Lock()
	var regs []*registered
	for _, r := range c.wfs {
		if holdWhileRunning(r.wf) {
			regs = append(regs, r)
		}
	}
	c.mu.Unlock()
	for _, r := range regs {
		if err := c.admit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// holdNames are the registered workflows with a HoldWhileRunning key, as
// IN-list arguments.
func (c *Client) holdNames() []any {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []any
	for name, r := range c.wfs {
		if holdWhileRunning(r.wf) {
			out = append(out, name)
		}
	}
	return out
}

// rejoinRuns reads, in tx, up to WithFetchBatch parked runs of wf with a
// step claimable now, in admission order: the runs that want their slots
// back.
func (c *Client) rejoinRuns(ctx context.Context, tx *sql.Tx, wf workflow.Workflow) ([]*runRow, error) {
	var rows []*runRow
	var r runRow
	vw, vargs := c.versionWhere("r.id", "r.workflow")
	args := append(append([]any{wf.Name, runActive, stepReady, stepSnoozed, c.nowNanos()}, vargs...), c.fetch)
	err := c.st.each(ctx, tx, "SELECT "+runCols+" FROM {runs} r WHERE workflow = ? AND status = ?"+
		" AND id IN (SELECT run_id FROM {run_hold} WHERE parked = 1)"+
		" AND EXISTS (SELECT 1 FROM {steps} st WHERE st.run_id = r.id AND st.status IN (?, ?) AND st.due <= ?)"+
		" AND "+vw+" ORDER BY priority DESC, seq LIMIT ?",
		args, func() error { cp := r; rows = append(rows, &cp); return nil }, r.targets()...)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read parked runs: %w", err)
	}
	return rows, nil
}

// storedLimitKeys reads, in tx, the key values run id was admitted with
// under each of wf's ConcurrencyLimits (run_limits).
func (c *Client) storedLimitKeys(ctx context.Context, tx *sql.Tx, wf workflow.Workflow, id string) ([]string, error) {
	keys := make([]string, len(wf.ConcurrencyLimits))
	var lim int
	var key string
	err := c.st.each(ctx, tx, "SELECT lim, lkey FROM {run_limits} WHERE run_id = ?", []any{id}, func() error {
		if lim >= 0 && lim < len(keys) {
			keys[lim] = key
		}
		return nil
	}, &lim, &key)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read run %s concurrency limit keys: %w", id, err)
	}
	return keys, nil
}

// unpark gives parked run id its HoldWhileRunning slots back in tx, the
// admission transaction that found room for them. It reports whether the
// run was still parked.
func (c *Client) unpark(ctx context.Context, tx *sql.Tx, id string) (bool, error) {
	n, err := c.st.execN(ctx, tx, "UPDATE {run_hold} SET parked = 0, n = n + 1 WHERE run_id = ? AND parked = 1", id)
	return n > 0, err
}
