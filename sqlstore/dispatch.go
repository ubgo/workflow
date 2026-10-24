package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// defaultHousekeepEvery is how many dispatcher passes run between two
// prunes of signals, consumed markers, old ticks and expired unique locks
// (WithHousekeepingEvery): with the one-second default poll, about once a
// minute on an idle client and more often on a busy one.
const defaultHousekeepEvery = 50

// defaultTickRetention is how long a fired tick's row is kept
// (WithTickRetention). A tick only guards against a second fire while some
// node may still see it due, which is well under a day.
const defaultTickRetention = 24 * time.Hour

// dispatcher moves time-driven work forward: cron ticks, delayed starts,
// admissions, sleeps, wait deadlines, signal deliveries, failure recovery
// and run-end notifications. It wakes when this client nudges it, when the
// earliest due time it knows of arrives on the scheduling clock, and every
// poll interval for work other clients made due.
func (c *Client) dispatcher() {
	defer c.wg.Done()
	var due clock.Timer
	defer func() {
		if due != nil {
			due.Stop()
		}
	}()
	for pass := 1; ; pass++ {
		wake := c.wakeCh()
		next := c.pass(c.ctx, pass)
		if c.ctx.Err() != nil {
			return
		}
		if due != nil {
			due.Stop()
			due = nil
		}
		var dueC <-chan time.Time
		if next > 0 {
			due = c.clk.NewTimer(time.Duration(next - c.nowNanos()))
			dueC = due.C()
		}
		poll := c.leaseClk.NewTimer(c.poll)
		select {
		case <-c.ctx.Done():
		case <-wake:
		case <-dueC:
			c.wakeLocal() // a backoff may have ended: wake the workers too
		case <-poll.C():
		}
		poll.Stop()
	}
}

// pass runs every dispatcher step once and returns the earliest future due
// time it knows of (Unix nanoseconds on the scheduling clock; 0: none).
// A failing step is logged and the others still run, so one bad row
// cannot stall the queue.
func (c *Client) pass(ctx context.Context, n int) int64 {
	steps := []struct {
		what       string
		fn         func(context.Context) error
		leaderOnly bool // with an elector, only the leader runs it
	}{
		{"fire cron ticks", c.fireSchedules, true},
		{"start delayed runs", c.promote, false},
		{"hand on chains", c.handoffChains, false},
		{"admit runs", c.admitAll, false},
		{"end sleeps", c.wakeSleeps, false},
		{"expire waits", c.expireWaits, false},
		{"deliver signals", c.deliver, false},
		{"recover failing runs", c.recoverFailing, false},
		{"fail runs past their RunTimeout", c.expireRuns, false},
		{"notify run ends", c.notifyFinished, false},
	}
	leader := c.IsLeader()
	for _, s := range steps {
		if ctx.Err() != nil {
			return 0
		}
		if s.leaderOnly && !c.firesSchedules(leader) {
			continue
		}
		if err := s.fn(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("sqlstore: dispatcher could not "+s.what, "error", err)
		}
	}
	if err := c.touchNode(ctx); err != nil && ctx.Err() == nil {
		c.log.Error("sqlstore: dispatcher could not report this node", "error", err)
	}
	if n%c.housekeepEvery == 0 && leader {
		if err := c.prune(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("sqlstore: dispatcher could not prune", "error", err)
		}
		if err := c.pruneNodes(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("sqlstore: dispatcher could not prune silent nodes", "error", err)
		}
		if err := c.trimHistories(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("sqlstore: dispatcher could not trim schedule histories", "error", err)
		}
	}
	next, err := c.nextDue(ctx)
	if err != nil && ctx.Err() == nil {
		c.log.Error("sqlstore: dispatcher could not read the next due time", "error", err)
	}
	idle, err := c.checkIdle(ctx)
	if err != nil && ctx.Err() == nil {
		c.log.Error("sqlstore: dispatcher could not check for idleness", "error", err)
	}
	if idle > 0 && (next == 0 || idle < next) {
		next = idle
	}
	return next
}

// nameArgs returns this client's workflow names as IN-list arguments,
// followed by extra.
func (c *Client) nameArgs(extra ...any) []any {
	return append(append([]any(nil), c.names...), extra...)
}

// inNames is the predicate limiting a query to this client's workflows.
func (c *Client) inNames() string { return "workflow IN (" + inList(len(c.names)) + ")" }

// promote turns delayed runs whose RunAt has come into pending runs.
func (c *Client) promote(ctx context.Context) error {
	_, err := c.st.exec(ctx, c.st.db, "UPDATE {runs} SET status = ? WHERE "+c.inNames()+" AND status = ? AND run_at <= ?",
		append(append([]any{runPending}, c.names...), runScheduled, c.nowNanos())...)
	return err
}

// admitAll admits what each workflow's policies allow.
func (c *Client) admitAll(ctx context.Context) error {
	c.mu.Lock()
	regs := make([]*registered, 0, len(c.wfs))
	for _, r := range c.wfs {
		regs = append(regs, r)
	}
	c.rateWake = time.Time{}
	c.mu.Unlock()
	for _, r := range regs {
		if err := c.admit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// stepRef names one step of one run.
type stepRef struct{ runID, name string }

// dueSteps reads up to WithFetchBatch steps of this client's workflows in
// status whose due time has come.
func (c *Client) dueSteps(ctx context.Context, status string) ([]stepRef, error) {
	var refs []stepRef
	var r stepRef
	err := c.st.each(ctx, c.st.db, "SELECT run_id, name FROM {steps} WHERE "+c.inNames()+" AND status = ? AND due > 0 AND due <= ? ORDER BY due LIMIT ?",
		c.nameArgs(status, c.nowNanos(), c.fetch), func() error { refs = append(refs, r); return nil }, &r.runID, &r.name)
	return refs, err
}

// onStep runs fn in a transaction on step ref of an active run, when the
// step is still in status and due; otherwise it does nothing (another
// client got there first).
func (c *Client) onStep(ctx context.Context, ref stepRef, status string, fn func(tx *sql.Tx, reg *registered, row *runRow, in []byte, o *txOut) error) error {
	var o txOut
	err := c.st.tx(ctx, func(tx *sql.Tx) error {
		o = txOut{}
		row, err := c.touch(ctx, tx, ref.runID)
		if err != nil || row.status != runActive {
			return err
		}
		reg := c.reg(row.workflow)
		if reg == nil {
			return nil
		}
		var st string
		var due int64
		var in []byte
		if err := c.st.one(ctx, tx, "SELECT status, due, in_data FROM {steps} WHERE run_id = ? AND name = ?",
			[]any{ref.runID, ref.name}, &st, &due, &in); err != nil {
			return err
		}
		if st != status || due == 0 || due > c.nowNanos() {
			return nil
		}
		return fn(tx, reg, row, in, &o)
	})
	if err != nil {
		return err
	}
	c.afterTx(ctx, o)
	return nil
}

// wakeSleeps completes sleep steps whose time is up; a sleep passes its
// input through as its output.
func (c *Client) wakeSleeps(ctx context.Context) error {
	refs, err := c.dueSteps(ctx, stepSleeping)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		err := c.onStep(ctx, ref, stepSleeping, func(tx *sql.Tx, reg *registered, row *runRow, in []byte, o *txOut) error {
			return c.completeStep(ctx, tx, reg, row, ref.name, in, o)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// expireWaits fails the runs whose wait step reached its deadline without
// a signal; waits are not retried.
func (c *Client) expireWaits(ctx context.Context) error {
	refs, err := c.dueSteps(ctx, stepWaiting)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		err := c.onStep(ctx, ref, stepWaiting, func(tx *sql.Tx, reg *registered, row *runRow, in []byte, o *txOut) error {
			s := reg.steps[ref.name]
			msg := fmt.Sprintf("sqlstore: wait step %q for event %q expired: %v: %v", ref.name, s.Wait.Event,
				workflow.TimeoutError(workflow.ErrWaitTimeout, ""), context.DeadlineExceeded)
			if err := c.markFailing(ctx, tx, row, ref.name, in, msg, o); err != nil {
				return err
			}
			return c.setTimeoutKind(ctx, tx, row.id, timeoutWait)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// candidate is a waiting step and a kept signal it may take.
type candidate struct {
	runID, step string
	sigID       int64
	payload     []byte
}

// deliver resumes waiting steps of this client's workflows with the kept
// signals they may take: same name and key, sent after their run was
// enqueued, not taken by their run before. Signal calls it at once; the
// dispatcher calls it every pass, which also closes the race of a wait
// that began while a signal was being stored.
func (c *Client) deliver(ctx context.Context) error {
	var cands []candidate
	var cd candidate
	err := c.st.each(ctx, c.st.db, `SELECT st.run_id, st.name, sg.id, sg.payload FROM {steps} st
		JOIN {runs} r ON r.id = st.run_id
		JOIN {signals} sg ON sg.name = st.wait_event AND sg.ev_key = st.wait_key AND sg.id > r.sig_floor
		WHERE st.status = ? AND st.`+c.inNames()+` AND r.status = ?
		AND NOT EXISTS (SELECT 1 FROM {consumed} cs WHERE cs.run_id = st.run_id AND cs.signal_id = sg.id)
		ORDER BY sg.id, r.seq LIMIT ?`,
		append(append([]any{stepWaiting}, c.nameArgs()...), runActive, c.fetch),
		func() error {
			cp := cd
			cp.payload = append([]byte(nil), cd.payload...)
			cands = append(cands, cp)
			return nil
		},
		&cd.runID, &cd.step, &cd.sigID, &cd.payload)
	if err != nil {
		return fmt.Errorf("sqlstore: find waiting steps for signals: %w", err)
	}
	for _, cand := range cands {
		var o txOut
		err := c.st.tx(ctx, func(tx *sql.Tx) error {
			o = txOut{}
			row, err := c.touch(ctx, tx, cand.runID)
			if err != nil || row.status != runActive {
				return err
			}
			reg := c.reg(row.workflow)
			if reg == nil {
				return nil
			}
			var st string
			var taken int64
			if err := c.st.one(ctx, tx, "SELECT status FROM {steps} WHERE run_id = ? AND name = ?", []any{cand.runID, cand.step}, &st); err != nil {
				return err
			}
			if err := c.st.one(ctx, tx, "SELECT COUNT(*) FROM {consumed} WHERE run_id = ? AND signal_id = ?",
				[]any{cand.runID, cand.sigID}, &taken); err != nil {
				return err
			}
			if st != stepWaiting || taken > 0 {
				return nil // resumed already, or another wait of this run took this signal
			}
			return c.takeSignal(ctx, tx, reg, row, cand.step, cand.sigID, cand.payload, &o)
		})
		if err != nil {
			return err
		}
		c.afterTx(ctx, o)
	}
	return nil
}

// recoverFailing takes over failing runs whose OnFailure claim expired
// (the client running it died) and finishes them.
func (c *Client) recoverFailing(ctx context.Context) error {
	var ids []string
	var id string
	err := c.st.each(ctx, c.st.db, "SELECT id FROM {runs} WHERE "+c.inNames()+" AND status = ? AND fail_until < ? LIMIT ?",
		c.nameArgs(runFailing, c.expiredBefore(), c.fetch), func() error { ids = append(ids, id); return nil }, &id)
	if err != nil {
		return err
	}
	for _, id := range ids {
		token := randomID("fail")
		now := c.leaseNanos()
		n, err := c.st.execN(ctx, c.st.db, "UPDATE {runs} SET fail_token = ?, fail_until = ? WHERE id = ? AND status = ? AND fail_until < ?",
			token, now+int64(c.lease), id, runFailing, now-int64(c.leaseSkew))
		if err != nil {
			return err
		}
		if n == 1 {
			c.finalizeFailure(ctx, failClaim{runID: id, token: token})
		}
	}
	return nil
}

// prune drops what can never matter again: signals no unfinished run may
// take (each run only takes signals above its floor), the consumed markers
// of finished runs, ticks long past and expired uniqueness locks.
func (c *Client) prune(ctx context.Context) error {
	var floor, top sql.NullInt64
	if err := c.st.one(ctx, c.st.db, "SELECT MIN(sig_floor) FROM {runs} WHERE status IN (?, ?, ?, ?)",
		[]any{runScheduled, runPending, runActive, runFailing}, &floor); err != nil {
		return err
	}
	if !floor.Valid {
		// No unfinished run: a later run's floor is at least the current
		// top, so every signal up to it is unreachable.
		if err := c.st.one(ctx, c.st.db, "SELECT MAX(id) FROM {signals}", nil, &top); err != nil {
			return err
		}
		floor = top
	}
	if floor.Valid {
		if _, err := c.st.exec(ctx, c.st.db, "DELETE FROM {signals} WHERE id <= ?", floor.Int64); err != nil {
			return err
		}
	}
	if _, err := c.st.exec(ctx, c.st.db, "DELETE FROM {consumed} WHERE run_id IN (SELECT id FROM {runs} WHERE "+terminalIn+")",
		terminalArgs()...); err != nil {
		return err
	}
	if _, err := c.st.exec(ctx, c.st.db, "DELETE FROM {ticks} WHERE fire_at < ?", c.nowNanos()-int64(c.tickRetention)); err != nil {
		return err
	}
	if _, err := c.UniqueStore().PruneExpired(ctx); err != nil {
		return err
	}
	_, err := c.DebounceStore().PruneExpired(ctx)
	return err
}

// nextDue is the earliest future due time among this client's delayed
// runs, ready steps in backoff, sleeps, wait deadlines, schedules and rate
// windows.
func (c *Client) nextDue(ctx context.Context) (int64, error) {
	now := c.nowNanos()
	var best int64
	consider := func(v sql.NullInt64) {
		if v.Valid && v.Int64 > now && (best == 0 || v.Int64 < best) {
			best = v.Int64
		}
	}
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{"SELECT MIN(due) FROM {steps} WHERE " + c.inNames() + " AND status IN (?, ?, ?, ?) AND due > ?",
			c.nameArgs(stepReady, stepSnoozed, stepSleeping, stepWaiting, now)},
		{"SELECT MIN(run_at) FROM {runs} WHERE " + c.inNames() + " AND status = ?", c.nameArgs(runScheduled)},
		{"SELECT MIN(i.deadline) FROM {runs} r JOIN {run_info} i ON i.run_id = r.id WHERE r." + c.inNames() + " AND r.status = ? AND i.deadline > 0",
			c.nameArgs(runActive)},
		{"SELECT MIN(next_fire) FROM {schedules} WHERE " + c.inNames() + " AND active = 1", c.nameArgs()},
	} {
		var v sql.NullInt64
		if err := c.st.one(ctx, c.st.db, q.sql, q.args, &v); err != nil {
			return best, err
		}
		consider(v)
	}
	c.mu.Lock()
	rw := c.rateWake
	c.mu.Unlock()
	if !rw.IsZero() {
		consider(sql.NullInt64{Int64: rw.UnixNano(), Valid: true})
	}
	return best, nil
}

// notifyFinished delivers run ends to the observers (workflow.RunNotifier).
// Each end is claimed with a lease before delivery and marked delivered
// after every observer returned, so a crash in between delivers it again:
// at least once, never lost.
func (c *Client) notifyFinished(ctx context.Context) error {
	c.mu.Lock()
	obs := c.observers
	c.mu.Unlock()
	if len(obs) == 0 {
		return nil
	}
	type ended struct {
		id, workflow, status, labels  string
		sid, errMsg, failStep, forced sql.NullString
		kind                          sql.NullString
		finished                      int64
		discarded                     sql.NullInt64
	}
	var list []ended
	var e ended
	err := c.st.each(ctx, c.st.db, "SELECT r.id, r.scheduled_id, r.workflow, r.status, r.err, r.labels, r.finished_at, x.discarded, r.fail_step, f.run_id, i.timeout"+
		" FROM {runs} r LEFT JOIN {run_ext} x ON x.run_id = r.id LEFT JOIN {run_forced} f ON f.run_id = r.id LEFT JOIN {run_info} i ON i.run_id = r.id WHERE r."+c.inNames()+
		" AND r."+terminalIn+" AND r.notified = 0 AND r.notify_until < ? ORDER BY r.seq LIMIT ?",
		c.nameArgs(append(terminalArgs(), c.expiredBefore(), c.fetch)...),
		func() error { list = append(list, e); return nil }, &e.id, &e.sid, &e.workflow, &e.status, &e.errMsg, &e.labels, &e.finished, &e.discarded, &e.failStep, &e.forced, &e.kind)
	if err != nil {
		return err
	}
	for _, e := range list {
		now := c.leaseNanos()
		n, err := c.st.execN(ctx, c.st.db, "UPDATE {runs} SET notify_until = ? WHERE id = ? AND notified = 0 AND notify_until < ?",
			now+int64(c.lease), e.id, now-int64(c.leaseSkew))
		if err != nil {
			return err
		}
		if n == 0 {
			continue // another client is delivering it
		}
		r := runRow{id: e.id, labels: e.labels}
		labels, lerr := r.labelMap()
		if lerr != nil {
			c.log.Error("sqlstore: run end delivered without labels", "run", e.id, "error", lerr)
		}
		ev := workflow.RunEvent{
			Ref:        workflow.RunRef{RunID: workflow.RunID(e.id), ScheduledID: workflow.ScheduledID(e.sid.String)},
			Workflow:   e.workflow,
			Status:     workflow.Status(e.status),
			Labels:     labels,
			FinishedAt: unixTime(e.finished),
		}
		ev.Err = runError(e.id, e.workflow, e.failStep.String, e.status, e.errMsg.String, e.kind.String, e.discarded.Int64 == 1, e.forced.Valid)
		if e.status == runFailed {
			ev.Step = e.failStep.String
		}
		for _, fn := range obs {
			c.observe(ctx, fn, ev)
		}
		if _, err := c.st.exec(ctx, c.st.db, "UPDATE {runs} SET notified = 1 WHERE id = ?", e.id); err != nil {
			return err
		}
	}
	return nil
}

// storedError is a failed run's error rebuilt from its stored message.
type storedError string

func (e storedError) Error() string { return string(e) }

// observe calls fn, recovering a panic so an observer can never change a
// run's status or stop the dispatcher.
func (c *Client) observe(ctx context.Context, fn workflow.RunFinishedFunc, ev workflow.RunEvent) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("sqlstore: run-finished observer panicked", "run", ev.Ref.RunID, "panic", fmt.Sprint(p))
		}
	}()
	fn(ctx, ev)
}
