package sqlstore

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ubgo/workflow"
)

// storedStatuses is which runs.status values may hold a run in status s.
// Queued, Running, Retrying and Waiting of an admitted run depend on its
// steps, so they are narrowed in Go (stepStatus) after the read.
func storedStatuses(s workflow.Status) []string {
	switch s {
	case workflow.StatusScheduled:
		return []string{runScheduled}
	case workflow.StatusQueued:
		return []string{runPending, runActive}
	case workflow.StatusRunning:
		return []string{runActive, runFailing}
	case workflow.StatusWaiting, workflow.StatusRetrying:
		return []string{runActive}
	case workflow.StatusDiscarded:
		return []string{runDiscarded}
	case workflow.StatusSucceeded:
		return []string{runSucceeded}
	case workflow.StatusFailed:
		return []string{runFailed}
	case workflow.StatusCancelled:
		return []string{runCancelled}
	}
	return nil
}

// runStatus maps a stored run status to workflow.Status. An active run's
// status comes from its step counts (stepStatus).
func runStatus(stored string, steps map[string]int64) workflow.Status {
	switch stored {
	case runScheduled:
		return workflow.StatusScheduled
	case runPending:
		return workflow.StatusQueued
	case runFailing:
		return workflow.StatusRunning
	case runSucceeded:
		return workflow.StatusSucceeded
	case runFailed:
		return workflow.StatusFailed
	case runCancelled:
		return workflow.StatusCancelled
	case runDiscarded:
		return workflow.StatusDiscarded
	}
	return stepStatus(steps)
}

// stepBackoff is the key stepCounts files a step under while it waits on
// its retry backoff: ready, failed at least once, and not yet due. It is
// not a stored step status; a backoff step is stored ready.
const stepBackoff = "backoff"

// stepCountCols are the step status columns stepCounts groups by: the
// stored status, and whether the step is in backoff (stepBackoff). The one
// argument is now on the scheduling clock.
const stepCountCols = "status, CASE WHEN status = '" + stepReady + "' AND failures > 0 AND due > ? THEN 1 ELSE 0 END"

// countKey is the stepStatus key of a step counted with stepCountCols.
func countKey(status string, backoff int64) string {
	if backoff == 1 {
		return stepBackoff
	}
	return status
}

// stepStatus is an admitted run's status from how many of its steps are in
// each step status (stepCountCols): Running while a step is claimed,
// Retrying while one waits on its retry backoff, Queued while one waits for
// a worker, Waiting while only waits and sleeps are in progress. That is
// workflow.StatusRetrying's precedence.
func stepStatus(counts map[string]int64) workflow.Status {
	switch {
	case counts[stepRunning] > 0:
		return workflow.StatusRunning
	case counts[stepBackoff] > 0:
		return workflow.StatusRetrying
	case counts[stepReady] > 0:
		return workflow.StatusQueued
	case counts[stepWaiting]+counts[stepSleeping]+counts[stepSnoozed] > 0:
		return workflow.StatusWaiting
	}
	return workflow.StatusQueued
}

// cursorPrefix versions the cursor format, so a later format can tell an
// old cursor apart.
const cursorPrefix = "v1:"

// encodeCursor is the RunPage.Next after a run enqueued at created with id:
// the next page starts strictly after it in (created_at, id) descending.
func encodeCursor(created int64, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.FormatInt(created, 10) + ":" + id))
}

func decodeCursor(s string) (int64, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	rest, ok := strings.CutPrefix(string(raw), cursorPrefix)
	if err != nil || !ok {
		return 0, "", fmt.Errorf("%w: run query cursor %q was not issued by this driver", workflow.ErrInvalid, s)
	}
	at, id, ok := strings.Cut(rest, ":")
	n, err := strconv.ParseInt(at, 10, 64)
	if !ok || err != nil || id == "" {
		return 0, "", fmt.Errorf("%w: run query cursor %q was not issued by this driver", workflow.ErrInvalid, s)
	}
	return n, id, nil
}

// infoRow is one run as Runs reads it.
type infoRow struct {
	run       runRow
	queue     string
	created   int64
	started   sql.NullInt64
	failStep  sql.NullString
	discarded sql.NullInt64
	runAt     int64
	extra     infoExtra // run_info, aliased i
}

// infoCols is the column list infoRow.targets scans, runs aliased r and
// run_ext x.
const infoCols = "r.seq, r.id, r.scheduled_id, r.workflow, r.payload, r.labels, r.priority, r.bucket, r.rate_keys, r.status, r.sig_floor, r.finished_at, r.err," +
	" COALESCE(x.queue, '" + workflow.DefaultQueue + "'), r.created_at, x.started_at, r.fail_step, x.discarded," +
	" r.run_at, i.tenant, i.output, i.has_output, i.progress, i.nodes"

func (r *infoRow) targets() []any {
	return append(r.run.targets(), &r.queue, &r.created, &r.started, &r.failStep, &r.discarded,
		&r.runAt, &r.extra.tenant, &r.extra.output, &r.extra.hasOutput, &r.extra.progress, &r.extra.nodes)
}

// Runs lists the runs of every workflow in this database (not only those
// registered here) that match q, newest enqueue first, ties by RunID
// descending (workflow.RunQuerier). Ref, workflow, queue, enqueue-time, the
// stored half of the status filter and the label index (run_labels, whose
// cut values only narrow) are SQL; the exact labels and the exact status of
// an admitted run (queued, running or waiting, from its steps) are checked
// in Go, reading on until the page is full. Payload is the stored payload.
// The cursor is opaque; one this driver did not issue wraps ErrInvalid.
func (c *Client) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	if err := workflow.ValidateRunQuery(q); err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.CheckRunQuery(caps, q); err != nil {
		return workflow.RunPage{}, err
	}
	limit := q.PageLimit()
	where, args := c.runFilter(q)
	if q.Cursor != "" {
		at, id, err := decodeCursor(q.Cursor)
		if err != nil {
			return workflow.RunPage{}, err
		}
		where += " AND (r.created_at < ? OR (r.created_at = ? AND r.id < ?))"
		args = append(args, at, at, id)
	}
	var page workflow.RunPage
	var last *infoRow
	for {
		rows, err := c.readInfo(ctx, where, args, limit+1)
		if err != nil {
			return workflow.RunPage{}, err
		}
		infos, err := c.runInfos(ctx, rows)
		if err != nil {
			return workflow.RunPage{}, err
		}
		for i, info := range infos {
			if !q.Match(info) {
				continue
			}
			if len(page.Runs) == limit {
				page.Next = encodeCursor(last.created, last.run.id)
				return page, nil
			}
			page.Runs = append(page.Runs, info)
			last = &rows[i]
		}
		if len(rows) <= limit {
			return page, nil // the table holds no more matches
		}
		// Every row read so far is behind us; read on after the last one.
		end := rows[len(rows)-1]
		where, args = c.runFilter(q)
		where += " AND (r.created_at < ? OR (r.created_at = ? AND r.id < ?))"
		args = append(args, end.created, end.created, end.run.id)
	}
}

// runFilter is the SQL half of q's filter, over runs r and run_ext x.
func (c *Client) runFilter(q workflow.RunQuery) (string, []any) {
	where := "1 = 1"
	var args []any
	if !q.Ref.RunID.IsZero() {
		where += " AND r.id = ?"
		args = append(args, string(q.Ref.RunID))
	}
	if !q.Ref.ScheduledID.IsZero() {
		where += " AND r.scheduled_id = ?"
		args = append(args, string(q.Ref.ScheduledID))
	}
	if q.Workflow != "" {
		where += " AND r.workflow = ?"
		args = append(args, q.Workflow)
	}
	if q.Queue != "" {
		where += " AND COALESCE(x.queue, '" + workflow.DefaultQueue + "') = ?"
		args = append(args, q.Queue)
	}
	if len(q.Statuses) > 0 {
		set := map[string]bool{}
		for _, s := range q.Statuses {
			for _, st := range storedStatuses(s) {
				set[st] = true
			}
		}
		where += " AND r.status IN (" + inList(len(set)) + ")"
		for _, st := range []string{runScheduled, runPending, runActive, runFailing, runSucceeded, runFailed, runCancelled, runDiscarded} {
			if set[st] {
				args = append(args, st)
			}
		}
	}
	lw, la := labelFilter(q.Labels, q.LabelPrefixes)
	where += lw
	args = append(args, la...)
	if !q.From.IsZero() {
		where += " AND r.created_at >= ?"
		args = append(args, q.From.UnixNano())
	}
	if !q.To.IsZero() {
		where += " AND r.created_at < ?"
		args = append(args, q.To.UnixNano())
	}
	if q.Tenant != "" {
		where += " AND i.tenant = ?"
		args = append(args, q.Tenant)
	}
	dw, da := dueFilter(q)
	return where + dw, append(args, da...)
}

// infoQuery is the statement readInfo runs for where: one page of runs in
// listing order, its last argument the row limit.
func infoQuery(where string) string {
	return "SELECT " + infoCols + " FROM {runs} r LEFT JOIN {run_ext} x ON x.run_id = r.id LEFT JOIN {run_info} i ON i.run_id = r.id WHERE " + where +
		" ORDER BY r.created_at DESC, r.id DESC LIMIT ?"
}

// readInfo reads up to n runs matching where, in listing order.
func (c *Client) readInfo(ctx context.Context, where string, args []any, n int) ([]infoRow, error) {
	var out []infoRow
	var r infoRow
	err := c.st.each(ctx, c.st.db, infoQuery(where), append(append([]any(nil), args...), n),
		func() error {
			cp := r
			cp.run.payload = append([]byte(nil), r.run.payload...)
			cp.extra.output = append([]byte(nil), r.extra.output...)
			out = append(out, cp)
			return nil
		}, r.targets()...)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: list runs: %w", err)
	}
	return out, nil
}

// runInfos turns rows into RunInfos, reading the step counts of all of
// them in one statement.
func (c *Client) runInfos(ctx context.Context, rows []infoRow) ([]workflow.RunInfo, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	ids := make([]any, len(rows))
	for i, r := range rows {
		ids[i] = r.run.id
	}
	counts := map[string]map[string]int64{}
	attempts := map[string]int64{}
	var runID, st string
	var backoff, n, att int64
	now := c.nowNanos()
	err := c.st.each(ctx, c.st.db, "SELECT run_id, "+stepCountCols+", COUNT(*), SUM(attempts) FROM {steps} WHERE run_id IN ("+inList(len(ids))+") GROUP BY run_id, 2, 3",
		append([]any{now}, ids...), func() error {
			if counts[runID] == nil {
				counts[runID] = map[string]int64{}
			}
			counts[runID][countKey(st, backoff)] += n
			attempts[runID] += att
			return nil
		}, &runID, &st, &backoff, &n, &att)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read step counts: %w", err)
	}
	out := make([]workflow.RunInfo, len(rows))
	for i, r := range rows {
		labels, lerr := r.run.labelMap()
		if lerr != nil {
			c.log.Error("sqlstore: run listed without labels", "run", r.run.id, "error", lerr)
		}
		info := workflow.RunInfo{
			Ref:        r.run.ref(),
			Workflow:   r.run.workflow,
			Queue:      r.queue,
			Status:     runStatus(r.run.status, counts[r.run.id]),
			Labels:     labels,
			Payload:    r.run.payload,
			Priority:   int(r.run.priority),
			EnqueuedAt: unixTime(r.created),
			Attempts:   int(attempts[r.run.id]),
		}
		if r.started.Int64 > 0 {
			info.StartedAt = unixTime(r.started.Int64)
		}
		if r.run.sid.Valid {
			info.RunAt = unixTime(r.runAt) // a delayed start: Job.RunAt or the Reschedule time
		}
		r.extra.fill(c, &info)
		if isTerminalRun(r.run.status) {
			info.FinishedAt = unixTime(r.run.finishedAt)
		}
		if r.run.status == runFailed || r.run.status == runCancelled || r.run.status == runDiscarded || r.run.status == runFailing {
			info.Error = r.run.errMsg.String // the step's message; RunEvent.Err adds the run (*workflow.RunError)
		}
		if r.run.status == runFailed || r.run.status == runFailing {
			info.FailedStep = r.failStep.String
		}
		out[i] = info
	}
	if err := c.readAttemptErrors(ctx, out, ids); err != nil {
		return nil, err
	}
	return out, c.nextAttempts(ctx, out, now)
}

// nextAttempts sets RunInfo.NextAttempt, NextAttemptAt and Error on the
// Retrying runs of infos: the step in backoff that is due first, its next
// attempt number, when its backoff ends and its last attempt's error
// (step_err). now is the instant the step counts
// were read at, so a step counted in backoff is found here too.
func (c *Client) nextAttempts(ctx context.Context, infos []workflow.RunInfo, now int64) error {
	at := map[string]int{}
	var ids []any
	var snoozed []any
	for i, info := range infos {
		switch info.Status {
		case workflow.StatusRetrying:
			at[string(info.Ref.RunID)] = i
			ids = append(ids, string(info.Ref.RunID))
		case workflow.StatusWaiting:
			at[string(info.Ref.RunID)] = i
			snoozed = append(snoozed, string(info.Ref.RunID))
		}
	}
	if len(snoozed) > 0 {
		// A snoozed step runs again as the same attempt (its attempt was
		// refunded, so attempts+1 is it).
		var runID string
		var due, attempts int64
		err := c.st.each(ctx, c.st.db, "SELECT run_id, due, attempts FROM {steps} WHERE run_id IN ("+inList(len(snoozed))+") AND status = ?",
			append(snoozed, stepSnoozed), func() error {
				info := &infos[at[runID]]
				if info.NextAttemptAt.IsZero() || due < info.NextAttemptAt.UnixNano() {
					info.NextAttempt, info.NextAttemptAt = int(attempts)+1, unixTime(due)
				}
				return nil
			}, &runID, &due, &attempts)
		if err != nil {
			return fmt.Errorf("sqlstore: read snoozed steps: %w", err)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	var runID string
	var due, attempts int64
	var msg sql.NullString
	err := c.st.each(ctx, c.st.db, "SELECT st.run_id, st.due, st.attempts, se.err FROM {steps} st LEFT JOIN {step_err} se ON se.step_id = st.id"+
		" WHERE st.run_id IN ("+inList(len(ids))+") AND st.status = ? AND st.failures > 0 AND st.due > ?", append(ids, stepReady, now),
		func() error {
			info := &infos[at[runID]]
			if info.NextAttemptAt.IsZero() || due < info.NextAttemptAt.UnixNano() {
				info.NextAttempt, info.NextAttemptAt, info.Error = int(attempts)+1, unixTime(due), msg.String
			}
			return nil
		}, &runID, &due, &attempts, &msg)
	if err != nil {
		return fmt.Errorf("sqlstore: read retry backoffs: %w", err)
	}
	return nil
}

// Retry re-runs a failed run from the step that failed
// (workflow.RunRetrier). The steps that succeeded keep their outputs; every
// other step goes back to pending with fresh attempt counts, and the run
// goes back to queued, keeping its RunID. It is admitted again under the
// workflow's concurrency and rate limits, by this client when it registers
// the workflow and is started, else by the next client that does. Its
// idempotency key is taken again (ErrDuplicate when another unfinished run
// holds it), and its end is delivered to run-finished observers again. A
// run that is not failed returns ErrWrongState.
func (c *Client) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	var out workflow.RunRef
	err = c.st.tx(ctx, func(tx *sql.Tx) error {
		row, err := c.touch(ctx, tx, r.id)
		if err != nil {
			return err
		}
		if row.status != runFailed {
			return fmt.Errorf("%w: run %s is %s, only a failed run can be retried", workflow.ErrWrongState, row.id, row.status)
		}
		var key sql.NullString
		if err := c.st.one(ctx, tx, "SELECT idem_key FROM {run_ext} WHERE run_id = ?", []any{row.id}, &key); err != nil && !isNoRows(err) {
			return err
		}
		var idem any
		if key.Valid && key.String != "" {
			idem = key.String
		}
		if _, err := c.st.exec(ctx, tx, `UPDATE {runs} SET status = ?, idem_active = ?, finished_at = 0, err = NULL, fail_step = NULL,
			fail_input = NULL, fail_token = NULL, fail_until = 0, notified = 0, notify_until = 0 WHERE id = ?`,
			runPending, idem, row.id); err != nil {
			return err
		}
		for _, side := range []string{"step_ext", "step_err"} {
			if _, err := c.st.exec(ctx, tx, "DELETE FROM {"+side+"} WHERE step_id IN (SELECT id FROM {steps} WHERE run_id = ? AND status <> ?)",
				row.id, stepSucceeded); err != nil {
				return err
			}
		}
		if _, err := c.st.exec(ctx, tx, `UPDATE {steps} SET status = ?, in_data = NULL, attempts = 0, failures = 0, reclaims = 0, due = 0,
			lease_token = NULL, lease_until = 0, wait_event = '', wait_key = '' WHERE run_id = ? AND status <> ?`,
			stepPending, row.id, stepSucceeded); err != nil {
			return err
		}
		if _, err := c.st.exec(ctx, tx, "UPDATE {run_ext} SET discarded = 0 WHERE run_id = ?", row.id); err != nil {
			return err
		}
		if _, err := c.st.exec(ctx, tx, "DELETE FROM {run_forced} WHERE run_id = ?", row.id); err != nil {
			return err
		}
		if err := c.clearRunResult(ctx, tx, row.id); err != nil {
			return err
		}
		row.status = runPending
		out = row.ref()
		return nil
	})
	if c.st.isUnique(err) {
		return workflow.RunRef{}, fmt.Errorf("%w: another unfinished run of %s holds this run's idempotency key", workflow.ErrDuplicate, r.workflow)
	}
	if err != nil {
		return workflow.RunRef{}, err
	}
	if reg := c.reg(r.workflow); reg != nil && c.usable() == nil {
		if err := c.admit(ctx, reg); err != nil {
			return out, err
		}
		c.nudge()
	}
	return out, nil
}

// Delete forgets a finished run with its steps (workflow.RunDeleter). A
// run that is not finished returns ErrWrongState.
func (c *Client) Delete(ctx context.Context, ref workflow.RunRef) error {
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return err
	}
	return c.st.tx(ctx, func(tx *sql.Tx) error {
		row, err := c.touch(ctx, tx, r.id)
		if err != nil {
			return err
		}
		if !isTerminalRun(row.status) {
			return fmt.Errorf("%w: run %s is %s, only a finished run can be deleted", workflow.ErrWrongState, row.id, row.status)
		}
		if _, err := c.st.exec(ctx, tx, "DELETE FROM {runs} WHERE id = ?", row.id); err != nil {
			return err
		}
		return c.deleteOrphans(ctx, tx, []any{row.id})
	})
}

// CancelAll cancels every run matching q whose status is among
// workflow.CancelStatuses(q) (workflow.BulkCanceller), each exactly as
// Cancel would, and returns how many it cancelled. It lists the matches
// first and cancels them one by one, so a run that moved on in between
// (finished, or started when only waiting runs were asked for) is left
// alone and not counted.
func (c *Client) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return 0, err
	}
	if err := workflow.CheckRunQuery(caps, q); err != nil {
		return 0, err
	}
	q.Statuses = workflow.CancelStatuses(q)
	q.Limit = workflow.MaxRunsLimit
	var refs []workflow.RunRef
	for {
		page, err := c.Runs(ctx, q)
		if err != nil {
			return 0, err
		}
		for _, info := range page.Runs {
			refs = append(refs, info.Ref)
		}
		if page.Next == "" {
			break
		}
		q.Cursor = page.Next
	}
	want := map[workflow.Status]bool{}
	for _, s := range q.Statuses {
		want[s] = true
	}
	n := 0
	for _, ref := range refs {
		ok, err := c.cancelIf(ctx, ref, want)
		if err != nil {
			return n, err
		}
		if ok {
			n++
		}
	}
	return n, nil
}

// cancelIf cancels the run ref when its status is still in want, and
// reports whether it did.
func (c *Client) cancelIf(ctx context.Context, ref workflow.RunRef, want map[workflow.Status]bool) (bool, error) {
	st, err := c.Status(ctx, ref)
	if errors.Is(err, workflow.ErrNotFound) {
		return false, nil
	}
	if err != nil || !want[st] {
		return false, err
	}
	r, err := c.lookup(ctx, ref)
	if err != nil {
		return false, err
	}
	var out txOut
	var changed bool
	err = c.st.tx(ctx, func(tx *sql.Tx) error {
		out = txOut{}
		row, err := c.touch(ctx, tx, r.id)
		if err != nil {
			return err
		}
		changed, err = c.finishRun(ctx, tx, row, runCancelled, workflow.ErrCancelled.Error(), &out)
		return err
	})
	if err != nil {
		return false, err
	}
	c.afterTx(ctx, out)
	return changed, nil
}
