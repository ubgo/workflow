package sqlrecorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/recorder"
)

// Runs implements recorder.Store and workflow.RunQuerier.
func (s *Store) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	p, err := s.Records(ctx, recorder.Query{RunQuery: q})
	if err != nil {
		return workflow.RunPage{}, err
	}
	return recorder.InfoPage(p), nil
}

// where builds the WHERE clause of q (everything but paging) over the
// runs table aliased r.
func (s *Store) where(q recorder.Query, a *args) string {
	col := func(c string) string { return "r." + s.d.quote(c) }
	var conds []string
	eq := func(c, v string) { conds = append(conds, col(c)+" = "+a.add(v)) }
	if !q.Ref.RunID.IsZero() {
		eq("run_id", string(q.Ref.RunID))
	}
	if !q.Ref.ScheduledID.IsZero() {
		eq("scheduled_id", string(q.Ref.ScheduledID))
	}
	if q.Workflow != "" {
		eq("workflow", q.Workflow)
	}
	if q.Queue != "" {
		eq("queue", q.Queue)
	}
	if q.Tenant != "" {
		eq("tenant", q.Tenant)
	}
	if len(q.Statuses) > 0 {
		vs := make([]string, len(q.Statuses))
		for i, st := range q.Statuses {
			vs[i] = string(st)
		}
		conds = append(conds, col("status")+" IN "+in(a, vs))
	}
	if len(q.Sources) > 0 {
		vs := make([]string, len(q.Sources))
		for i, src := range q.Sources {
			vs[i] = string(src)
		}
		conds = append(conds, col("source")+" IN "+in(a, vs))
	}
	if len(q.Outcomes) > 0 {
		vs := make([]string, len(q.Outcomes))
		for i, o := range q.Outcomes {
			vs[i] = string(o)
		}
		conds = append(conds, col("outcome")+" IN "+in(a, vs))
	}
	if q.OnlyStale {
		conds = append(conds, col("stale")+" = 1")
	}
	labels := s.table(tableLabels)
	lq := s.d.quote
	// labelExists adds the key's placeholder before the value's: with "?"
	// placeholders the argument order is the text order, so the value's
	// condition is built inside, after the key.
	labelExists := func(k string, valueCond func() string) string {
		head := "EXISTS (SELECT 1 FROM " + labels + " l WHERE l." + lq("run_key") + " = " + col("run_key") +
			" AND l." + lq("k") + " = " + a.add(k) + " AND "
		return head + valueCond() + ")"
	}
	for _, k := range slices.Sorted(maps.Keys(q.Labels)) {
		conds = append(conds, labelExists(k, func() string { return "l." + lq("v") + " = " + a.add(q.Labels[k]) }))
	}
	for _, k := range slices.Sorted(maps.Keys(q.LabelPrefixes)) {
		conds = append(conds, labelExists(k, func() string {
			return "l." + lq("v") + " LIKE " + a.add(escapeLike(q.LabelPrefixes[k])+"%") + " ESCAPE '" + likeEscape + "'"
		}))
	}
	if !q.From.IsZero() {
		conds = append(conds, col("enqueued_at")+" >= "+a.add(nanos(q.From)))
	}
	if !q.To.IsZero() {
		conds = append(conds, col("enqueued_at")+" < "+a.add(nanos(q.To)))
	}
	if !q.DueFrom.IsZero() || !q.DueTo.IsZero() {
		conds = append(conds, col("run_at")+" <> 0")
		if !q.DueFrom.IsZero() {
			conds = append(conds, col("run_at")+" >= "+a.add(nanos(q.DueFrom)))
		}
		if !q.DueTo.IsZero() {
			conds = append(conds, col("run_at")+" < "+a.add(nanos(q.DueTo)))
		}
	}
	if q.Search != "" {
		pat := "%" + escapeLike(strings.ToLower(q.Search)) + "%"
		var ors []string
		for _, c := range []string{"run_id", "scheduled_id", "workflow", "source_ref"} {
			ors = append(ors, "LOWER(COALESCE("+col(c)+", '')) LIKE "+a.add(pat)+" ESCAPE '"+likeEscape+"'")
		}
		conds = append(conds, "("+strings.Join(ors, " OR ")+")")
	}
	if len(conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(conds, " AND ")
}

// Records implements recorder.Store: newest enqueue first, ties by key,
// descending, in pages of q.PageLimit with a recorder.EncodeCursor cursor.
func (s *Store) Records(ctx context.Context, q recorder.Query) (recorder.RecordPage, error) {
	if err := recorder.ValidateQuery(q); err != nil {
		return recorder.RecordPage{}, err
	}
	a := &args{d: s.d}
	where := s.where(q, a)
	stmt := s.selectRuns() + where
	enq, key := "r."+s.d.quote("enqueued_at"), "r."+s.d.quote("run_key")
	if q.Cursor != "" {
		c, err := recorder.DecodeCursor(q.Cursor)
		if err != nil {
			return recorder.RecordPage{}, err
		}
		join := " WHERE "
		if where != "" {
			join = " AND "
		}
		t := nanos(c.EnqueuedAt)
		stmt += join + "(" + enq + " < " + a.add(t) + " OR (" + enq + " = " + a.add(t) + " AND " + key + " < " + a.add(c.Key) + "))"
	}
	limit := q.PageLimit()
	stmt += " ORDER BY " + enq + " DESC, " + key + " DESC LIMIT " + fmt.Sprint(limit+1)
	rows, err := s.db.QueryContext(ctx, stmt, a.vals...)
	if err != nil {
		return recorder.RecordPage{}, fmt.Errorf("sqlrecorder: list runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var page recorder.RecordPage
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return recorder.RecordPage{}, err
		}
		page.Runs = append(page.Runs, r)
	}
	if err := rows.Err(); err != nil {
		return recorder.RecordPage{}, err
	}
	if len(page.Runs) > limit {
		page.Runs = page.Runs[:limit]
		last := page.Runs[limit-1]
		page.Next = recorder.EncodeCursor(recorder.Cursor{EnqueuedAt: last.EnqueuedAt, Key: last.Key()})
	}
	return page, nil
}

// Run implements recorder.Store: the run with its steps and their events.
func (s *Store) Run(ctx context.Context, ref workflow.RunRef) (recorder.RunRecord, error) {
	if err := recorder.CheckRef("Run", ref); err != nil {
		return recorder.RunRecord{}, err
	}
	a := &args{d: s.d}
	var conds []string
	if !ref.RunID.IsZero() {
		conds = append(conds, "r."+s.d.quote("run_id")+" = "+a.add(string(ref.RunID)))
	}
	if !ref.ScheduledID.IsZero() {
		conds = append(conds, "r."+s.d.quote("scheduled_id")+" = "+a.add(string(ref.ScheduledID)))
	}
	// A RunID row wins over a ScheduledID-only row of the same run (two
	// rows exist only until a write joins them).
	stmt := s.selectRuns() + " WHERE " + strings.Join(conds, " OR ") + " ORDER BY r." + s.d.quote("run_key") + " ASC LIMIT 1"
	rec, err := scanRun(s.db.QueryRowContext(ctx, stmt, a.vals...))
	if errors.Is(err, sql.ErrNoRows) {
		return recorder.RunRecord{}, fmt.Errorf("sqlrecorder: run %s: %w", recorder.KeyOf(ref), workflow.ErrNotFound)
	}
	if err != nil {
		return recorder.RunRecord{}, fmt.Errorf("sqlrecorder: read run: %w", err)
	}
	if rec.Ref.RunID.IsZero() {
		return rec, nil
	}
	steps, err := s.steps(ctx, rec.Ref.RunID)
	if err != nil {
		return recorder.RunRecord{}, err
	}
	rec.Steps = steps
	return rec, nil
}

// steps reads a run's step rows and their events, in RunRecord order.
func (s *Store) steps(ctx context.Context, runID workflow.RunID) ([]recorder.StepRecord, error) {
	q := s.d.quote
	a := &args{d: s.d}
	rows, err := s.db.QueryContext(ctx, s.selectSteps()+" WHERE "+q("run_id")+" = "+a.add(string(runID)), a.vals...)
	if err != nil {
		return nil, fmt.Errorf("sqlrecorder: read steps: %w", err)
	}
	var steps []recorder.StepRecord
	for rows.Next() {
		sr, err := scanStep(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		steps = append(steps, sr)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlrecorder: read steps: %w", err)
	}
	if len(steps) == 0 {
		return nil, nil
	}
	b := &args{d: s.d}
	stmt := "SELECT " + q("step") + ", " + q("attempt") + ", " + q("seq") + ", " + q("message") + ", " + q("level") + ", " + q("attrs") + ", " + q("at") +
		" FROM " + s.table(tableEvents) + " WHERE " + q("run_id") + " = " + b.add(string(runID)) +
		" ORDER BY " + q("step") + ", " + q("attempt") + ", " + q("seq")
	erows, err := s.db.QueryContext(ctx, stmt, b.vals...)
	if err != nil {
		return nil, fmt.Errorf("sqlrecorder: read events: %w", err)
	}
	defer func() { _ = erows.Close() }()
	for erows.Next() {
		var (
			step, attrs         string
			attempt, seq, level int64
			at                  int64
			e                   recorder.EventRecord
		)
		if err := erows.Scan(&step, &attempt, &seq, &e.Message, &level, &attrs, &at); err != nil {
			return nil, err
		}
		e.Seq, e.Level, e.At = int(seq), slog.Level(level), fromNanos(at)
		if err := decodeJSON(attrs, &e.Attrs); err != nil {
			return nil, fmt.Errorf("sqlrecorder: event attrs: %w", err)
		}
		for i := range steps {
			if steps[i].Step == step && steps[i].Attempt == int(attempt) {
				steps[i].Events = append(steps[i].Events, e)
				break
			}
		}
	}
	if err := erows.Err(); err != nil {
		return nil, err
	}
	recorder.SortSteps(steps)
	return steps, nil
}

// terminalStatuses is every terminal status, for the queries that skip or
// pick finished runs. TestTerminalStatuses pins it to Status.IsTerminal.
var terminalStatuses = []string{
	string(workflow.StatusSucceeded), string(workflow.StatusFailed),
	string(workflow.StatusCancelled), string(workflow.StatusDiscarded),
}

// Pending implements recorder.Store.
func (s *Store) Pending(ctx context.Context, limit int) ([]recorder.RunRecord, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: sqlrecorder Pending: limit %d is not positive", workflow.ErrInvalid, limit)
	}
	a := &args{d: s.d}
	stmt := s.selectRuns() + " WHERE r." + s.d.quote("status") + " NOT IN " + in(a, terminalStatuses) +
		" ORDER BY r." + s.d.quote("checked_at") + " ASC, r." + s.d.quote("run_key") + " ASC LIMIT " + fmt.Sprint(limit)
	rows, err := s.db.QueryContext(ctx, stmt, a.vals...)
	if err != nil {
		return nil, fmt.Errorf("sqlrecorder: pending: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []recorder.RunRecord
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
