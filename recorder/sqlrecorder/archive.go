package sqlrecorder

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/recorder"
)

// WithArchiveTable makes Prune's stage two move each expired run into
// <prefix>archive (created by Migrate for a store with this option; the
// default schema and SchemaVersion do not change) instead of only
// deleting it: the whole record (run, steps, events, labels) is written
// there as one JSON row in the same transaction that deletes it from the
// hot tables, so a run is in exactly one of the two places and the move
// is exactly once, whatever stops a prune (a cancelled context, a crash,
// a failed commit: the batch rolls back whole and the next prune moves
// it). Read the archive with ArchivedRun and ArchivedRuns, or with SQL.
//
// It works with Retention.Archive, whose callback still sees each batch
// first and keeps its at-least-once delivery, and needs WithRetention,
// which decides what is moved and when. The archive table has no
// retention of its own: trim it with the app's own DELETE on finished_at
// or archived_at.
//
//ds:def id=sqlrec-witharchivetable-cduufku5 owner=@khanakia stability=stable
func WithArchiveTable() Option {
	return func(s *Store) error {
		s.archiveTable = true
		return nil
	}
}

// archiveColumns are the archive table's columns, in insert order.
var archiveColumns = []string{"run_key", "run_id", "workflow", "tenant", "status", "finished_at", "archived_at", "record"}

// archiveDDL returns the archive table and its indexes, idempotent like
// ddl. They are outside SchemaVersion: a later change to this table needs
// its own step.
func (s *Store) archiveDDL() []string {
	d := s.d
	id, txt, num := d.idType, d.textType(), d.intType()
	q := d.quote
	cols := []string{
		// One row per archived run, keyed like <prefix>runs.
		q("run_key") + " " + id(keyWidth) + " NOT NULL PRIMARY KEY",
		q("run_id") + " " + id(keyWidth) + " NULL",
		q("workflow") + " " + id(nameWidth) + " NOT NULL",
		q("tenant") + " " + id(nameWidth) + " NOT NULL",
		q("status") + " " + id(statusWidth) + " NOT NULL",
		// finished_at is the run's end, archived_at the store's clock at
		// the move; Unix nanoseconds like every other time column.
		q("finished_at") + " " + num + " NOT NULL",
		q("archived_at") + " " + num + " NOT NULL",
		// record is the recorder.RunRecord, steps and events included, as
		// JSON.
		q("record") + " " + txt + " NOT NULL",
	}
	type index struct {
		name string
		cols []string
	}
	idx := []index{
		{"run_id", []string{"run_id"}},
		{"finished", []string{"finished_at", "run_key"}},
		{"workflow", []string{"workflow", "finished_at"}},
	}
	quoted := func(cs []string) string {
		out := make([]string, len(cs))
		for i, c := range cs {
			out[i] = q(c)
		}
		return strings.Join(out, ", ")
	}
	name := func(ix index) string { return q(s.prefix + tableArchive + "_" + ix.name) }
	if d == MySQL {
		for _, ix := range idx {
			cols = append(cols, "INDEX "+name(ix)+" ("+quoted(ix.cols)+")")
		}
		return []string{"CREATE TABLE IF NOT EXISTS " + s.table(tableArchive) + " (\n  " + strings.Join(cols, ",\n  ") + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"}
	}
	out := []string{"CREATE TABLE IF NOT EXISTS " + s.table(tableArchive) + " (\n  " + strings.Join(cols, ",\n  ") + "\n)"}
	for _, ix := range idx {
		out = append(out, "CREATE INDEX IF NOT EXISTS "+name(ix)+" ON "+s.table(tableArchive)+" ("+quoted(ix.cols)+")")
	}
	return out
}

// moveRun moves the run of victim v into the archive table inside tx:
// it reads the run (locking its row), deletes it with its steps, events
// and labels, and writes the archive row. moved is false when the run is
// gone already (another process pruned it first), and then nothing is
// written.
func (s *Store) moveRun(ctx context.Context, tx *sql.Tx, v victim) (moved bool, err error) {
	q := s.d.quote
	a := &args{d: s.d}
	rec, err := scanRun(tx.QueryRowContext(ctx, s.selectRuns()+" WHERE r."+q("run_key")+" = "+a.add(v.key)+s.d.forUpdate(), a.vals...))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read run %s: %w", v.key, err)
	}
	if !rec.Ref.RunID.IsZero() {
		if rec.Steps, err = s.steps(ctx, tx, rec.Ref.RunID); err != nil {
			return false, err
		}
	}
	body, err := encodeJSON(rec)
	if err != nil {
		return false, fmt.Errorf("encode run %s: %w", v.key, err)
	}
	b := &args{d: s.d}
	res, err := tx.ExecContext(ctx, "DELETE FROM "+s.table(tableRuns)+" WHERE "+q("run_key")+" = "+b.add(v.key), b.vals...)
	if err != nil {
		return false, err
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return false, err // deleted by another prune since the read
	}
	if !rec.Ref.RunID.IsZero() {
		if err := s.deleteSteps(ctx, tx, rec.Ref.RunID); err != nil {
			return false, err
		}
	}
	if err := s.writeLabels(ctx, tx, v.key, "", nil); err != nil {
		return false, err
	}
	// A run archived before under the same key (recorded again since, then
	// expired again) keeps only its newest copy.
	c := &args{d: s.d}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.table(tableArchive)+" WHERE "+q("run_key")+" = "+c.add(v.key), c.vals...); err != nil {
		return false, err
	}
	ins := &args{d: s.d}
	vals := []string{
		ins.add(v.key), ins.add(nullID(string(rec.Ref.RunID))), ins.add(rec.Workflow), ins.add(rec.Tenant),
		ins.add(string(rec.Status)), ins.add(nanos(rec.FinishedAt)), ins.add(nanos(s.clk.Now())), ins.add(body),
	}
	cols := make([]string, len(archiveColumns))
	for i, col := range archiveColumns {
		cols[i] = q(col)
	}
	stmt := "INSERT INTO " + s.table(tableArchive) + " (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")"
	if _, err := tx.ExecContext(ctx, stmt, ins.vals...); err != nil {
		return false, err
	}
	return true, nil
}

// ArchiveQuery selects archived runs (ArchivedRuns). Zero fields select
// everything.
//
//ds:def id=sqlrec-archivequery-56dg4s52 owner=@khanakia stability=stable
type ArchiveQuery struct {
	// Workflow and Tenant keep only the runs of that workflow or tenant.
	Workflow string
	Tenant   string

	// Limit is the page size: zero means workflow.DefaultRunsLimit, more
	// than workflow.MaxRunsLimit is refused.
	Limit int

	// Cursor is the Next of the previous page; empty starts at the oldest
	// run.
	Cursor string
}

// ArchivePage is one page of ArchivedRuns, oldest finish first.
//
//ds:def id=sqlrec-archivepage-fgu6n8d6 owner=@khanakia stability=stable
type ArchivePage struct {
	Runs []recorder.RunRecord

	// Next is the Cursor of the following page; empty on the last one.
	Next string
}

// ArchivedRuns lists the runs WithArchiveTable moved, oldest finish
// first (ties by run key), a page at a time. Paging is by (finished_at,
// run key), so runs that finished at the same instant are never skipped
// across a page boundary. A Cursor it did not issue, a negative Limit or
// one over workflow.MaxRunsLimit fail with workflow.ErrInvalid.
//
//ds:def id=sqlrec-store-archivedruns-39uycka3 owner=@khanakia stability=stable
func (s *Store) ArchivedRuns(ctx context.Context, aq ArchiveQuery) (ArchivePage, error) {
	if aq.Limit < 0 || aq.Limit > workflow.MaxRunsLimit {
		return ArchivePage{}, fmt.Errorf("%w: sqlrecorder: ArchiveQuery.Limit %d is outside [0, %d]", workflow.ErrInvalid, aq.Limit, workflow.MaxRunsLimit)
	}
	limit := aq.Limit
	if limit == 0 {
		limit = workflow.DefaultRunsLimit
	}
	q := s.d.quote
	a := &args{d: s.d}
	var conds []string
	if aq.Workflow != "" {
		conds = append(conds, q("workflow")+" = "+a.add(aq.Workflow))
	}
	if aq.Tenant != "" {
		conds = append(conds, q("tenant")+" = "+a.add(aq.Tenant))
	}
	if aq.Cursor != "" {
		fin, key, err := decodeArchiveCursor(aq.Cursor)
		if err != nil {
			return ArchivePage{}, err
		}
		conds = append(conds, "("+q("finished_at")+" > "+a.add(fin)+" OR ("+q("finished_at")+" = "+a.add(fin)+" AND "+q("run_key")+" > "+a.add(key)+"))")
	}
	stmt := "SELECT " + q("run_key") + ", " + q("finished_at") + ", " + q("record") + " FROM " + s.table(tableArchive)
	if len(conds) > 0 {
		stmt += " WHERE " + strings.Join(conds, " AND ")
	}
	stmt += " ORDER BY " + q("finished_at") + ", " + q("run_key") + " LIMIT " + strconv.Itoa(limit+1)
	rows, err := s.db.QueryContext(ctx, stmt, a.vals...)
	if err != nil {
		return ArchivePage{}, fmt.Errorf("sqlrecorder: list archived runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var (
		page ArchivePage
		keys []string
		fins []int64
	)
	for rows.Next() {
		var (
			key, body string
			fin       int64
			rec       recorder.RunRecord
		)
		if err := rows.Scan(&key, &fin, &body); err != nil {
			return ArchivePage{}, fmt.Errorf("sqlrecorder: list archived runs: %w", err)
		}
		if err := decodeJSON(body, &rec); err != nil {
			return ArchivePage{}, fmt.Errorf("sqlrecorder: archived run %s: %w", key, err)
		}
		page.Runs, keys, fins = append(page.Runs, rec), append(keys, key), append(fins, fin)
	}
	if err := rows.Err(); err != nil {
		return ArchivePage{}, fmt.Errorf("sqlrecorder: list archived runs: %w", err)
	}
	if len(page.Runs) > limit {
		page.Runs = page.Runs[:limit]
		page.Next = encodeArchiveCursor(fins[limit-1], keys[limit-1])
	}
	return page, nil
}

// ArchivedRun returns one run WithArchiveTable moved, with its steps and
// events, as Run returned it before the move; workflow.ErrNotFound when
// it is not in the archive table.
//
//ds:def id=sqlrec-store-archivedrun-x7b4wy6a owner=@khanakia stability=stable
func (s *Store) ArchivedRun(ctx context.Context, ref workflow.RunRef) (recorder.RunRecord, error) {
	if err := recorder.CheckRef("ArchivedRun", ref); err != nil {
		return recorder.RunRecord{}, err
	}
	q := s.d.quote
	a := &args{d: s.d}
	var conds []string
	if !ref.RunID.IsZero() {
		conds = append(conds, q("run_key")+" = "+a.add(recorder.KeyOf(workflow.RunRef{RunID: ref.RunID})))
	}
	if !ref.ScheduledID.IsZero() {
		conds = append(conds, q("run_key")+" = "+a.add(recorder.KeyOf(workflow.RunRef{ScheduledID: ref.ScheduledID})))
	}
	// A RunID row wins over a ScheduledID-only row, as in Run.
	stmt := "SELECT " + q("record") + " FROM " + s.table(tableArchive) + " WHERE " + strings.Join(conds, " OR ") + " ORDER BY " + q("run_key") + " ASC LIMIT 1"
	var body string
	err := s.db.QueryRowContext(ctx, stmt, a.vals...).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return recorder.RunRecord{}, fmt.Errorf("sqlrecorder: archived run %s: %w", recorder.KeyOf(ref), workflow.ErrNotFound)
	}
	if err != nil {
		return recorder.RunRecord{}, fmt.Errorf("sqlrecorder: read archived run: %w", err)
	}
	var rec recorder.RunRecord
	if err := decodeJSON(body, &rec); err != nil {
		return recorder.RunRecord{}, fmt.Errorf("sqlrecorder: archived run %s: %w", recorder.KeyOf(ref), err)
	}
	return rec, nil
}

// archiveCursorSep separates an archive cursor's finish time from its run
// key; the time part is a plain integer.
const archiveCursorSep = "|"

func encodeArchiveCursor(fin int64, key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(fin, 10) + archiveCursorSep + key))
}

func decodeArchiveCursor(c string) (int64, string, error) {
	bad := fmt.Errorf("%w: sqlrecorder: bad archive cursor", workflow.ErrInvalid)
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, "", bad
	}
	ts, key, ok := strings.Cut(string(raw), archiveCursorSep)
	if !ok || key == "" {
		return 0, "", bad
	}
	fin, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return 0, "", bad
	}
	return fin, key, nil
}
