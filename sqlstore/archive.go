package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// Hot and cold tables. The queue's working tables (runs, steps and the
// rows that hang off them) are hot: every claim, admission and status
// change reads or rewrites them, so on Postgres every update leaves a dead
// row version behind for VACUUM, and their indexes grow with every run
// kept. Finished runs only cost space there, so retention (WithRetention)
// deletes them. WithArchiveTable keeps that history without keeping it
// hot: Prune moves each run, with its succeeded steps' outputs, into the
// cold tables in the same transaction that deletes it from the hot ones.
// The cold tables are append-only (written once, never updated), so they
// add no dead rows and no work to any query the queue runs.
const (
	tblRunsArchive  = "runs_archive"
	tblStepsArchive = "steps_archive"
)

// archiveDefs is the cold tables of migration 007.
func archiveDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblRunsArchive, lines: []string{
		// One row per run, moved once from runs.
		"id " + t.id + " NOT NULL PRIMARY KEY",
		"scheduled_id " + t.id,
		"workflow " + t.name + " NOT NULL",
		// status is the run's terminal state as runs stored it.
		"status " + t.short + " NOT NULL",
		"labels " + t.text + " NOT NULL",
		"payload " + t.blob,
		"err " + t.text,
		"created_at " + t.int + " NOT NULL",
		"finished_at " + t.int + " NOT NULL",
		// archived_at is when Prune moved it, on the scheduling clock.
		"archived_at " + t.int + " NOT NULL",
	}, indexes: []index{{"wf", "workflow, finished_at"}}}, {stem: tblStepsArchive, lines: []string{
		// The output of each step of an archived run that succeeded.
		"run_id " + t.id + " NOT NULL",
		"name " + t.name + " NOT NULL",
		"out_data " + t.blob,
		"PRIMARY KEY (run_id, name)",
	}}}
}

// WithArchiveTable makes Prune move finished runs into the cold tables
// (<prefix>runs_archive and <prefix>steps_archive) instead of dropping
// them: the queue's hot tables stay as small as retention keeps them while
// the history stays in the database, readable with ArchivedRuns. The move
// is one transaction per chunk, so a run is in exactly one of the two
// places. It works with WithArchive (whose hook still sees each chunk
// first) and needs WithRetention, which decides what is moved and when.
// The cold tables have no retention of their own: trim them with the
// app's own DELETE, or partition them, as the history's owner decides.
func WithArchiveTable() Option { return func(cl *Client) { cl.archiveTable = true } }

// ArchiveQuery selects archived runs (ArchivedRuns). Zero fields select
// everything.
type ArchiveQuery struct {
	// Workflow keeps one workflow's runs.
	Workflow string
	// RunID keeps one run.
	RunID workflow.RunID
	// FinishedBefore keeps runs that finished before it, for paging: pass
	// the last FinishedAt of the previous page.
	FinishedBefore time.Time
	// Limit caps the result; zero or less means 100.
	Limit int
}

// defaultArchivePage is ArchiveQuery's Limit when it sets none.
const defaultArchivePage = 100

// ArchivedRuns reads runs WithArchiveTable moved to the cold tables,
// newest finish first, each with its succeeded steps' outputs. It reads
// the tables directly and needs Migrate, not Start.
func (c *Client) ArchivedRuns(ctx context.Context, q ArchiveQuery) ([]FinishedRun, error) {
	where := "1 = 1"
	var args []any
	if q.Workflow != "" {
		where += " AND workflow = ?"
		args = append(args, q.Workflow)
	}
	if q.RunID != "" {
		where += " AND id = ?"
		args = append(args, string(q.RunID))
	}
	if !q.FinishedBefore.IsZero() {
		where += " AND finished_at < ?"
		args = append(args, q.FinishedBefore.UnixNano())
	}
	limit := q.Limit
	if limit <= 0 {
		limit = defaultArchivePage
	}
	var out []FinishedRun
	idx := map[string]int{}
	var id, wf, status, labels string
	var sid, errMsg sql.NullString
	var payload []byte
	var created, finished int64
	err := c.st.each(ctx, c.st.db, "SELECT id, scheduled_id, workflow, status, labels, payload, err, created_at, finished_at FROM {runs_archive} WHERE "+
		where+" ORDER BY finished_at DESC, id LIMIT ?", append(args, limit), func() error {
		r := runRow{id: id, labels: labels}
		lm, lerr := r.labelMap()
		if lerr != nil {
			c.log.Error("sqlstore: archived run without labels", "run", id, "error", lerr)
		}
		idx[id] = len(out)
		out = append(out, FinishedRun{
			Ref:        workflow.RunRef{RunID: workflow.RunID(id), ScheduledID: workflow.ScheduledID(sid.String)},
			Workflow:   wf,
			Status:     workflow.Status(status),
			Labels:     lm,
			Payload:    append([]byte(nil), payload...),
			Err:        errMsg.String,
			CreatedAt:  unixTime(created),
			FinishedAt: unixTime(finished),
		})
		return nil
	}, &id, &sid, &wf, &status, &labels, &payload, &errMsg, &created, &finished)
	if err != nil || len(out) == 0 {
		return out, err
	}
	ids := make([]any, len(out))
	for i, fr := range out {
		ids[i] = string(fr.Ref.RunID)
	}
	var runID, name string
	var data []byte
	err = c.st.each(ctx, c.st.db, "SELECT run_id, name, out_data FROM {steps_archive} WHERE run_id IN ("+inList(len(ids))+")", ids, func() error {
		fr := &out[idx[runID]]
		if fr.Outputs == nil {
			fr.Outputs = map[string][]byte{}
		}
		fr.Outputs[name] = append([]byte(nil), data...)
		return nil
	}, &runID, &name, &data)
	return out, err
}

// moveToArchive copies, in tx, the runs among ids that cond still selects
// (locked first, so a run retried meanwhile is neither copied nor deleted)
// into the cold tables, and returns those runs' ids for the delete.
func (c *Client) moveToArchive(ctx context.Context, tx *sql.Tx, cond string, args []any) ([]any, error) {
	lock := ""
	if c.st.engine != engineSQLite {
		lock = " FOR UPDATE"
	} else if _, err := c.st.exec(ctx, tx, "UPDATE {runs} SET status = status WHERE "+cond, args...); err != nil {
		// SQLite has no row locks. A transaction that reads first and
		// writes second fails at once with SQLITE_BUSY when another
		// connection committed in between, and the busy timeout does not
		// apply to that upgrade; a write first takes the database's write
		// lock (waiting out the timeout) so the reads below see what the
		// writes will change.
		return nil, fmt.Errorf("sqlstore: lock runs to archive: %w", err)
	}
	var keep []any
	var id string
	if err := c.st.each(ctx, tx, "SELECT id FROM {runs} WHERE "+cond+lock, args, func() error { keep = append(keep, id); return nil }, &id); err != nil {
		return nil, fmt.Errorf("sqlstore: lock runs to archive: %w", err)
	}
	if len(keep) == 0 {
		return nil, nil
	}
	in := "(" + inList(len(keep)) + ")"
	if _, err := c.st.exec(ctx, tx, "INSERT INTO {runs_archive} (id, scheduled_id, workflow, status, labels, payload, err, created_at, finished_at, archived_at)"+
		" SELECT id, scheduled_id, workflow, status, labels, payload, err, created_at, finished_at, ? FROM {runs} WHERE id IN "+in,
		append([]any{c.nowNanos()}, keep...)...); err != nil {
		return nil, fmt.Errorf("sqlstore: archive runs: %w", err)
	}
	if _, err := c.st.exec(ctx, tx, "INSERT INTO {steps_archive} (run_id, name, out_data) SELECT run_id, name, out_data FROM {steps} WHERE status = ? AND run_id IN "+in,
		append([]any{stepSucceeded}, keep...)...); err != nil {
		return nil, fmt.Errorf("sqlstore: archive step outputs: %w", err)
	}
	return keep, nil
}
