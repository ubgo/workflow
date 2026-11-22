// Command sql-archive keeps the SQL driver's queue tables small without
// losing history: Prune moves finished runs, with the outputs of their
// succeeded steps, into the cold archive tables (WithArchiveTable), and
// ArchivedRuns reads them back a page at a time. It also sets how many rows
// one dispatcher query reads (WithFetchBatch) and makes its own run and
// step IDs (WithIDGenerator), so the archived IDs it prints are stable.
//
// It runs on a SQLite file in a temporary directory and needs nothing
// installed. A fake clock stands in for the hours passing.
//
// It is the program behind the "Keep the hot tables small" and "Run IDs"
// sections of docs/drivers/sqlstore.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

const (
	// invoices is how many runs the program finishes and archives.
	invoices = 3
	// pageSize is the ArchiveQuery limit, below invoices so the program
	// shows paging, with a tie across the page boundary.
	pageSize = 2
	// keepSucceeded is the retention of succeeded runs in the hot tables.
	keepSucceeded = time.Hour
	// fetchBatch is the dispatcher's rows per query; the default is 100.
	fetchBatch = 50
	// runTimeout bounds the wait for one run, generous for a slow CI host.
	runTimeout = 10 * time.Second
	// statusPoll is how often finish re-reads a run's status.
	statusPoll = 5 * time.Millisecond
)

// start is the fake clock's first reading, a fixed date so the output is
// the same on every run.
var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sql-archive:", err)
		os.Exit(1)
	}
}

// snippet:ids
// sequentialIDs numbers every ID per kind: "run_000001", "step_000001".
// An ID must be 1 to 64 bytes and unique per kind across every client of
// the database, so a real app uses a UUIDv7 or ULID; a counter in one
// process is enough for this demo and keeps its output stable.
type sequentialIDs struct{ runs, steps, others atomic.Int64 }

// next is the sqlstore.IDGenerator: it is told which kind of ID to make.
func (s *sequentialIDs) next(kind sqlstore.IDKind) (string, error) {
	n := &s.others // IDScheduled (a delayed run) and IDSchedule (an unnamed schedule)
	switch kind {
	case sqlstore.IDRun:
		n = &s.runs
	case sqlstore.IDStep:
		n = &s.steps
	case sqlstore.IDScheduled, sqlstore.IDSchedule:
	}
	return fmt.Sprintf("%s_%06d", kind, n.Add(1)), nil
}

// snippet:end

// invoice renders then sends one invoice; both steps keep an output, which
// the archive keeps too.
var invoice = workflow.Workflow{Name: "invoice", Steps: []workflow.Step{
	{Name: "render", Run: func(_ context.Context, in []byte) ([]byte, error) {
		return []byte(`"pdf:` + strings.Trim(string(in), `"`) + `"`), nil
	}},
	{Name: "send", After: []string{"render"}, Run: func(context.Context, []byte) ([]byte, error) {
		return []byte(`"sent"`), nil
	}},
}}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) (err error) {
	dir, err := os.MkdirTemp("", "sql-archive-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	db, err := driversqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	clk := clock.NewFake(start)
	ids := &sequentialIDs{}

	// snippet:archive
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
		sqlstore.WithRetention(sqlstore.Retention{Succeeded: keepSucceeded, Undelivered: true}),
		sqlstore.WithArchiveTable(),         // Prune moves runs to wf_runs_archive and wf_steps_archive
		sqlstore.WithFetchBatch(fetchBatch), // rows per dispatcher query (default 100)
		sqlstore.WithIDGenerator(ids.next),  // run, step and delayed-start IDs
		sqlstore.WithoutAutoPrune(),         // this program calls Prune itself
		sqlstore.WithClock(clk),
	)
	// snippet:end
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil {
		return err
	}
	if err := c.Register(invoice); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(runCtx); err != nil {
		return err
	}
	defer c.Stop()

	var first workflow.RunRef
	for i := 1; i <= invoices; i++ {
		ref, err := finish(ctx, c, workflow.Job{Workflow: invoice.Name, Payload: fmt.Appendf(nil, `"INV-%d"`, i)})
		if err != nil {
			return err
		}
		if i == 1 {
			first = ref
		}
		if i == 2 {
			clk.Advance(time.Minute) // INV-1 and INV-2 finish at the same instant, INV-3 a minute later
		}
	}
	clk.Advance(2 * keepSucceeded) // past the retention: every run may go

	// snippet:prune
	res, err := c.Prune(ctx) // moves, then deletes, in one transaction per chunk
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "pruned %d runs from the hot tables\n", res.Deleted)
	if _, err := c.Status(ctx, first); errors.Is(err, workflow.ErrNotFound) {
		fmt.Fprintf(w, "status of %s: not found (it left the queue)\n", first.RunID)
	}
	// snippet:end

	// snippet:read
	var before time.Time     // zero: start from the newest finish
	var after workflow.RunID // the tie-breaker: runs that finished at before, after this ID
	for page := 1; ; page++ {
		runs, err := c.ArchivedRuns(ctx, sqlstore.ArchiveQuery{Workflow: invoice.Name, FinishedBefore: before, AfterRunID: after, Limit: pageSize})
		if err != nil {
			return err
		}
		if len(runs) == 0 {
			break
		}
		for _, r := range runs {
			fmt.Fprintf(w, "page %d: %s %s %s outputs %s\n", page, r.Ref.RunID, r.Status, r.Payload, outputs(r.Outputs))
		}
		last := runs[len(runs)-1] // the next page starts after it in (finish, ID) order
		before, after = last.FinishedAt, last.Ref.RunID
	}
	// snippet:end
	return nil
}

// outputs prints a run's step outputs sorted by step name.
func outputs(m map[string][]byte) string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = name + "=" + string(m[name])
	}
	return strings.Join(parts, " ")
}

// finish enqueues job and waits until its run has ended.
func finish(ctx context.Context, c *sqlstore.Client, job workflow.Job) (workflow.RunRef, error) {
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		return ref, err
	}
	deadline := time.Now().Add(runTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return ref, err
		}
		if st.IsTerminal() {
			return ref, nil
		}
		if time.Now().After(deadline) {
			return ref, fmt.Errorf("run %s is %s after %s", ref.RunID, st, runTimeout)
		}
		time.Sleep(statusPoll)
	}
}
