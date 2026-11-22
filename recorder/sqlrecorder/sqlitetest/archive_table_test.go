package sqlitetest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/sqlrecorder"
)

// archivedKeys counts the archive table's rows per run key.
func archivedKeys(t *testing.T, s *sqlrecorder.Store) map[string]int {
	t.Helper()
	out := map[string]int{}
	cursor := ""
	for {
		page, err := s.ArchivedRuns(context.Background(), sqlrecorder.ArchiveQuery{Limit: 1, Cursor: cursor})
		must(t, err)
		for _, r := range page.Runs {
			out[r.Key()]++
		}
		if page.Next == "" {
			return out
		}
		cursor = page.Next
	}
}

// TestArchiveTableMovesOnce: with WithArchiveTable, stage two moves each
// expired run, whole (steps, events, labels), into <prefix>archive in the
// transaction that deletes it, so ArchivedRun returns what Run returned
// and the hot tables keep only the live and kept runs.
func TestArchiveTableMovesOnce(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(t0)
	s, db, _ := newStore(t, sqlrecorder.WithClock(clk), sqlrecorder.WithArchiveTable(), sqlrecorder.WithRetention(prunePlan(nil)))
	seedPrune(t, s, clk)
	before := map[string]recorder.RunRecord{}
	for _, ref := range []workflow.RunRef{rid("old1"), rid("old2"), rid("old3"), {ScheduledID: "sch9"}} {
		r, err := s.Run(ctx, ref)
		must(t, err)
		before[r.Key()] = r
	}
	res, err := s.Prune(ctx)
	must(t, err)
	if res.RunsDeleted != 4 || res.Archived != 4 {
		t.Fatalf("result = %+v, want 4 deleted and 4 archived", res)
	}
	if n := count(t, db, "p_runs"); n != 2 {
		t.Fatalf("hot runs left = %d, want 2", n)
	}
	if n := count(t, db, "p_archive"); n != 4 {
		t.Fatalf("archive rows = %d, want 4", n)
	}
	for key, want := range before {
		ref := rid(strings.TrimPrefix(key, "r:"))
		if strings.HasPrefix(key, "s:") {
			ref = workflow.RunRef{ScheduledID: workflow.ScheduledID(strings.TrimPrefix(key, "s:"))}
		}
		got, err := s.ArchivedRun(ctx, ref)
		must(t, err)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("archived %s =\n%+v\nwant\n%+v", key, got, want)
		}
		if _, err := s.Run(ctx, ref); !errors.Is(err, workflow.ErrNotFound) {
			t.Errorf("%s still in the hot tables: %v", key, err)
		}
	}
	if _, err := s.ArchivedRun(ctx, rid("live")); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("ArchivedRun of a live run = %v, want ErrNotFound", err)
	}
	// Filters and paging.
	page, err := s.ArchivedRuns(ctx, sqlrecorder.ArchiveQuery{Workflow: "nope"})
	must(t, err)
	if len(page.Runs) != 0 {
		t.Errorf("workflow filter returned %d runs", len(page.Runs))
	}
	for key, n := range archivedKeys(t, s) {
		if n != 1 {
			t.Errorf("paging returned %s %d times", key, n)
		}
	}
	if _, err := s.ArchivedRuns(ctx, sqlrecorder.ArchiveQuery{Cursor: "junk!"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("bad cursor = %v, want ErrInvalid", err)
	}
}

// TestArchiveTableExactlyOnce: a prune cut off between steps leaves every
// expired run in exactly one place. The hot delete fails after the
// archive row is written, a commit fails, then the context is cancelled as
// the second batch writes its archive rows: each time that batch's
// transaction rolls back whole, and the next prune moves it. (Archiving in
// a transaction of its own before the delete, as the Archive callback
// does, fails the first case.)
func TestArchiveTableExactlyOnce(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(t0)
	s, db, in := newStore(t, sqlrecorder.WithClock(clk), sqlrecorder.WithArchiveTable(), sqlrecorder.WithRetention(prunePlan(nil)))
	seedPrune(t, s, clk)
	before := dump(t, db, prefix)

	// The delete fails after the archive row is written, then a commit
	// fails: neither leaves an archive row or changes the hot tables.
	for _, f := range []fault{
		{op: opExec, match: `DELETE FROM "p_runs"`, err: errBoom},
		{op: opCommit, times: 1, err: errBoom},
	} {
		in.add(f)
		if _, err := s.Prune(ctx); !errors.Is(err, errBoom) {
			t.Fatalf("prune with a failing step = %v, want the injected error", err)
		}
		if n := count(t, db, "p_archive"); n != 0 {
			t.Fatalf("a rolled-back move left %d archive rows", n)
		}
		if after := dump(t, db, prefix); after != before {
			t.Fatalf("a rolled-back move changed the hot tables:\n%s\nwant\n%s", after, before)
		}
		in.reset()
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	in.add(fault{op: opExec, match: `INSERT INTO "p_archive"`, skip: 2, times: 1, before: cancel})
	if _, err := s.Prune(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled prune = %v, want context.Canceled", err)
	}
	if n := count(t, db, "p_archive"); n != 2 {
		t.Fatalf("after the cancel: %d archive rows, want the first batch's 2", n)
	}
	if n := count(t, db, "p_runs"); n != 4 {
		t.Fatalf("after the cancel: %d hot runs, want 4 (the cancelled batch rolled back)", n)
	}
	in.reset()

	res, err := s.Prune(ctx)
	must(t, err)
	if res.RunsDeleted != 2 || res.Archived != 2 {
		t.Fatalf("last prune = %+v, want the 2 runs left moved", res)
	}
	keys := archivedKeys(t, s)
	if len(keys) != 4 {
		t.Fatalf("archived %v, want 4 runs", keys)
	}
	for key, n := range keys {
		if n != 1 {
			t.Errorf("%s archived %d times, want once", key, n)
		}
	}
	if n := count(t, db, "p_runs"); n != 2 {
		t.Fatalf("hot runs left = %d, want 2", n)
	}
}

// TestArchiveTableWithCallback: with Retention.Archive set too, the
// callback sees a batch whose move then fails again on the next prune
// (its documented at-least-once delivery), while the table holds each run
// once.
func TestArchiveTableWithCallback(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(t0)
	calls := map[string]int{}
	s, db, in := newStore(t, sqlrecorder.WithClock(clk), sqlrecorder.WithArchiveTable(), sqlrecorder.WithRetention(prunePlan(func(_ context.Context, rs []recorder.RunRecord) error {
		for _, r := range rs {
			calls[r.Key()]++
		}
		return nil
	})))
	seedPrune(t, s, clk)
	in.add(fault{op: opCommit, times: 1, err: errBoom})
	if _, err := s.Prune(ctx); !errors.Is(err, errBoom) {
		t.Fatalf("prune with a failing commit = %v, want the commit error", err)
	}
	in.reset()
	res, err := s.Prune(ctx)
	must(t, err)
	if res.RunsDeleted != 4 || res.Archived != 4 {
		t.Fatalf("second prune = %+v, want 4 deleted and 4 handed to Archive", res)
	}
	for key, n := range archivedKeys(t, s) {
		if n != 1 {
			t.Errorf("%s in the table %d times, want once", key, n)
		}
	}
	if n := count(t, db, "p_archive"); n != 4 {
		t.Fatalf("archive rows = %d, want 4", n)
	}
	twice := 0
	for _, n := range calls {
		if n == 2 {
			twice++
		}
	}
	if twice != 2 {
		t.Errorf("callback deliveries %v: want the rolled-back batch's 2 runs twice", calls)
	}
}

// TestArchiveTableMigrate: the archive table is opt-in. A default store's
// Migrate creates no archive table and its scripts are unchanged; with
// WithArchiveTable, Migrate creates it on a database a default store
// migrated before (the version stays SchemaVersion), concurrent Migrates
// all succeed, and a second Migrate changes nothing.
func TestArchiveTableMigrate(t *testing.T) {
	ctx := context.Background()
	plain, db, _ := newStore(t)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = 'p_archive'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("a default store made the archive table: %d, %v", n, err)
	}
	with, err := sqlrecorder.New(db, sqlrecorder.SQLite, sqlrecorder.WithTablePrefix(prefix), sqlrecorder.WithArchiveTable())
	must(t, err)
	if got, base := len(with.MigrationScripts()), len(plain.MigrationScripts()); got != base+4 {
		t.Fatalf("scripts with the archive table = %d, want %d + 4 (a table, three indexes)", got, base)
	}
	errs := make(chan error, 3)
	for range 3 {
		go func() { errs <- with.Migrate(ctx) }()
	}
	for range 3 {
		must(t, <-errs)
	}
	must(t, with.Migrate(ctx))
	if n := count(t, db, "p_archive"); n != 0 {
		t.Fatalf("archive rows after Migrate = %d", n)
	}
	var v int
	if err := db.QueryRow(`SELECT MAX("version") FROM "p_schema"`).Scan(&v); err != nil || v != sqlrecorder.SchemaVersion {
		t.Fatalf("schema version = %d, %v; want %d", v, err, sqlrecorder.SchemaVersion)
	}
}
