package sqlitetest

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/sqlrecorder"
)

// unmigrated opens a database through the fault wrapper and a store on it
// that has not run Migrate.
func unmigrated(t *testing.T, opts ...sqlrecorder.Option) (*sqlrecorder.Store, *sql.DB, *injector) {
	t.Helper()
	db, in := openDB(t)
	s, err := sqlrecorder.New(db, sqlrecorder.SQLite, append([]sqlrecorder.Option{sqlrecorder.WithTablePrefix(prefix)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return s, db, in
}

func schemaRows(t *testing.T, db *sql.DB) []int {
	t.Helper()
	rows, err := db.Query(`SELECT "version" FROM "p_schema"`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

// TestMigrate: the first Migrate creates every table and index and records
// the version once; a second only reads the version; scripts applied by an
// app's own tool are then recorded; a newer schema is refused naming both
// versions.
func TestMigrate(t *testing.T) {
	ctx := context.Background()
	s, db, in := unmigrated(t)
	must(t, s.Migrate(ctx))
	creates := 0
	for _, st := range in.statements() {
		if strings.HasPrefix(st, "CREATE ") {
			creates++
		}
	}
	if want := len(s.MigrationScripts()); creates != want || want != 15 {
		t.Fatalf("CREATE statements run = %d, scripts %d, want 15 (5 tables, 10 indexes)", creates, want)
	}
	if v := schemaRows(t, db); !slices.Equal(v, []int{sqlrecorder.SchemaVersion}) {
		t.Fatalf("schema rows = %v", v)
	}
	in.reset()
	must(t, s.Migrate(ctx))
	if st := in.statements(); len(st) != 1 || !strings.Contains(st[0], `MAX("version")`) {
		t.Fatalf("a migrated store ran %q", st)
	}

	// Scripts applied by hand: Migrate records the version.
	s2, db2, _ := unmigrated(t)
	for _, st := range s2.MigrationScripts() {
		if _, err := db2.Exec(st); err != nil {
			t.Fatal(err)
		}
	}
	if v := schemaRows(t, db2); len(v) != 0 {
		t.Fatalf("scripts recorded a version: %v", v)
	}
	must(t, s2.Migrate(ctx))
	if v := schemaRows(t, db2); !slices.Equal(v, []int{sqlrecorder.SchemaVersion}) {
		t.Fatalf("schema rows after scripts = %v", v)
	}

	if _, err := db.Exec(`UPDATE "p_schema" SET "version" = 7`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "version 7, newer than this release's 1") {
		t.Fatalf("newer schema: %v", err)
	}
}

// TestMigrateConcurrent: three Migrates at once on one fresh database all
// succeed and record one version row.
func TestMigrateConcurrent(t *testing.T) {
	s, db, _ := unmigrated(t)
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for range 3 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.Migrate(context.Background()) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		must(t, err)
	}
	if v := schemaRows(t, db); len(v) != 1 {
		t.Fatalf("schema rows = %v", v)
	}
}

// TestMigrateFailures: every failing step names itself and wraps the
// cause; contention on a statement is retried, up to the attempts, and a
// cancelled context stops the retry.
func TestMigrateFailures(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		f    fault
		msg  string
		is   bool
	}{
		{"create table", fault{op: opExec, match: `CREATE TABLE IF NOT EXISTS "p_steps"`}, `sqlrecorder: migrate: injected failure` + "\n" + `CREATE TABLE IF NOT EXISTS "p_steps"`, true},
		{"create index", fault{op: opExec, match: `CREATE INDEX IF NOT EXISTS "p_labels_kv"`}, `"p_labels_kv"`, true},
		{"record version", fault{op: opExec, match: `INSERT INTO "p_schema"`}, "record schema version", true},
		{"read version", fault{op: opQuery, match: `MAX("version")`}, "read schema version", true},
		{"read version scan", fault{op: opCorrupt, match: `MAX("version")`, col: 0, val: "x"}, "read schema version", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, db, in := unmigrated(t)
			if c.name == "read version scan" {
				// The table must exist for a row to come back.
				must(t, s.Migrate(ctx))
				if _, err := db.Exec(`DELETE FROM "p_schema"`); err != nil {
					t.Fatal(err)
				}
			}
			c.f.err = errBoom
			f := in.add(c.f)
			err := s.Migrate(ctx)
			if err == nil || !strings.Contains(err.Error(), c.msg) || (c.is && !errors.Is(err, errBoom)) {
				t.Fatalf("err = %v, want %q", err, c.msg)
			}
			if in.hits(f) != 1 {
				t.Fatalf("hits %d", in.hits(f))
			}
		})
	}
	t.Run("contention clears", func(t *testing.T) {
		s, db, in := unmigrated(t)
		f := in.add(fault{op: opExec, match: `CREATE TABLE IF NOT EXISTS "p_runs"`, times: 2, err: errBusy})
		must(t, s.Migrate(ctx))
		if in.hits(f) != 2 || len(schemaRows(t, db)) != 1 {
			t.Fatalf("hits %d", in.hits(f))
		}
	})
	t.Run("contention on the version read clears", func(t *testing.T) {
		// What a second process sees while the first switches a fresh
		// SQLite file to WAL.
		s, db, in := unmigrated(t)
		f := in.add(fault{op: opQuery, match: `MAX("version")`, times: 2, err: errBusy})
		must(t, s.Migrate(ctx))
		if in.hits(f) != 2 || len(schemaRows(t, db)) != 1 {
			t.Fatalf("hits %d", in.hits(f))
		}
	})
	t.Run("contention gives up", func(t *testing.T) {
		s, _, in := unmigrated(t, sqlrecorder.WithTxAttempts(2))
		f := in.add(fault{op: opExec, match: `CREATE TABLE IF NOT EXISTS "p_runs"`, err: errBusy})
		if err := s.Migrate(ctx); !errors.Is(err, errBusy) || in.hits(f) != 2 {
			t.Fatalf("err = %v, hits %d", err, in.hits(f))
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		c, cancel := context.WithCancel(ctx)
		defer cancel()
		s, _, in := unmigrated(t, sqlrecorder.WithRetryable(func(err error) bool {
			if errors.Is(err, errBusy) {
				cancel()
				return true
			}
			return false
		}))
		f := in.add(fault{op: opExec, match: `CREATE TABLE IF NOT EXISTS "p_runs"`, err: errBusy})
		if err := s.Migrate(c); !errors.Is(err, errBusy) || in.hits(f) != 1 {
			t.Fatalf("err = %v, hits %d", err, in.hits(f))
		}
	})
}

// prunePlan is the retention of the prune tests: runs of every workflow
// are deleted an hour after they finish, except workflow "keep", whose runs
// stay and lose their steps after an hour. Two runs per batch, so a prune
// of four runs takes two batches.
func prunePlan(archive func(context.Context, []recorder.RunRecord) error) recorder.Retention {
	return recorder.Retention{
		RetentionRule: recorder.RetentionRule{Runs: time.Hour},
		PerWorkflow:   map[string]recorder.RetentionRule{"keep": {Steps: time.Hour}},
		Batch:         2,
		Archive:       archive,
	}
}

// seedPrune writes three finished runs of "w" with steps, events and
// labels, a finished run known only by its ScheduledID, a finished "keep"
// run with a step, and an unfinished run with a step. It returns the clock,
// moved two hours past the finishes.
func seedPrune(t *testing.T, s *sqlrecorder.Store, clk *clock.Fake) {
	t.Helper()
	ctx := context.Background()
	full := func(id, wf string, finish bool) {
		ref := rid(id)
		must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: wf, Labels: map[string]string{"id": id}, Status: workflow.StatusRunning, EnqueuedAt: t0, At: t0}))
		must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Step: "s", Attempt: 1, At: t0}))
		must(t, s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: "s", Attempt: 1, Message: "m", At: t0}))
		if finish {
			must(t, s.FinishRun(ctx, recorder.Finish{Ref: ref, Status: workflow.StatusSucceeded, FinishedAt: t0, At: t0}))
		}
	}
	full("old1", "w", true)
	full("old2", "w", true)
	full("old3", "w", true)
	full("kept", "keep", true)
	full("live", "w", false)
	sched := workflow.RunRef{ScheduledID: "sch9"}
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: sched, Workflow: "w", Labels: map[string]string{"id": "sch9"}, Status: workflow.StatusScheduled, EnqueuedAt: t0, At: t0}))
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: sched, Status: workflow.StatusCancelled, FinishedAt: t0, At: t0}))
	clk.Advance(2 * time.Hour)
}

// TestPrune: stage two archives and deletes every expired run, in batches,
// a ScheduledID-only run included; stage one clears the steps of the
// per-workflow rule's runs and keeps their rows; unfinished runs stay.
func TestPrune(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(t0)
	var archived [][]recorder.RunRecord
	s, db, _ := newStore(t, sqlrecorder.WithClock(clk), sqlrecorder.WithRetention(prunePlan(func(_ context.Context, rs []recorder.RunRecord) error {
		archived = append(archived, rs)
		return nil
	})))
	seedPrune(t, s, clk)
	res, err := s.Prune(ctx)
	must(t, err)
	if res.RunsDeleted != 4 || res.Archived != 4 || res.StepsCleared != 1 {
		t.Fatalf("result = %+v", res)
	}
	if len(archived) != 2 || len(archived[0]) != 2 || len(archived[1]) != 2 {
		t.Fatalf("archive batches = %d", len(archived))
	}
	var keys []string
	for _, b := range archived {
		for _, r := range b {
			keys = append(keys, r.Key())
			if r.Ref.RunID != "" && (len(r.Steps) != 1 || len(r.Steps[0].Events) != 1) {
				t.Fatalf("archived %s without its steps: %+v", r.Key(), r.Steps)
			}
		}
	}
	slices.Sort(keys)
	if strings.Join(keys, ",") != "r:old1,r:old2,r:old3,s:sch9" {
		t.Fatalf("archived = %v", keys)
	}
	if n := count(t, db, "p_runs"); n != 2 {
		t.Fatalf("runs left = %d", n)
	}
	if n := count(t, db, "p_labels"); n != 2 {
		t.Fatalf("label rows left = %d", n)
	}
	kept, err := s.Run(ctx, rid("kept"))
	must(t, err)
	if kept.Steps != nil {
		t.Fatalf("kept run still has steps: %+v", kept.Steps)
	}
	live, err := s.Run(ctx, rid("live"))
	must(t, err)
	if len(live.Steps) != 1 || len(live.Steps[0].Events) != 1 {
		t.Fatalf("live run lost steps: %+v", live.Steps)
	}
	// A second prune finds nothing.
	if res, err := s.Prune(ctx); err != nil || res != (recorder.PruneResult{}) {
		t.Fatalf("second prune = %+v, %v", res, err)
	}
	// No retention: Prune does nothing.
	s0, _, in := newStore(t)
	if res, err := s0.Prune(ctx); err != nil || res != (recorder.PruneResult{}) || len(in.statements()) != 0 {
		t.Fatalf("prune without retention = %+v, %v, ran %q", res, err, in.statements())
	}
}

// TestPruneFailures fails each statement of a prune and checks the error
// names the stage, wraps the cause, and the failed batch is untouched.
func TestPruneFailures(t *testing.T) {
	ctx := context.Background()
	const (
		pick    = `SELECT r."run_key", r."run_id" FROM`
		readRun = `ORDER BY r."run_key" ASC LIMIT 1`
	)
	stepsOnly := recorder.Retention{RetentionRule: recorder.RetentionRule{Steps: time.Hour}}
	cases := []struct {
		name string
		ret  recorder.Retention
		f    fault
		msg  string
		is   bool
	}{
		{"pick runs", prunePlan(nil), fault{op: opQuery, match: pick}, "prune scan", true},
		{"pick row read", prunePlan(nil), fault{op: opNext, match: pick}, "injected", true},
		{"pick scan", prunePlan(nil), fault{op: opCorrupt, match: pick, col: 0, val: nil}, "converting NULL", false},
		{"archive read", prunePlan(func(context.Context, []recorder.RunRecord) error { return nil }), fault{op: opQuery, match: readRun}, "read run", true},
		{"delete events", prunePlan(nil), fault{op: opExec, match: `DELETE FROM "p_events"`}, "prune runs", true},
		{"delete steps", prunePlan(nil), fault{op: opExec, match: `DELETE FROM "p_steps"`}, "prune runs", true},
		{"delete labels", prunePlan(nil), fault{op: opExec, match: `DELETE FROM "p_labels"`}, "prune runs", true},
		{"delete run", prunePlan(nil), fault{op: opExec, match: `DELETE FROM "p_runs"`}, "prune runs", true},
		{"stage one pick", stepsOnly, fault{op: opQuery, match: pick}, "prune scan", true},
		{"stage one delete", stepsOnly, fault{op: opExec, match: `DELETE FROM "p_steps"`}, "prune steps", true},
		{"stage one begin", stepsOnly, fault{op: opBegin}, "prune steps", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clk := clock.NewFake(t0)
			s, db, in := newStore(t, sqlrecorder.WithClock(clk), sqlrecorder.WithRetention(c.ret))
			seedPrune(t, s, clk)
			before := dump(t, db, prefix)
			c.f.err = errBoom
			f := in.add(c.f)
			res, err := s.Prune(ctx)
			if err == nil || !strings.Contains(err.Error(), c.msg) || (c.is && !errors.Is(err, errBoom)) {
				t.Fatalf("err = %v, want %q", err, c.msg)
			}
			if in.hits(f) != 1 || res != (recorder.PruneResult{}) {
				t.Fatalf("hits %d, result %+v", in.hits(f), res)
			}
			in.reset()
			if after := dump(t, db, prefix); after != before {
				t.Fatalf("a failed prune changed the database:\nbefore %s\nafter %s", before, after)
			}
		})
	}
}
