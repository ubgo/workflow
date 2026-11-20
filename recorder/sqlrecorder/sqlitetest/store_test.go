package sqlitetest

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/recordertest"
	"github.com/ubgo/workflow/recorder/sqlrecorder"
	"github.com/ubgo/workflow/workflowtest"
)

// prefix is the table prefix of every store here, so statement fragments
// read `"p_runs"` and so on.
const prefix = "p_"

var t0 = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// errBoom is the injected failure; it is not a contention error, so a write
// that hits it fails at once.
var errBoom = errors.New("injected failure")

// errBusy reads as SQLite contention to DefaultRetryable.
var errBusy = errors.New("database is locked (5) (SQLITE_BUSY)")

// newStore opens a database through the fault wrapper and a migrated store
// on it.
func newStore(t *testing.T, opts ...sqlrecorder.Option) (*sqlrecorder.Store, *sql.DB, *injector) {
	t.Helper()
	db, in := openDB(t)
	s, err := sqlrecorder.New(db, sqlrecorder.SQLite, append([]sqlrecorder.Option{sqlrecorder.WithTablePrefix(prefix)}, opts...)...)
	if err != nil {
		t.Fatalf("sqlrecorder.New: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	in.reset()
	return s, db, in
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func rid(id string) workflow.RunRef { return workflow.RunRef{RunID: workflow.RunID(id)} }

// TestConformance holds the store on SQLite to the recorder.Store
// contract, through the fault wrapper with no rule set.
func TestConformance(t *testing.T) {
	recordertest.RunStore(t, func(t *testing.T, cfg recordertest.StoreConfig) recorder.Store {
		s, _, _ := newStore(t, sqlrecorder.WithRetention(cfg.Retention), sqlrecorder.WithClock(cfg.Clock))
		return s
	})
}

// TestRecorderOnMemoryDriver runs the recorder driver suite on the memory
// driver, recording into the SQLite store.
func TestRecorderOnMemoryDriver(t *testing.T) {
	recordertest.RunDriver(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(t0)
		return workflowtest.Instance{Client: memory.New(memory.WithClock(clk)), Clock: clk}
	}}, recordertest.WithStore(func(t *testing.T, cfg recordertest.StoreConfig) recorder.Store {
		s, _, _ := newStore(t, sqlrecorder.WithClock(cfg.Clock))
		return s
	}))
}

// TestRowsWritten reads the tables back after each kind of write: one run
// row with its labels in their own rows, one row per step attempt, one row
// per event with a growing Seq, and times as UTC nanoseconds.
func TestRowsWritten(t *testing.T) {
	ctx := context.Background()
	s, db, _ := newStore(t)
	ref := rid("r1")
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "w", Labels: map[string]string{"b": "2", "a": "1"}, Status: workflow.StatusQueued, EnqueuedAt: t0, At: t0}))
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: ref, Step: "s", Attempt: 1, Input: []byte("in"), InputSize: 2, At: t0.Add(time.Second)}))
	for i := range 3 {
		must(t, s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: "s", Attempt: 1, Message: "m", Level: 4, Attrs: []workflow.EventAttr{{Key: "i", Value: string(rune('0' + i))}}, At: t0.Add(2 * time.Second)}))
	}
	must(t, s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "s", Attempt: 1, Status: recorder.StepSucceeded, Output: []byte("out"), OutputSize: 3, At: t0.Add(3 * time.Second)}))
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: ref, Status: workflow.StatusSucceeded, FinishedAt: t0.Add(4 * time.Second), At: t0.Add(4 * time.Second)}))

	rows, err := db.Query(`SELECT "k", "v" FROM "p_labels" WHERE "run_key" = ? ORDER BY "k"`, "r:r1")
	must(t, err)
	var labels []string
	for rows.Next() {
		var k, v string
		must(t, rows.Scan(&k, &v))
		labels = append(labels, k+"="+v)
	}
	must(t, rows.Close())
	if strings.Join(labels, ",") != "a=1,b=2" {
		t.Fatalf("label rows = %v (run_key r:r1)", labels)
	}
	var status string
	var enq, fin int64
	must(t, db.QueryRow(`SELECT "status", "enqueued_at", "finished_at" FROM "p_runs" WHERE "run_id" = ?`, "r1").Scan(&status, &enq, &fin))
	if status != "succeeded" || enq != t0.UnixNano() || fin != t0.Add(4*time.Second).UnixNano() {
		t.Fatalf("run row = %s %d %d", status, enq, fin)
	}
	var seqs []int
	erows, err := db.Query(`SELECT "seq" FROM "p_events" WHERE "run_id" = ? ORDER BY "seq"`, "r1")
	must(t, err)
	for erows.Next() {
		var n int
		must(t, erows.Scan(&n))
		seqs = append(seqs, n)
	}
	must(t, erows.Close())
	if len(seqs) != 3 || seqs[0] != 1 || seqs[2] != 3 {
		t.Fatalf("event seqs = %v", seqs)
	}
	if n := count(t, db, "p_steps"); n != 1 {
		t.Fatalf("step rows = %d", n)
	}
	// Labels a later write fills on a held run get their rows; labels the
	// run already has are never overwritten.
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("r2"), Workflow: "w", At: t0}))
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("r2"), Labels: map[string]string{"c": "3"}, At: t0.Add(5 * time.Second)}))
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Labels: map[string]string{"a": "changed"}, At: t0.Add(5 * time.Second)}))
	r2, err := s.Run(ctx, rid("r2"))
	must(t, err)
	if r2.Labels["c"] != "3" || count(t, db, "p_labels") != 3 {
		t.Fatalf("labels after fill = %v, rows %d", r2.Labels, count(t, db, "p_labels"))
	}
	r, err := s.Run(ctx, ref)
	must(t, err)
	if r.Labels["a"] != "1" {
		t.Fatalf("a held label was overwritten: %v", r.Labels)
	}
	if len(r.Steps) != 1 || len(r.Steps[0].Events) != 3 || r.Steps[0].Duration() != 2*time.Second {
		t.Fatalf("run read back = %+v", r.Steps)
	}
}

// seedWrites is the state every write-failure case starts from: a run with
// a label, a started step and an event, a delayed run known by its
// ScheduledID, and a run without labels.
func seedWrites(t *testing.T, s *sqlrecorder.Store) {
	t.Helper()
	ctx := context.Background()
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("r1"), Workflow: "w", Labels: map[string]string{"team": "a"}, Status: workflow.StatusRunning, EnqueuedAt: t0, At: t0}))
	must(t, s.StartStep(ctx, recorder.StepStart{Ref: rid("r1"), Step: "s", Attempt: 1, At: t0}))
	must(t, s.AddEvent(ctx, recorder.EventWrite{Ref: rid("r1"), Step: "s", Attempt: 1, Message: "m", At: t0}))
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: workflow.RunRef{ScheduledID: "sch1"}, Workflow: "w", Labels: map[string]string{"x": "y"}, Status: workflow.StatusScheduled, EnqueuedAt: t0, At: t0}))
	must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("r3"), Workflow: "w", Status: workflow.StatusQueued, EnqueuedAt: t0, At: t0}))
}

// TestWriteFailuresLeaveNoTrace fails each statement of each write, and the
// begin and commit of its transaction, and proves the write returns the
// failure and the database is as it was: every write is one transaction.
func TestWriteFailuresLeaveNoTrace(t *testing.T) {
	ctx := context.Background()
	at := t0.Add(time.Minute)
	upsertNew := func(s *sqlrecorder.Store) error {
		return s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("r2"), Workflow: "w", Labels: map[string]string{"k": "v"}, At: at})
	}
	upsertLabels := func(s *sqlrecorder.Store) error {
		return s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("r3"), Labels: map[string]string{"new": "1"}, At: at})
	}
	join := func(s *sqlrecorder.Store) error {
		return s.UpsertRun(ctx, recorder.RunWrite{Ref: workflow.RunRef{RunID: "r9", ScheduledID: "sch1"}, At: at})
	}
	joinTwo := func(s *sqlrecorder.Store) error {
		return s.UpsertRun(ctx, recorder.RunWrite{Ref: workflow.RunRef{RunID: "r1", ScheduledID: "sch1"}, At: at})
	}
	startNew := func(s *sqlrecorder.Store) error {
		return s.StartStep(ctx, recorder.StepStart{Ref: rid("r1"), Step: "s", Attempt: 2, At: at})
	}
	startAgain := func(s *sqlrecorder.Store) error {
		return s.StartStep(ctx, recorder.StepStart{Ref: rid("r1"), Step: "s", Attempt: 1, At: at})
	}
	endHeld := func(s *sqlrecorder.Store) error {
		return s.EndStep(ctx, recorder.StepEnd{Ref: rid("r1"), Step: "s", Attempt: 1, Status: recorder.StepFailed, Error: "x", At: at})
	}
	endUnstarted := func(s *sqlrecorder.Store) error {
		return s.EndStep(ctx, recorder.StepEnd{Ref: rid("r1"), Step: "t", Attempt: 1, Status: recorder.StepSucceeded, At: at})
	}
	event := func(s *sqlrecorder.Store) error {
		return s.AddEvent(ctx, recorder.EventWrite{Ref: rid("r1"), Step: "s", Attempt: 1, Message: "m2", At: at})
	}
	eventEarly := func(s *sqlrecorder.Store) error {
		return s.AddEvent(ctx, recorder.EventWrite{Ref: rid("r1"), Step: "u", Attempt: 1, Message: "early", At: at})
	}
	finish := func(s *sqlrecorder.Store) error {
		return s.FinishRun(ctx, recorder.Finish{Ref: rid("r1"), Status: workflow.StatusSucceeded, FinishedAt: at, At: at})
	}
	outcome := func(s *sqlrecorder.Store) error {
		return s.SetOutcome(ctx, recorder.OutcomeWrite{Ref: rid("r1"), Outcome: workflow.OutcomeSkipped, At: at})
	}
	check := func(s *sqlrecorder.Store) error {
		return s.MarkChecked(ctx, recorder.Check{Ref: rid("r1"), Stale: true, At: at})
	}

	const (
		loadRuns  = `FROM "p_runs" r WHERE`
		loadStep  = `FROM "p_steps" WHERE "run_id" = ? AND "step" = ?`
		lockNoop  = `WHERE 1 = 0`
		updRun    = `UPDATE "p_runs" SET`
		insRun    = `INSERT INTO "p_runs"`
		delRun    = `DELETE FROM "p_runs"`
		insLabels = `INSERT INTO "p_labels"`
		delLabels = `DELETE FROM "p_labels"`
		insStep   = `INSERT INTO "p_steps"`
		updStep   = `UPDATE "p_steps"`
		maxSeq    = `MAX("seq")`
		insEvent  = `INSERT INTO "p_events"`
		maxStart  = `MAX("started_at")`
	)
	cases := []struct {
		name  string
		write func(*sqlrecorder.Store) error
		f     fault
		// msg, when set, is a fragment of the error (a read or decode
		// failure has no sentinel to match); else errors.Is(err, errBoom).
		msg string
	}{
		{"begin", upsertNew, fault{op: opBegin}, ""},
		{"commit", upsertNew, fault{op: opCommit}, ""},
		{"sqlite write lock", upsertNew, fault{op: opExec, match: lockNoop}, ""},
		{"load query", upsertNew, fault{op: opQuery, match: loadRuns}, ""},
		{"load row read", upsertNew, fault{op: opNext, match: loadRuns}, ""},
		{"load close", upsertNew, fault{op: opClose, match: loadRuns}, ""},
		{"load scan", upsertLabels, fault{op: opCorrupt, match: loadRuns, col: 11, val: "not a number"}, "outcome_set"},
		{"load labels json", upsertLabels, fault{op: opCorrupt, match: loadRuns, col: 14, val: "{"}, "labels"},
		{"load errors json", upsertLabels, fault{op: opCorrupt, match: loadRuns, col: 21, val: "{"}, "errors"},
		{"load attempted_by json", upsertLabels, fault{op: opCorrupt, match: loadRuns, col: 22, val: "{"}, "attempted_by"},
		{"insert run", upsertNew, fault{op: opExec, match: insRun}, ""},
		{"insert label", upsertNew, fault{op: opExec, match: insLabels}, ""},
		{"update run", outcome, fault{op: opExec, match: updRun}, ""},
		{"update run on check", check, fault{op: opExec, match: updRun}, ""},
		{"relabel delete", upsertLabels, fault{op: opExec, match: delLabels}, ""},
		{"relabel insert", upsertLabels, fault{op: opExec, match: insLabels}, ""},
		{"join delete old row", join, fault{op: opExec, match: delRun}, ""},
		{"join delete old labels", join, fault{op: opExec, match: delLabels}, ""},
		{"join insert", join, fault{op: opExec, match: insRun}, ""},
		{"join two rows, second delete", joinTwo, fault{op: opExec, match: delRun, skip: 1}, ""},
		{"start load step", startNew, fault{op: opQuery, match: loadStep}, ""},
		{"start insert step", startNew, fault{op: opExec, match: insStep}, ""},
		{"restart update step", startAgain, fault{op: opExec, match: updStep}, ""},
		{"end load step", endHeld, fault{op: opQuery, match: loadStep}, ""},
		{"end update step", endHeld, fault{op: opExec, match: updStep}, ""},
		{"end insert step", endUnstarted, fault{op: opExec, match: insStep}, ""},
		{"event load step", event, fault{op: opQuery, match: loadStep}, ""},
		{"event max seq", event, fault{op: opQuery, match: maxSeq}, ""},
		{"event max seq scan", event, fault{op: opCorrupt, match: maxSeq, col: 0, val: "x"}, "converting"},
		{"event insert", event, fault{op: opExec, match: insEvent}, ""},
		{"event placeholder step", eventEarly, fault{op: opExec, match: insStep}, ""},
		{"finish latest start", finish, fault{op: opQuery, match: maxStart}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, db, in := newStore(t)
			seedWrites(t, s)
			before := dump(t, db, prefix)
			c.f.err = errBoom
			f := in.add(c.f)
			err := c.write(s)
			switch {
			case c.msg != "":
				if err == nil || !strings.Contains(err.Error(), c.msg) {
					t.Fatalf("err = %v, want one naming %q", err, c.msg)
				}
			case !errors.Is(err, errBoom):
				t.Fatalf("err = %v, want the injected failure", err)
			}
			if in.hits(f) != 1 {
				t.Fatalf("fault fired %d times, want 1 (a non-contention error is not retried)", in.hits(f))
			}
			in.reset()
			if after := dump(t, db, prefix); after != before {
				t.Fatalf("a failed write changed the database:\nbefore %s\nafter %s", before, after)
			}
			// Without the fault the same write succeeds.
			if err := c.write(s); err != nil {
				t.Fatalf("the write fails without a fault: %v", err)
			}
		})
	}
}

// TestContentionRetry: a contention error retries the whole transaction
// and the write lands once; one that never clears gives up after the
// configured attempts with the cause wrapped; a non-contention error and a
// cancelled context are not retried.
func TestContentionRetry(t *testing.T) {
	ctx := context.Background()
	write := func(s *sqlrecorder.Store, c context.Context) error {
		return s.UpsertRun(c, recorder.RunWrite{Ref: rid("r1"), Workflow: "w", Labels: map[string]string{"k": "v"}, At: t0})
	}
	for _, o := range []op{opBegin, opExec, opCommit} {
		t.Run("clears", func(t *testing.T) {
			s, db, in := newStore(t)
			match := ""
			if o == opExec {
				match = `INSERT INTO "p_labels"`
			}
			f := in.add(fault{op: o, match: match, times: 2, err: errBusy})
			must(t, write(s, ctx))
			if in.hits(f) != 2 || count(t, db, "p_runs") != 1 || count(t, db, "p_labels") != 1 {
				t.Fatalf("op %d: hits %d, runs %d, labels %d", o, in.hits(f), count(t, db, "p_runs"), count(t, db, "p_labels"))
			}
		})
	}
	t.Run("gives up", func(t *testing.T) {
		s, db, in := newStore(t, sqlrecorder.WithTxAttempts(3))
		f := in.add(fault{op: opExec, match: `INSERT INTO "p_runs"`, err: errBusy})
		err := write(s, ctx)
		if !errors.Is(err, errBusy) || !strings.Contains(err.Error(), "gave up after 3 attempts") || in.hits(f) != 3 {
			t.Fatalf("err = %v, hits %d", err, in.hits(f))
		}
		if count(t, db, "p_runs") != 0 {
			t.Fatal("a given-up write left a row")
		}
	})
	t.Run("custom retryable", func(t *testing.T) {
		odd := errors.New("engine says: try again")
		s, db, in := newStore(t, sqlrecorder.WithRetryable(func(err error) bool { return errors.Is(err, odd) }))
		f := in.add(fault{op: opExec, match: `INSERT INTO "p_runs"`, times: 1, err: odd})
		must(t, write(s, ctx))
		if in.hits(f) != 1 || count(t, db, "p_runs") != 1 {
			t.Fatalf("hits %d, runs %d", in.hits(f), count(t, db, "p_runs"))
		}
		in.reset()
		g := in.add(fault{op: opExec, match: `INSERT INTO "p_runs"`, err: errBusy})
		if err := s.UpsertRun(ctx, recorder.RunWrite{Ref: rid("r2"), At: t0}); !errors.Is(err, errBusy) || in.hits(g) != 1 {
			t.Fatalf("a driver error the custom test refuses was retried: %v, hits %d", err, in.hits(g))
		}
	})
	t.Run("cancelled between tries", func(t *testing.T) {
		c, cancel := context.WithCancel(ctx)
		defer cancel()
		s, db, in := newStore(t, sqlrecorder.WithRetryable(func(err error) bool {
			if errors.Is(err, errBusy) {
				cancel()
				return true
			}
			return false
		}))
		f := in.add(fault{op: opExec, match: `INSERT INTO "p_runs"`, err: errBusy})
		if err := write(s, c); !errors.Is(err, errBusy) || in.hits(f) != 1 {
			t.Fatalf("err = %v, hits %d", err, in.hits(f))
		}
		if count(t, db, "p_runs") != 0 {
			t.Fatal("row written")
		}
	})
}

// TestReadFailures fails each statement of each read and checks the error
// says which read failed and wraps the cause.
func TestReadFailures(t *testing.T) {
	ctx := context.Background()
	const (
		list    = `ORDER BY r."enqueued_at" DESC`
		one     = `ORDER BY r."run_key" ASC LIMIT 1`
		steps   = `FROM "p_steps" WHERE "run_id" = ?`
		events  = `FROM "p_events" WHERE`
		pending = `ORDER BY r."checked_at" ASC`
	)
	records := func(s *sqlrecorder.Store) error {
		_, err := s.Records(ctx, recorder.Query{})
		return err
	}
	runs := func(s *sqlrecorder.Store) error {
		_, err := s.Runs(ctx, workflow.RunQuery{})
		return err
	}
	run := func(s *sqlrecorder.Store) error {
		_, err := s.Run(ctx, rid("r1"))
		return err
	}
	pend := func(s *sqlrecorder.Store) error {
		_, err := s.Pending(ctx, 10)
		return err
	}
	cases := []struct {
		name string
		read func(*sqlrecorder.Store) error
		f    fault
		msg  string // a fragment of the error; the cause must also be errBoom unless sentinel is false
		is   bool
	}{
		{"list query", records, fault{op: opQuery, match: list}, "list runs", true},
		{"list query through Runs", runs, fault{op: opQuery, match: list}, "list runs", true},
		{"list row read", records, fault{op: opNext, match: list}, "injected", true},
		{"list scan", records, fault{op: opCorrupt, match: list, col: 16, val: "x"}, "payload_size", false},
		{"run query", run, fault{op: opQuery, match: one}, "read run", true},
		{"run labels json", run, fault{op: opCorrupt, match: one, col: 14, val: "[1"}, "labels", false},
		{"steps query", run, fault{op: opQuery, match: steps}, "read steps", true},
		{"steps scan", run, fault{op: opCorrupt, match: steps, col: 2, val: "x"}, "converting", false},
		{"steps row read", run, fault{op: opNext, match: steps}, "read steps", true},
		{"steps close", run, fault{op: opClose, match: steps}, "read steps", true},
		{"events query", run, fault{op: opQuery, match: events}, "read events", true},
		{"events scan", run, fault{op: opCorrupt, match: events, col: 1, val: "x"}, "converting", false},
		{"events attrs json", run, fault{op: opCorrupt, match: events, col: 5, val: "{"}, "event attrs", false},
		{"events row read", run, fault{op: opNext, match: events}, "injected", true},
		{"pending query", pend, fault{op: opQuery, match: pending}, "pending", true},
		{"pending scan", pend, fault{op: opCorrupt, match: pending, col: 18, val: "x"}, "output_size", false},
		{"pending row read", pend, fault{op: opNext, match: pending}, "injected", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _, in := newStore(t)
			seedWrites(t, s)
			c.f.err = errBoom
			f := in.add(c.f)
			err := c.read(s)
			if err == nil || !strings.Contains(err.Error(), c.msg) || (c.is && !errors.Is(err, errBoom)) {
				t.Fatalf("err = %v, want %q (wrapping the cause: %v)", err, c.msg, c.is)
			}
			if in.hits(f) != 1 {
				t.Fatalf("fault fired %d times", in.hits(f))
			}
		})
	}
}

// TestRunReadsJoinedAndScheduledRuns: a run known only by its ScheduledID
// reads without steps; a lookup by both IDs prefers the RunID row.
func TestRunReadsJoinedAndScheduledRuns(t *testing.T) {
	ctx := context.Background()
	s, _, in := newStore(t)
	seedWrites(t, s)
	in.reset()
	r, err := s.Run(ctx, workflow.RunRef{ScheduledID: "sch1"})
	must(t, err)
	if r.Ref.ScheduledID != "sch1" || r.Steps != nil || r.Labels["x"] != "y" {
		t.Fatalf("scheduled run = %+v", r)
	}
	for _, st := range in.statements() {
		if strings.Contains(st, `"p_steps"`) {
			t.Fatalf("a run without a RunID read steps: %s", st)
		}
	}
	r, err = s.Run(ctx, workflow.RunRef{RunID: "r1", ScheduledID: "sch1"})
	must(t, err)
	if r.Ref.RunID != "r1" || len(r.Steps) != 1 {
		t.Fatalf("both IDs read %+v", r)
	}
	if _, err := s.Run(ctx, rid("nope")); !errors.Is(err, workflow.ErrNotFound) || !strings.Contains(err.Error(), "r:nope") {
		t.Fatalf("missing run: %v", err)
	}
}

// TestPending orders by last check then key, skips finished runs and
// refuses a non-positive limit.
func TestPending(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newStore(t)
	for _, id := range []string{"c", "a", "b", "done"} {
		must(t, s.UpsertRun(ctx, recorder.RunWrite{Ref: rid(id), Workflow: "w", Status: workflow.StatusRunning, At: t0}))
	}
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: rid("done"), Status: workflow.StatusFailed, FinishedAt: t0, At: t0}))
	must(t, s.MarkChecked(ctx, recorder.Check{Ref: rid("a"), At: t0.Add(time.Minute)}))
	got, err := s.Pending(ctx, 10)
	must(t, err)
	var ids []string
	for _, r := range got {
		ids = append(ids, string(r.Ref.RunID))
	}
	if strings.Join(ids, ",") != "b,c,a" {
		t.Fatalf("pending order = %v", ids)
	}
	if got, err := s.Pending(ctx, 1); err != nil || len(got) != 1 {
		t.Fatalf("limit 1: %d %v", len(got), err)
	}
	for _, n := range []int{0, -1} {
		if _, err := s.Pending(ctx, n); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("Pending(%d): %v", n, err)
		}
	}
}

// TestFinishJoinsDelayedRun: the finish of a delayed run kept under its
// ScheduledID, naming its RunID too, leaves one row under the RunID with
// both IDs and the end recorded.
func TestFinishJoinsDelayedRun(t *testing.T) {
	ctx := context.Background()
	s, db, _ := newStore(t)
	seedWrites(t, s)
	fin := t0.Add(time.Hour)
	must(t, s.FinishRun(ctx, recorder.Finish{Ref: workflow.RunRef{RunID: "r9", ScheduledID: "sch1"}, Status: workflow.StatusSucceeded, FinishedAt: fin, At: fin}))
	r, err := s.Run(ctx, rid("r9"))
	must(t, err)
	if r.Ref.ScheduledID != "sch1" || r.Status != workflow.StatusSucceeded || !r.FinishedAt.Equal(fin) || r.Labels["x"] != "y" {
		t.Fatalf("joined run = %+v", r)
	}
	var n int
	must(t, db.QueryRow(`SELECT COUNT(*) FROM "p_runs" WHERE "scheduled_id" = ?`, "sch1").Scan(&n))
	if n != 1 {
		t.Fatalf("rows with the ScheduledID = %d", n)
	}
	var key string
	must(t, db.QueryRow(`SELECT "run_key" FROM "p_labels" WHERE "k" = ?`, "x").Scan(&key))
	if key != "r:r9" {
		t.Fatalf("label row key = %q", key)
	}
}
