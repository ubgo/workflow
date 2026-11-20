package e2e_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/recordertest"
	"github.com/ubgo/workflow/recorder/sqlrecorder"
)

// dialectOf maps an engine name to the recorder store's dialect.
func dialectOf(name string) sqlrecorder.Dialect {
	switch name {
	case "postgres":
		return sqlrecorder.Postgres
	case "mysql":
		return sqlrecorder.MySQL
	}
	return sqlrecorder.SQLite
}

// newSQLStore builds and migrates a sqlrecorder store under prefix+"rec_".
func newSQLStore(t *testing.T, e engine, db *sql.DB, prefix string, opts ...sqlrecorder.Option) *sqlrecorder.Store {
	t.Helper()
	s, err := sqlrecorder.New(db, dialectOf(e.name), append([]sqlrecorder.Option{sqlrecorder.WithTablePrefix(prefix + "rec_")}, opts...)...)
	if err != nil {
		t.Fatalf("sqlrecorder.New: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return s
}

// storeFactory is a recordertest.NewStore on engine e.
func storeFactory(e engine) recordertest.NewStore {
	return func(t *testing.T, cfg recordertest.StoreConfig) recorder.Store {
		db, prefix := e.open(t)
		return newSQLStore(t, e, db, prefix, sqlrecorder.WithRetention(cfg.Retention), sqlrecorder.WithClock(cfg.Clock))
	}
}

// TestSQLRecorderConformance holds sqlrecorder to the Store contract on
// every engine.
func TestSQLRecorderConformance(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) { recordertest.RunStore(t, storeFactory(e)) })
	}
}

// TestSQLRecorderMigrate: Migrate is idempotent, safe to run concurrently,
// records the version, and refuses a newer schema; two prefixes share a
// database without meeting.
func TestSQLRecorderMigrate(t *testing.T) {
	ctx := context.Background()
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			s, err := sqlrecorder.New(db, dialectOf(e.name), sqlrecorder.WithTablePrefix(prefix+"rec_"))
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, 3)
			for range 3 {
				wg.Add(1)
				go func() { defer wg.Done(); errs <- s.Migrate(ctx) }()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatalf("concurrent Migrate: %v", err)
				}
			}
			if err := s.Migrate(ctx); err != nil {
				t.Fatalf("second Migrate: %v", err)
			}
			q := `"`
			if e.name == "mysql" {
				q = "`"
			}
			var n int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + q + prefix + "rec_schema" + q).Scan(&n); err != nil || n != 1 {
				t.Fatalf("schema rows = %d, %v", n, err)
			}
			if _, err := db.Exec("UPDATE " + q + prefix + "rec_schema" + q + " SET " + q + "version" + q + " = 99"); err != nil {
				t.Fatal(err)
			}
			if err := s.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "newer") {
				t.Fatalf("Migrate on a newer schema: %v", err)
			}
			// A second store on another prefix in the same database.
			other := newSQLStore(t, e, db, prefix+"b")
			if err := other.UpsertRun(ctx, recorder.RunWrite{Ref: workflow.RunRef{RunID: "r1"}, Workflow: "w", At: start}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Run(ctx, workflow.RunRef{RunID: "r1"}); !errors.Is(err, workflow.ErrNotFound) {
				t.Fatalf("prefixes met: %v", err)
			}
		})
	}
}

// TestSQLRecorderBigBodiesAndUnicode stores a large body, binary bytes and
// non-ASCII text intact on every engine.
func TestSQLRecorderBigBodiesAndUnicode(t *testing.T) {
	ctx := context.Background()
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			s := newSQLStore(t, e, db, prefix)
			big := []byte(strings.Repeat("x", 256<<10))
			bin := []byte{0, 1, 2, 0xff, 0xfe}
			ref := workflow.RunRef{RunID: "r-ünï"}
			if err := s.UpsertRun(ctx, recorder.RunWrite{Ref: ref, Workflow: "wf-日本", Labels: map[string]string{"note": "héllo"}, Payload: big, PayloadSize: len(big), At: start}); err != nil {
				t.Fatal(err)
			}
			if err := s.StartStep(ctx, recorder.StepStart{Ref: ref, Step: "s", Attempt: 1, Input: bin, InputSize: len(bin), At: start}); err != nil {
				t.Fatal(err)
			}
			if err := s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Step: "s", Attempt: 1, Message: "✓ done", Attrs: []workflow.EventAttr{{Key: "k", Value: "välue"}}, At: start}); err != nil {
				t.Fatal(err)
			}
			r, err := s.Run(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Payload) != len(big) || r.Workflow != "wf-日本" || r.Labels["note"] != "héllo" || string(r.Steps[0].Input) != string(bin) ||
				r.Steps[0].Events[0].Message != "✓ done" || r.Steps[0].Events[0].Attrs[0].Value != "välue" {
				t.Fatalf("round trip lost data: payload %d wf %q labels %v input %v event %+v", len(r.Payload), r.Workflow, r.Labels, r.Steps[0].Input, r.Steps[0].Events)
			}
			// Case-sensitive identity: R-ÜNÏ is another run.
			if _, err := s.Run(ctx, workflow.RunRef{RunID: "R-ünï"}); !errors.Is(err, workflow.ErrNotFound) {
				t.Fatalf("run IDs compare case-insensitively: %v", err)
			}
		})
	}
}

// TestSQLRecorderPruneBatches prunes more runs than one batch holds.
func TestSQLRecorderPruneBatches(t *testing.T) {
	ctx := context.Background()
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var archived int
			s := newSQLStore(t, e, db, prefix, sqlrecorder.WithRetention(recorder.Retention{
				RetentionRule: recorder.RetentionRule{Runs: time.Hour},
				Archive:       func(_ context.Context, rs []recorder.RunRecord) error { archived += len(rs); return nil },
				Batch:         3,
			}), sqlrecorder.WithClock(clock.NewFake(start.Add(48*time.Hour))))
			for i := range 10 {
				ref := workflow.RunRef{RunID: workflow.RunID("r" + string(rune('a'+i)))}
				if err := s.FinishRun(ctx, recorder.Finish{Ref: ref, Workflow: "w", Status: workflow.StatusSucceeded, FinishedAt: start, At: start}); err != nil {
					t.Fatal(err)
				}
			}
			// A ScheduledID-only run, cancelled before it started.
			if err := s.FinishRun(ctx, recorder.Finish{Ref: workflow.RunRef{ScheduledID: "s1"}, Workflow: "w", Status: workflow.StatusCancelled, FinishedAt: start, At: start}); err != nil {
				t.Fatal(err)
			}
			res, err := s.Prune(ctx)
			if err != nil || res.RunsDeleted != 11 || archived != 11 {
				t.Fatalf("prune = %+v archived %d err %v", res, archived, err)
			}
			p, _ := s.Records(ctx, recorder.Query{})
			if len(p.Runs) != 0 {
				t.Fatalf("%d runs left", len(p.Runs))
			}
		})
	}
}
