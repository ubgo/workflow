package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/sqlb/ddl"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore/outbox"
	"github.com/ubgo/workflow/workflowtest"
)

// frozenChecksums pins the ledger checksum of every released outbox
// migration with the default prefix. A released migration is never
// edited; append a line for a new one, never change one.
var frozenChecksums = map[string]string{
	"sqlite 001_outbox":   "b33f3f69ad5c928debc638b3db7c685bca4ff4943e10cabcaebf81481733090e",
	"postgres 001_outbox": "9b2e964e1b7506f7285eca5f52f48a657690f1aa8d712c7ad5eabde3442b622e",
	"mysql 001_outbox":    "7125189956ad517226b0d55ad51bb2627596f7bca7760b1bbfb50afa2664b1be",
}

func TestMigrationScriptsAreFrozen(t *testing.T) {
	seen := 0
	for _, d := range []sqlb.Dialect{sqlb.SQLiteDialect{}, sqlb.PostgresDialect{}, sqlb.MySQLDialect{}} {
		scripts, err := outbox.MigrationScripts(d)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range scripts {
			var sc ddl.Script
			for _, st := range m.Statements {
				sc = append(sc, ddl.Statement{SQL: st, Transactional: true})
			}
			key := sqlb.DialectName(d) + " " + m.Name
			if m.Owner != outbox.MigrationOwner {
				t.Errorf("%s: owner %q, want %q", key, m.Owner, outbox.MigrationOwner)
			}
			want, ok := frozenChecksums[key]
			if !ok {
				t.Errorf("%s has no pinned checksum; add %q", key, migrate.Checksum(sc))
				continue
			}
			seen++
			if got := migrate.Checksum(sc); got != want {
				t.Errorf("%s changed: checksum %s, pinned %s. A released migration is never edited; append a new one", key, got, want)
			}
		}
	}
	if seen != len(frozenChecksums) {
		t.Errorf("checked %d pinned migrations, want %d", seen, len(frozenChecksums))
	}
}

func TestSchemaOption(t *testing.T) {
	scripts, err := outbox.MigrationScripts(sqlb.PostgresDialect{}, outbox.WithSchema("jobs"))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range scripts {
		if m.Owner != outbox.MigrationOwner+"@jobs" {
			t.Errorf("owner %q under WithSchema", m.Owner)
		}
		for _, st := range m.Statements {
			if !strings.Contains(st, `"jobs"."wf_outbox"`) {
				t.Errorf("statement does not name the schema: %s", st)
			}
		}
	}
	for _, d := range []sqlb.Dialect{sqlb.SQLiteDialect{}, sqlb.MySQLDialect{}} {
		if _, err := outbox.MigrationScripts(d, outbox.WithSchema("jobs")); !errors.Is(err, workflow.ErrUnsupported) {
			t.Errorf("WithSchema on %s: %v, want ErrUnsupported", sqlb.DialectName(d), err)
		}
	}
	if _, err := outbox.MigrationScripts(sqlb.PostgresDialect{}, outbox.WithSchema("Jobs;")); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("a schema outside the pattern: %v, want ErrInvalid", err)
	}
}

// TestTableAppliedElsewhere: the outbox works on a table created from
// MigrationScripts with no ledger, each statement run twice.
func TestTableAppliedElsewhere(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			scripts, err := outbox.MigrationScripts(e.dialect, outbox.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range scripts {
				for range 2 {
					for _, st := range m.Statements {
						if _, err := db.Exec(st); err != nil {
							t.Fatalf("%s: %v", m.Name, err)
						}
					}
				}
			}
			ob, err := outbox.New(db, e.dialect, outbox.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			orders := e.dialect.QuoteIdent(prefix + "orders")
			if _, err := db.Exec("CREATE TABLE IF NOT EXISTS " + orders + " (id VARCHAR(64) NOT NULL PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			f := fixture{db: db, ob: ob, orders: orders}
			f.write(t, "o1", workflow.Job{Workflow: "ship"}, true)
			m, runs := target(t)
			relay, err := ob.NewRelay(m)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := relay.Flush(context.Background()); err != nil || n != 1 {
				t.Fatalf("Flush = %d, %v; want 1", n, err)
			}
			waitFor(t, "the run", func() bool { return runs.Load() == 1 })
			if err := ob.Migrate(context.Background()); err != nil {
				t.Fatalf("Migrate over a hand-applied table: %v", err)
			}
		})
	}
}

// noKeyTarget is a started Fake without CapIdempotencyKey, recording jobs.
func noKeyTarget(t *testing.T) *workflowtest.Fake {
	t.Helper()
	f := workflowtest.NewFake(workflowtest.FakeCaps(workflow.CapDelayedStart))
	if err := f.Register(workflow.Workflow{Name: "ship", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return f
}

// TestRelayNeedsIdempotencyKeys: a target without CapIdempotencyKey is
// refused unless AllowDuplicates names the trade-off; with it, the relay
// sends no entry key (the target would refuse one) but keeps the job's own.
func TestRelayNeedsIdempotencyKeys(t *testing.T) {
	e := engines(t)[0]
	f := newFixture(t, e)
	noKey := noKeyTarget(t)
	if _, err := f.ob.NewRelay(noKey); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("NewRelay to a target without keys: %v, want ErrUnsupported", err)
	}
	relay, err := f.ob.NewRelay(noKey, outbox.AllowDuplicates())
	if err != nil {
		t.Fatalf("NewRelay with AllowDuplicates: %v", err)
	}
	f.write(t, "o1", workflow.Job{Workflow: "ship"}, true)
	if n, err := relay.Flush(context.Background()); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v; want 1", n, err)
	}
	jobs := noKey.Jobs()
	if len(jobs) != 1 || jobs[0].IdempotencyKey != "" {
		t.Fatalf("jobs = %+v; want one job with no idempotency key", jobs)
	}

	// A target with keys still gets the entry ID as the key.
	m, _ := target(t)
	rec := &recorder{Client: m}
	keyed, err := f.ob.NewRelay(rec)
	if err != nil {
		t.Fatal(err)
	}
	id := f.write(t, "o2", workflow.Job{Workflow: "ship"}, true)
	if _, err := keyed.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.keys) != 1 || rec.keys[0] != id {
		t.Fatalf("keys sent = %v, want [%s]", rec.keys, id)
	}
}

// TestDeadEntries: an entry that goes dead reaches the OnDead hook once,
// ListDead pages it with its last error, Requeue sends it again, and a
// Requeue or listing cursor that names no dead entry is ErrNotFound.
func TestDeadEntries(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			clk := clock.NewFake(time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC))
			f := newFixture(t, e, outbox.WithClock(clk))
			m, runs := target(t)
			var mu sync.Mutex
			var dead []outbox.DeadEntry
			var causes []error
			relay, err := f.ob.NewRelay(failing{m}, outbox.WithMaxAttempts(1), outbox.WithOnDead(func(_ context.Context, e outbox.DeadEntry, cause error) {
				mu.Lock()
				defer mu.Unlock()
				dead = append(dead, e)
				causes = append(causes, cause)
			}))
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for i := range 3 {
				ids = append(ids, f.write(t, fmt.Sprintf("o%d", i), workflow.Job{Workflow: "ship", Payload: []byte(fmt.Sprintf(`"%d"`, i)),
					Labels: map[string]string{"tenant": "t1"}}, true))
			}
			if _, err := relay.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			if len(dead) != 3 || dead[0].ID != ids[0] || dead[0].Attempts != 1 || dead[0].Job.Labels["tenant"] != "t1" ||
				dead[0].Job.IdempotencyKey != "" || causes[0] == nil || dead[0].LastErr != causes[0].Error() {
				mu.Unlock()
				t.Fatalf("OnDead saw %+v (causes %v); want the 3 entries, in order, with their job and error", dead, causes)
			}
			mu.Unlock()

			page, err := f.ob.ListDead(ctx, outbox.DeadQuery{Limit: 2})
			if err != nil || len(page) != 2 || page[0].ID != ids[0] || page[1].ID != ids[1] || string(page[1].Job.Payload) != `"1"` {
				t.Fatalf("first page = %+v, %v", page, err)
			}
			rest, err := f.ob.ListDead(ctx, outbox.DeadQuery{After: page[1].ID})
			if err != nil || len(rest) != 1 || rest[0].ID != ids[2] {
				t.Fatalf("second page = %+v, %v", rest, err)
			}
			if other, err := f.ob.ListDead(ctx, outbox.DeadQuery{Workflow: "other"}); err != nil || len(other) != 0 {
				t.Fatalf("filtered by another workflow = %+v, %v", other, err)
			}
			if _, err := f.ob.ListDead(ctx, outbox.DeadQuery{After: "nope"}); !errors.Is(err, workflow.ErrNotFound) {
				t.Fatalf("cursor naming no dead entry: %v, want ErrNotFound", err)
			}

			if err := f.ob.Requeue(ctx, ids[0]); err != nil {
				t.Fatal(err)
			}
			if err := f.ob.Requeue(ctx, ids[0]); !errors.Is(err, workflow.ErrNotFound) {
				t.Fatalf("Requeue of an entry no longer dead: %v, want ErrNotFound", err)
			}
			if err := f.ob.Requeue(ctx, "nope"); !errors.Is(err, workflow.ErrNotFound) {
				t.Fatalf("Requeue of an unknown id: %v, want ErrNotFound", err)
			}
			good, err := f.ob.NewRelay(m)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := good.Flush(ctx); err != nil || n != 1 {
				t.Fatalf("Flush after Requeue = %d, %v; want 1", n, err)
			}
			waitFor(t, "the requeued run", func() bool { return runs.Load() == 1 })
			if f.pending(t, "dead") != 2 {
				t.Fatalf("dead = %d after one Requeue, want 2", f.pending(t, "dead"))
			}
		})
	}
}

// TestEntryIDGenerator: a custom generator's ID is the entry ID and the
// key the relay sends; a generator that errors or returns an ID that does
// not fit fails Write and writes nothing.
func TestEntryIDGenerator(t *testing.T) {
	e := engines(t)[0]
	var n atomic.Int64
	f := newFixture(t, e, outbox.WithIDGenerator(func() (string, error) { return fmt.Sprintf("evt-%d", n.Add(1)), nil }))
	if id := f.write(t, "o1", workflow.Job{Workflow: "ship"}, true); id != "evt-1" {
		t.Fatalf("entry id %q, want evt-1", id)
	}
	for name, gen := range map[string]outbox.IDGenerator{
		"error":    func() (string, error) { return "", errors.New("id service down") },
		"too long": func() (string, error) { return strings.Repeat("x", 65), nil },
		"empty":    func() (string, error) { return "", nil },
	} {
		t.Run(name, func(t *testing.T) {
			bad := newFixture(t, e, outbox.WithIDGenerator(gen))
			tx, err := bad.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := bad.ob.Write(context.Background(), tx, workflow.Job{Workflow: "ship"}); err == nil {
				t.Fatal("Write succeeded with a broken generator")
			}
		})
	}
}
