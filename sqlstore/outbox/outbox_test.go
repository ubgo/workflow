package outbox_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/sqlb"
	drivermysql "github.com/ubgo/sqlb/contrib/driver-mysql"
	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore/outbox"
)

// The same variables sqlstore's tests read (its Taskfile sets them); without
// them only SQLite runs.
const (
	envPostgresDSN = "WFTEST_PG_DSN"
	envMySQLDSN    = "WFTEST_MYSQL_DSN"
)

type engine struct {
	name    string
	dialect sqlb.Dialect
	open    func(t *testing.T) (*sql.DB, string)
}

func engines(t *testing.T) []engine {
	out := []engine{{name: "sqlite", dialect: sqlb.SQLiteDialect{}, open: func(t *testing.T) (*sql.DB, string) {
		db, err := driversqlite.Open(filepath.Join(t.TempDir(), "ob.db"))
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, "wf_"
	}}}
	if dsn := os.Getenv(envPostgresDSN); dsn != "" {
		out = append(out, engine{"postgres", sqlb.PostgresDialect{}, shared(t, func() (*sql.DB, error) { return driverpgx.Open(dsn) }, `"`)})
	}
	if dsn := os.Getenv(envMySQLDSN); dsn != "" {
		out = append(out, engine{"mysql", sqlb.MySQLDialect{}, shared(t, func() (*sql.DB, error) { return drivermysql.Open(dsn) }, "`")})
	}
	return out
}

// shared opens one pool per run and gives each test its own table prefix,
// dropping its tables after.
func shared(t *testing.T, open func() (*sql.DB, error), quote string) func(*testing.T) (*sql.DB, string) {
	var once sync.Once
	var db *sql.DB
	var openErr error
	return func(tt *testing.T) (*sql.DB, string) {
		once.Do(func() {
			db, openErr = open()
			if openErr == nil {
				t.Cleanup(func() { _ = db.Close() })
			}
		})
		if openErr != nil {
			tt.Fatalf("open database: %v", openErr)
		}
		b := make([]byte, 6)
		_, _ = rand.Read(b)
		prefix := "o" + hex.EncodeToString(b) + "_"
		tt.Cleanup(func() {
			for _, tbl := range []string{"outbox", "orders", "migrations", "migrate_lock"} {
				_, _ = db.Exec("DROP TABLE IF EXISTS " + quote + prefix + tbl + quote)
			}
		})
		return db, prefix
	}
}

// fixture is one migrated outbox with an app table beside it.
type fixture struct {
	db     *sql.DB
	ob     *outbox.Outbox
	orders string
}

func newFixture(t *testing.T, e engine, opts ...outbox.Option) fixture {
	t.Helper()
	db, prefix := e.open(t)
	ob, err := outbox.New(db, e.dialect, append([]outbox.Option{outbox.WithTablePrefix(prefix)}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ob.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := ob.Migrate(context.Background()); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	orders := e.dialect.QuoteIdent(prefix + "orders")
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS " + orders + " (id VARCHAR(64) NOT NULL PRIMARY KEY)"); err != nil {
		t.Fatalf("create app table: %v", err)
	}
	return fixture{db: db, ob: ob, orders: orders}
}

// write commits (or rolls back) one app row plus one outbox entry.
func (f fixture) write(t *testing.T, order string, job workflow.Job, commit bool) string {
	t.Helper()
	ctx := context.Background()
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() }) // a no-op after Commit; frees locks when a check fails first
	if _, err := tx.Exec("INSERT INTO " + f.orders + " (id) VALUES ('" + order + "')"); err != nil {
		t.Fatal(err)
	}
	id, err := f.ob.Write(ctx, tx, job)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if commit {
		err = tx.Commit()
	} else {
		err = tx.Rollback()
	}
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (f fixture) pending(t *testing.T, status string) int {
	t.Helper()
	var n int
	if err := f.db.QueryRow("SELECT COUNT(*) FROM " + f.tableName() + " WHERE status = '" + status + "'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// tableName is the outbox table, found through the app table's prefix.
func (f fixture) tableName() string {
	return f.orders[:len(f.orders)-len("orders")-1] + "outbox" + f.orders[len(f.orders)-1:]
}

// target is a started memory client with one workflow, counting runs.
func target(t *testing.T) (*memory.Client, *atomic.Int64) {
	t.Helper()
	m := memory.New()
	var runs atomic.Int64
	if err := m.Register(workflow.Workflow{Name: "ship", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) {
		runs.Add(1)
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); m.Stop() })
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return m, &runs
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestCommitRelaysRollbackDoesNot is the outbox's point: a committed entry
// reaches the target through a started relay, a rolled-back one never does.
func TestCommitRelaysRollbackDoesNot(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			f := newFixture(t, e)
			m, runs := target(t)
			relay, err := f.ob.NewRelay(m, outbox.WithPollInterval(10*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := relay.Start(ctx); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(relay.Stop)

			f.write(t, "rolled-back", workflow.Job{Workflow: "ship"}, false)
			f.write(t, "committed", workflow.Job{Workflow: "ship", Labels: map[string]string{"tenant": "t1"}}, true)
			relay.Notify()
			waitFor(t, "the committed entry to run", func() bool { return runs.Load() == 1 })
			waitFor(t, "the outbox to empty", func() bool { return f.pending(t, "pending") == 0 })
			time.Sleep(50 * time.Millisecond) // several polls: a leaked entry would run in this time
			if n := runs.Load(); n != 1 {
				t.Fatalf("runs = %d, want 1 (the rolled-back entry must never relay)", n)
			}
		})
	}
}

// recorder wraps a target: it records every RunRef the target returned and
// fails the first n enqueues after the target accepted them, the way a relay
// that crashed before marking an entry sent looks to the target.
type recorder struct {
	workflow.Client
	mu       sync.Mutex
	refs     []workflow.RunRef
	keys     []string
	failNext int
}

func (r *recorder) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	ref, err := r.Client.Enqueue(ctx, job)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs = append(r.refs, ref)
	r.keys = append(r.keys, job.IdempotencyKey)
	if err == nil && r.failNext > 0 {
		r.failNext--
		return ref, errors.New("lost after the target accepted it")
	}
	return ref, err
}

// TestRedeliveryIsIdempotent proves the entry ID travels as the
// idempotency key: an entry the target accepted but the relay could not
// mark is enqueued again with the same key, and the target returns the
// first run instead of starting a second.
func TestRedeliveryIsIdempotent(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			clk := clock.NewFake(time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC))
			f := newFixture(t, e, outbox.WithClock(clk))
			m, _ := target(t)
			rec := &recorder{Client: m, failNext: 1}
			relay, err := f.ob.NewRelay(rec)
			if err != nil {
				t.Fatal(err)
			}
			// RunAt keeps the target's run unfinished, so it still holds the key.
			id := f.write(t, "o1", workflow.Job{Workflow: "ship", RunAt: time.Now().Add(time.Hour)}, true)
			ctx := context.Background()
			if n, err := relay.Flush(ctx); err != nil || n != 0 {
				t.Fatalf("first Flush = %d, %v; want 0 accepted (the mark was lost)", n, err)
			}
			clk.Advance(time.Second) // the default backoff after one failure
			if n, err := relay.Flush(ctx); err != nil || n != 1 {
				t.Fatalf("second Flush = %d, %v; want 1", n, err)
			}
			rec.mu.Lock()
			defer rec.mu.Unlock()
			if len(rec.refs) != 2 || rec.refs[0] != rec.refs[1] {
				t.Fatalf("target refs = %+v, want one run returned twice", rec.refs)
			}
			if rec.keys[0] != id || rec.keys[1] != id {
				t.Fatalf("keys = %v, want the entry ID %s both times", rec.keys, id)
			}
		})
	}
}

// failing is a target that refuses every enqueue.
type failing struct{ workflow.Client }

func (failing) Enqueue(context.Context, workflow.Job) (workflow.RunRef, error) {
	return workflow.RunRef{}, errors.New("target down")
}

// TestBackoffThenDead walks one entry through its retries on a fake clock:
// not retried before its backoff ends, dead after WithMaxAttempts.
func TestBackoffThenDead(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			clk := clock.NewFake(time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC))
			f := newFixture(t, e, outbox.WithClock(clk))
			m, _ := target(t)
			relay, err := f.ob.NewRelay(failing{m}, outbox.WithMaxAttempts(2),
				outbox.WithBackoff(workflow.Backoff{Initial: time.Minute, Factor: 2}))
			if err != nil {
				t.Fatal(err)
			}
			f.write(t, "o1", workflow.Job{Workflow: "ship"}, true)
			ctx := context.Background()
			if _, err := relay.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			clk.Advance(59 * time.Second)
			if _, err := relay.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if f.pending(t, "pending") != 1 || f.pending(t, "dead") != 0 {
				t.Fatal("the entry was retried before its backoff ended")
			}
			clk.Advance(time.Second)
			if _, err := relay.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if f.pending(t, "dead") != 1 {
				t.Fatalf("after 2 failed attempts: pending=%d dead=%d, want dead", f.pending(t, "pending"), f.pending(t, "dead"))
			}
		})
	}
}

// TestRelaysShareTheTable runs several relays at once over one table: every
// entry is enqueued exactly once (no failures, so no redelivery).
func TestRelaysShareTheTable(t *testing.T) {
	const entries, relays = 60, 4
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			f := newFixture(t, e)
			m, runs := target(t)
			rec := &recorder{Client: m}
			for i := range entries {
				f.write(t, "o"+string(rune('A'+i/26))+string(rune('a'+i%26)), workflow.Job{Workflow: "ship"}, true)
			}
			var wg sync.WaitGroup
			for range relays {
				relay, err := f.ob.NewRelay(rec, outbox.WithBatchSize(7))
				if err != nil {
					t.Fatal(err)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, err := relay.Flush(context.Background()); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			waitFor(t, "every run", func() bool { return runs.Load() >= entries })
			rec.mu.Lock()
			defer rec.mu.Unlock()
			seen := map[string]bool{}
			for _, k := range rec.keys {
				if seen[k] {
					t.Fatalf("entry %s enqueued twice", k)
				}
				seen[k] = true
			}
			if len(seen) != entries || f.pending(t, "pending") != 0 {
				t.Fatalf("enqueued %d distinct entries, %d left; want %d, 0", len(seen), f.pending(t, "pending"), entries)
			}
		})
	}
}

func TestRefusals(t *testing.T) {
	e := engines(t)[0]
	f := newFixture(t, e)
	ctx := context.Background()
	if _, err := f.ob.Write(ctx, nil, workflow.Job{Workflow: "ship"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil tx: %v", err)
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := f.ob.Write(ctx, tx, workflow.Job{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("no workflow: %v", err)
	}
	if _, err := f.ob.NewRelay(nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil target: %v", err)
	}
	m, _ := target(t)
	if _, err := f.ob.NewRelay(m, outbox.WithLease(time.Second), outbox.WithEnqueueTimeout(time.Second)); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("lease not above timeout: %v", err)
	}
	if _, err := outbox.New(f.db, e.dialect, outbox.WithTablePrefix("Bad-")); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("bad prefix: %v", err)
	}
}
