package sqlstore_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/sqlb"
	drivermysql "github.com/ubgo/sqlb/contrib/driver-mysql"
	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// start is the fake clock's first instant in every test.
var start = time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)

// envPostgresDSN names the variable holding the Postgres test database's
// DSN. The Taskfile sets it (task test:pg); without it the Postgres tests
// skip, so `go test` works on a machine with no Postgres.
const envPostgresDSN = "WFTEST_PG_DSN"

// envMySQLDSN names the variable holding the MySQL test database's DSN
// (task test:mysql sets it); without it the MySQL tests skip.
const envMySQLDSN = "WFTEST_MYSQL_DSN"

// Test timings: a short poll so a client sees another's work quickly, and
// a lease short enough that the lease-expiry cases finish well inside the
// suite's 5s wait, yet long enough that a loaded machine (three engines
// under -race) never delays a live worker's renewal past it. Production defaults are a second and thirty seconds.
const (
	testPoll  = 10 * time.Millisecond
	testLease = 2 * time.Second

	// queueCaseLease is the lease of the clients the queue-limit cases build.
	queueCaseLease = 5 * time.Second
)

// engine is one database the suite runs on.
type engine struct {
	name    string
	dialect sqlb.Dialect
	// open returns a database for one test and the table prefix to use in
	// it; cleanup is registered on t.
	open func(t *testing.T) (*sql.DB, string)
}

func engines(t *testing.T) []engine {
	out := []engine{{name: "sqlite", dialect: sqlb.SQLiteDialect{}, open: openSQLite}}
	if dsn, ok := os.LookupEnv(envPostgresDSN); ok && dsn != "" {
		out = append(out, engine{name: "postgres", dialect: sqlb.PostgresDialect{}, open: sharedOpener(t, func() (*sql.DB, error) { return driverpgx.Open(dsn) }, `"`)})
	} else {
		t.Logf("%s unset: Postgres cases skipped (run task test:pg)", envPostgresDSN)
	}
	if dsn, ok := os.LookupEnv(envMySQLDSN); ok && dsn != "" {
		out = append(out, engine{name: "mysql", dialect: sqlb.MySQLDialect{}, open: sharedOpener(t, func() (*sql.DB, error) { return drivermysql.Open(dsn) }, "`")})
	} else {
		t.Logf("%s unset: MySQL cases skipped (run task test:mysql)", envMySQLDSN)
	}
	return out
}

// openSQLite gives each test its own database file.
func openSQLite(t *testing.T) (*sql.DB, string) {
	t.Helper()
	db, err := driversqlite.Open(filepath.Join(t.TempDir(), "wf.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, "wf_"
}

// sharedOpener shares one pool across the run and gives each test its own
// table prefix, dropping its tables afterwards, so tests never see each
// other's rows. quote is the engine's identifier quote.
func sharedOpener(t *testing.T, open func() (*sql.DB, error), quote string) func(t *testing.T) (*sql.DB, string) {
	var once sync.Once
	var db *sql.DB
	var openErr error
	return func(tt *testing.T) (*sql.DB, string) {
		tt.Helper()
		once.Do(func() {
			db, openErr = open()
			if openErr == nil {
				t.Cleanup(func() { _ = db.Close() })
			}
		})
		if openErr != nil {
			tt.Fatalf("open database: %v", openErr)
		}
		prefix := "t" + randHex(6) + "_"
		tt.Cleanup(func() { dropTables(db, prefix, quote) })
		return db, prefix
	}
}

func dropTables(db *sql.DB, prefix, quote string) {
	for _, tbl := range []string{"runs", "steps", "signals", "consumed", "schedules", "ticks", "locks", "starts", "batches", "unique_locks", "schedule_misfire", "debounce",
		"run_ext", "step_ext", "pauses", "schedule_ext", "run_chain", "step_err", "run_labels", "schedule_last", "attempt_errors", "run_forced", "outputs", "leaders", "runs_archive", "steps_archive", "run_info", "attempt_err_node", "nodes", "step_cancel", "schedule_spec", "unique_holders", "schedule_tenant", "journal", "buckets", "migrations", "migrate_lock"} {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + quote + prefix + tbl + quote)
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// newClient builds and migrates a client; t's cleanup stops it before the
// database closes.
func newClient(t *testing.T, e engine, db *sql.DB, prefix string, opts ...sqlstore.Option) *sqlstore.Client {
	t.Helper()
	all := append([]sqlstore.Option{
		sqlstore.WithTablePrefix(prefix),
		sqlstore.WithPollInterval(testPoll),
		sqlstore.WithLease(testLease),
	}, opts...)
	c, err := sqlstore.New(db, e.dialect, all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(c.Stop)
	return c
}

// TestConformance runs the shared driver suite on every engine, on a fake
// scheduling clock, with a Restart hook over the same database: the
// durable cases (a waiting run, a scheduled run and a step cut off by a
// stop all survive into a new client) run here, and a Crash hook whose old
// client writes nothing more, so the crash case proves lease expiry.
func TestConformance(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
				db, prefix := e.open(t)
				clk := clock.NewFake(start)
				current := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
				return workflowtest.Instance{
					Client: current,
					Clock:  clk,
					Restart: func(t *testing.T) workflow.Client {
						current = newClient(t, e, db, prefix, sqlstore.WithClock(clk))
						return current
					},
					Crash: func(t *testing.T) workflow.Client {
						sqlstore.Crash(current)
						current = newClient(t, e, db, prefix, sqlstore.WithClock(clk))
						return current
					},
					EnqueueOnly: func(t *testing.T) workflow.Client {
						return newClient(t, e, db, prefix, sqlstore.WithClock(clk), sqlstore.WithEnqueueOnly())
					},
				}
			}, NewWithQueues: func(t *testing.T, qs []workflow.QueueConfig) workflowtest.Instance {
				db, prefix := e.open(t)
				clk := clock.NewFake(start)
				// A lease is wall time (WithLeaseClock), so a client slowed by -race
				// can miss its heartbeat inside testLease and lose an attempt the
				// queue-limit cases hold on purpose; a longer lease leaves headroom,
				// and the crash case waits for it to expire with a matching limit.
				mk := func(extra ...sqlstore.Option) *sqlstore.Client {
					return newClient(t, e, db, prefix, append([]sqlstore.Option{sqlstore.WithClock(clk), sqlstore.WithQueues(qs...), sqlstore.WithLease(queueCaseLease)}, extra...)...)
				}
				current := mk()
				return workflowtest.Instance{
					Client: current,
					Clock:  clk,
					Restart: func(t *testing.T) workflow.Client {
						current = mk()
						return current
					},
					Crash: func(t *testing.T) workflow.Client {
						sqlstore.Crash(current)
						current = mk()
						return current
					},
					Peer: func(t *testing.T) workflow.Client { return mk() },
				}
			}})
		})
	}
}
