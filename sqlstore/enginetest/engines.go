package enginetest

// The engines, timings and client builder every test of this module uses.
// They live in a non-test file so the shard packages under shards/ can run
// the conformance suites (conformance.go) in test binaries of their own.

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
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/sqlstore/internal/probe"
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

	// queueCaseLease is the lease of the shared suite's clients and of the
	// live-lease test.
	queueCaseLease = 5 * time.Second

	// queueLimitLease is the lease of the clients the queue-limit cases
	// build. Two clients share a global slot count kept by leases, so a
	// renewal stalled past 2/3 of the lease frees a held slot early and the
	// case sees one attempt too many; at load 235 under -race a 5s lease
	// still lost renewals. Only the crash case waits one out.
	queueLimitLease = 10 * time.Second

	// crashLease is the lease of the tests that crash a worker on purpose
	// and wait for a takeover. A live worker fences itself (ErrLeaseLost)
	// when a renewal write stalls for two thirds of its lease, and then the
	// step runs again before the test crashed anything; on a loaded machine
	// under -race a few hundred milliseconds of stall is common, so these
	// tests use a lease whose fence needs a full second of it. Each takeover
	// waits out about one lease.
	crashLease = 1500 * time.Millisecond

	// conformanceWait is the shared suite's wait (Harness.Wait) on these
	// engines, and statusWait the ceiling of waitStatus. Both only bound a
	// wait for something that must happen, so a passing run is no slower;
	// the suite's 5s fake-clock default assumes an idle machine, and SQLite
	// under -race beside other suites misses it without anything being
	// wrong. Windows in which nothing may happen (Harness.Quiet) keep their
	// defaults.
	conformanceWait = 30 * time.Second
	statusWait      = 30 * time.Second
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

// crash abandons c the way a killed process would, for the conformance
// suite's crash case and the lease tests.
func crash(c *sqlstore.Client) { probe.Crash(c) }

// startWith registers wfs on c and starts it with a context the test's
// cleanup cancels.
func startWith(t *testing.T, c *sqlstore.Client, wfs ...workflow.Workflow) context.Context {
	t.Helper()
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx
}
