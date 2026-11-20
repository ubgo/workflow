package e2e_test

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	drivermysql "github.com/ubgo/sqlb/contrib/driver-mysql"
	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
)

// The environment variables that turn on each live service. Without one,
// that service's tests skip with the reason.
const (
	envPostgresDSN = "WFTEST_PG_DSN"
	envMySQLDSN    = "WFTEST_MYSQL_DSN"
	envRedisURL    = "WFTEST_REDIS_URL"
	envHatchet     = "HATCHET_CLIENT_TOKEN"
)

// start is the fake clock's first instant in every driver case.
var start = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)

// engine is one SQL database the tests run on.
type engine struct {
	name string
	// open returns a database for one test and a table prefix of its own,
	// whose tables are dropped when the test ends.
	open func(t *testing.T) (*sql.DB, string)
	// quote is the engine's identifier quote, for dropping tables.
	quote string
}

// engines returns SQLite always, and Postgres and MySQL when their DSN is
// set.
func engines(t *testing.T) []engine {
	out := []engine{{name: "sqlite", quote: `"`, open: openSQLite}}
	if dsn := os.Getenv(envPostgresDSN); dsn != "" {
		out = append(out, engine{name: "postgres", quote: `"`, open: sharedOpener(t, func() (*sql.DB, error) { return driverpgx.Open(dsn) }, `"`)})
	} else {
		t.Logf("%s unset: Postgres cases skipped (task recorder:e2e:pg)", envPostgresDSN)
	}
	if dsn := os.Getenv(envMySQLDSN); dsn != "" {
		out = append(out, engine{name: "mysql", quote: "`", open: sharedOpener(t, func() (*sql.DB, error) { return drivermysql.Open(dsn) }, "`")})
	} else {
		t.Logf("%s unset: MySQL cases skipped (task recorder:e2e:mysql)", envMySQLDSN)
	}
	return out
}

// openSQLite gives each test its own database file.
func openSQLite(t *testing.T) (*sql.DB, string) {
	t.Helper()
	db, err := driversqlite.Open(filepath.Join(t.TempDir(), "rec.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, "t" + randHex(4) + "_"
}

// sharedOpener shares one pool per engine across the run and gives each
// test its own table prefix, dropping its tables (the recorder's and the
// SQL driver's) afterwards.
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
		prefix := "t" + randHex(4) + "_"
		tt.Cleanup(func() { dropPrefixed(db, prefix, quote) })
		return db, prefix
	}
}

// recorderTables are the sqlrecorder table names after the prefix.
var recorderTables = []string{"events", "steps", "labels", "runs", "schema"}

// sqlstoreTables are the sqlstore driver's table names after the prefix,
// for the driver cases that put both in one database.
var sqlstoreTables = []string{"runs", "steps", "signals", "consumed", "schedules", "ticks", "locks", "starts", "batches", "unique_locks", "schedule_misfire", "debounce",
	"run_ext", "step_ext", "pauses", "schedule_ext", "run_chain", "step_err", "run_labels", "schedule_last", "attempt_errors", "run_forced", "outputs", "leaders", "runs_archive", "steps_archive", "run_info", "attempt_err_node", "nodes", "step_cancel", "schedule_spec", "unique_holders", "schedule_tenant", "journal", "buckets", "migrations", "migrate_lock"}

// dropPrefixed drops every table a test made under prefix: the recorder's
// under prefix+"rec_", the driver's under prefix.
func dropPrefixed(db *sql.DB, prefix, quote string) {
	for _, tbl := range recorderTables {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + quote + prefix + "rec_" + tbl + quote)
	}
	for _, tbl := range sqlstoreTables {
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
