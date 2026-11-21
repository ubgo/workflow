// Command sql-options walks the SQL driver's everyday options one at a
// time:
//
//   - pick a database and its dialect (SQLite, Postgres, MySQL);
//   - workers and polling, and Client.Wake after a transaction that used
//     EnqueueTx, so the run starts without waiting for the next poll;
//   - how database errors are sorted (WithErrorClassifier,
//     DefaultErrorClassifier, ErrorClass, WithTxAttempts);
//   - the remaining options (WithClock, WithLogger, WithMiddleware,
//     WithJitterSource) and Client.DB, the pool for reading the tables;
//   - the shared stores built on their own, with no driver client running
//     (NewDataStore, NewJournalStore).
//
// It runs on SQLite files in a temporary directory and needs nothing
// installed; openDB shows the Postgres and MySQL lines the same program
// would use.
//
// It is the program behind the "Pick a database", "Workers and polling",
// "Database errors", "Options" and "Per-run facts, journal and bucket
// state" sections of docs/drivers/sqlstore.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	drivermysql "github.com/ubgo/sqlb/contrib/driver-mysql"
	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

// engine names a database engine openDB can open.
type engine string

// The engines openDB takes.
const (
	engineSQLite   engine = "sqlite"
	enginePostgres engine = "postgres"
	engineMySQL    engine = "mysql"
)

const (
	// workers is how many step attempts the client runs at once.
	workers = 4
	// idlePoll is the poll interval of the Wake demo: an hour, so a run
	// that starts at once can only have been started by Wake.
	idlePoll = time.Hour
	// txAttempts is how often a transaction that lost a lock race is
	// tried in all; raised from the default 5 for a SQLite file that many
	// processes write.
	txAttempts = 10
	// pinnedJitter is the value WithJitterSource returns, so retry delays
	// with Backoff.Jitter are the same on every run of a test.
	pinnedJitter = 0.5
	// runTimeout bounds the wait for one run, generous for a slow CI host
	// and far below idlePoll.
	runTimeout = 10 * time.Second
	// statusPoll is how often finish re-reads a run's status.
	statusPoll = 5 * time.Millisecond
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sql-options:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) (err error) {
	w = &lockedWriter{w: w} // the step middleware prints from a worker goroutine
	dir, err := os.MkdirTemp("", "sql-options-*")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	db, dialect, err := openDB(engineSQLite, filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	if err := wake(ctx, w, db, dialect); err != nil {
		return err
	}
	if err := classify(ctx, w, db, dialect); err != nil {
		return err
	}
	if err := options(ctx, w, db, dialect); err != nil {
		return err
	}
	return stores(ctx, w, filepath.Join(dir, "state.db"))
}

// snippet:pick
// openDB opens the pool and returns the sqlb dialect its SQL is written
// for. The driver module is the app's choice; sqlstore itself requires
// none.
func openDB(e engine, dsn string) (*sql.DB, sqlb.Dialect, error) {
	switch e {
	case engineSQLite: // one machine; a file path, ":memory:" is lost on exit
		db, err := driversqlite.Open(dsn)
		return db, sqlb.SQLiteDialect{}, err
	case enginePostgres: // many machines; the only engine with WithSchema
		db, err := driverpgx.Open(dsn)
		return db, sqlb.PostgresDialect{}, err
	case engineMySQL: // many machines; the schema is the database in the DSN
		db, err := drivermysql.Open(dsn)
		return db, sqlb.MySQLDialect{}, err
	}
	return nil, nil, fmt.Errorf("unknown engine %q", e)
}

// snippet:end

// receipt is the workflow the demos run.
var receipt = workflow.Workflow{Name: "send-receipt", Steps: []workflow.Step{{Name: "send",
	Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}

// wake enqueues inside the app's transaction on a client that polls once
// an hour, and wakes it after the commit.
func wake(ctx context.Context, w io.Writer, db *sql.DB, dialect sqlb.Dialect) error {
	// snippet:workers
	c, err := sqlstore.New(db, dialect,
		sqlstore.WithWorkers(workers),       // step attempts at once (default 8)
		sqlstore.WithPollInterval(idlePoll), // look for other clients' work (default 1s)
	)
	// snippet:end
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil {
		return err
	}
	if err := c.Register(receipt); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(runCtx); err != nil {
		return err
	}
	defer c.Stop()
	// A first run by plain Enqueue, which wakes the client itself: once it
	// has ended, the client idles until its next poll.
	first, err := c.Enqueue(ctx, workflow.Job{Workflow: receipt.Name, Payload: []byte(`"A-0"`)})
	if err != nil {
		return err
	}
	if _, err := finish(ctx, c, first); err != nil {
		return err
	}
	began := time.Now()

	// snippet:wake
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	ref, err := c.EnqueueTx(ctx, tx, workflow.Job{Workflow: receipt.Name, Payload: []byte(`"A-1"`)})
	if err != nil {
		return errors.Join(err, tx.Rollback())
	}
	// ... the app's own writes in tx ...
	if err := tx.Commit(); err != nil {
		return err
	}
	c.Wake() // look now, not at the next poll an hour away
	// snippet:end

	if _, err := finish(ctx, c, ref); err != nil {
		return err
	}
	fmt.Fprintf(w, "run enqueued in a transaction ran before the next poll: %v\n", time.Since(began) < idlePoll)
	return nil
}

// snippet:classifier
// proxyError is how a database proxy in front of the pool reports errors:
// a code of its own, which DefaultErrorClassifier cannot read.
type proxyError struct{ code string }

// Error implements error.
func (e *proxyError) Error() string { return "proxy: " + e.code }

// Codes the proxy sends.
const (
	proxyDuplicate = "DUP"  // a unique index refused the write
	proxyBusy      = "BUSY" // a lock conflict; try the transaction again
	proxyDown      = "DOWN" // the database behind the proxy is unreachable
)

// classifyProxy sorts the proxy's errors and leaves every other error to
// the default, which knows the drivers sqlb's contrib modules open.
func classifyProxy(err error) sqlstore.ErrorClass {
	var pe *proxyError
	if errors.As(err, &pe) {
		return sqlstore.ErrorClass{
			UniqueViolation: pe.code == proxyDuplicate,
			Retryable:       pe.code == proxyBusy,
			Unavailable:     pe.code == proxyDown,
		}
	}
	return sqlstore.DefaultErrorClassifier(err)
}

// snippet:end

// classify shows how errors are sorted and the client options that use
// it.
func classify(ctx context.Context, w io.Writer, db *sql.DB, dialect sqlb.Dialect) error {
	// snippet:classify
	c, err := sqlstore.New(db, dialect,
		sqlstore.WithErrorClassifier(classifyProxy),
		sqlstore.WithTxAttempts(txAttempts), // tries in all for a lost lock race (default 5; 1 turns it off)
	)
	// snippet:end
	if err != nil {
		return err
	}
	// A real unique violation from SQLite, made through the client's pool.
	if _, err := c.DB().ExecContext(ctx, "CREATE TABLE demo_keys (k TEXT PRIMARY KEY)"); err != nil {
		return err
	}
	if _, err := c.DB().ExecContext(ctx, "INSERT INTO demo_keys (k) VALUES ('a')"); err != nil {
		return err
	}
	_, dup := c.DB().ExecContext(ctx, "INSERT INTO demo_keys (k) VALUES ('a')")
	for _, e := range []struct {
		name string
		err  error
	}{
		{"sqlite duplicate", dup},
		{"proxy BUSY", fmt.Errorf("claim: %w", &proxyError{code: proxyBusy})},
		{"proxy DOWN", &proxyError{code: proxyDown}},
		{"other", errors.New("syntax error")},
	} {
		fmt.Fprintf(w, "%-16s %+v\n", e.name+":", classifyProxy(e.err))
	}
	return nil
}

// options builds a client with the remaining options and reads its tables
// through Client.DB.
func options(ctx context.Context, w io.Writer, db *sql.DB, dialect sqlb.Dialect) error {
	// snippet:options
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// logSteps is a step middleware that prints each attempt it wraps.
	logSteps := func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			fmt.Fprintf(w, "middleware: %s/%s attempt %d\n", info.Workflow, info.Step, info.Attempt)
			return next(ctx, in)
		}
	}
	c, err := sqlstore.New(db, dialect,
		sqlstore.WithClock(clock.Real{}),                                  // every due time; tests pass clock.NewFake
		sqlstore.WithLogger(logger),                                       // what the driver cannot return to a caller
		sqlstore.WithMiddleware(logSteps),                                 // around every step and OnFailure handler
		sqlstore.WithJitterSource(func() float64 { return pinnedJitter }), // Backoff.Jitter's draws, pinned
	)
	// snippet:end
	if err != nil {
		return err
	}
	if err := c.Register(receipt); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(runCtx); err != nil {
		return err
	}
	defer c.Stop()
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: receipt.Name, Payload: []byte(`"A-2"`)})
	if err != nil {
		return err
	}
	if _, err := finish(ctx, c, ref); err != nil {
		return err
	}

	// snippet:db
	var n int
	// Client.DB is the pool the client was built with: read the tables
	// directly for a report, or run Migrate inside a wider schema step.
	if err := c.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM wf_runs WHERE status = 'succeeded'").Scan(&n); err != nil {
		return err
	}
	fmt.Fprintf(w, "succeeded runs in wf_runs: %d\n", n)
	// snippet:end
	return nil
}

// stores builds two shared stores over a database no driver client runs
// on, each with its own migration list.
func stores(ctx context.Context, w io.Writer, path string) (err error) {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	// snippet:stores
	data, err := sqlstore.NewDataStore(db, sqlb.SQLiteDialect{}, sqlstore.WithTablePrefix("app_"))
	if err != nil {
		return err
	}
	if err := data.Migrate(ctx); err != nil { // the data table only, in the shared ledger
		return err
	}
	journal, err := sqlstore.NewJournalStore(db, sqlb.SQLiteDialect{}, sqlstore.WithTablePrefix("app_"))
	if err != nil {
		return err
	}
	if err := journal.Migrate(ctx); err != nil {
		return err
	}
	ver, err := data.Put(ctx, workflow.Folder("import"), "cursor", []byte(`42`), 0) // 0: no version check
	if err != nil {
		return err
	}
	if err := journal.Save(ctx, "run_1", "fetch", []byte(`"page-1"`)); err != nil {
		return err
	}
	// snippet:end

	val, _, ok, err := data.Get(ctx, workflow.Folder("import"), "cursor")
	if err != nil {
		return err
	}
	memo, found, err := journal.Load(ctx, "run_1", "fetch")
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "data store without a client: cursor %s (found %v, version %d)\n", val, ok, ver)
	fmt.Fprintf(w, "journal store without a client: fetch %s (found %v)\n", memo, found)
	return nil
}

// finish waits until the run behind ref has ended and returns its status.
func finish(ctx context.Context, c *sqlstore.Client, ref workflow.RunRef) (workflow.Status, error) {
	deadline := time.Now().Add(runTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return st, err
		}
		if st.IsTerminal() {
			return st, nil
		}
		if time.Now().After(deadline) {
			return st, fmt.Errorf("run %s is %s after %s", ref.RunID, st, runTimeout)
		}
		time.Sleep(statusPoll)
	}
}

// lockedWriter serialises writes from the program and its step middleware.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// Write implements io.Writer: one write at a time to the wrapped writer.
func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
