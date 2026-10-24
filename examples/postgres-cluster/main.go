// Command postgres-cluster runs three workers of one queue over one
// database, the way several processes of a deployed app share it: thirty
// jobs each run exactly once, a cron tick that every worker sees due fires
// once, and every worker shuts down cleanly.
//
// With PG_DSN set it runs on that Postgres database (in tables under a
// fresh prefix, dropped at the end); without it, on a SQLite file in a
// temporary directory, which needs nothing installed. The three clients
// live in one process here so the program can count what they did; in
// production each is its own process with the same code.
//
// It is the program behind docs/operations/deploy.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

// envDSN names the Postgres DSN; unset, the program runs on SQLite.
const envDSN = "PG_DSN"

const (
	workers = 3  // clients over the one database
	jobs    = 30 // jobs enqueued across them

	// pollInterval is short so the demo finishes in well under a second;
	// production keeps the driver's default.
	pollInterval = 20 * time.Millisecond
	// runTimeout bounds the wait for all jobs, generous for a slow CI host.
	runTimeout = 30 * time.Second
	// statusPoll is how often waitAll re-reads a run's status.
	statusPoll = 10 * time.Millisecond
	// tickTimeout bounds the wait for the cron tick to fire once.
	tickTimeout = 10 * time.Second
	// secondTickWindow is how long the program watches for a duplicate
	// fire: many poll intervals, so a second worker had every chance to
	// claim the same tick.
	secondTickWindow = 500 * time.Millisecond
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "postgres-cluster:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	db, dialect, prefix, cleanup, err := open(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	return cluster(ctx, out, db, dialect, prefix)
}

// counts records how often each run's step executed, across all workers.
type counts struct {
	mu sync.Mutex
	n  map[workflow.RunID]int
}

// hit counts one execution of the step running under ctx. StepInfo is
// always present inside a step, so its ok result is not checked.
func (c *counts) hit(ctx context.Context) {
	info, _ := workflow.StepInfoFrom(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[info.RunID]++
}

// cluster starts the workers, enqueues the jobs, fires one cron tick and
// shuts everything down.
func cluster(ctx context.Context, out io.Writer, db *sql.DB, dialect sqlb.Dialect, prefix string) error {
	ran := &counts{n: map[workflow.RunID]int{}}
	ticks := make(chan time.Time, 10)
	// One scheduling clock for every worker, so the program decides when
	// the cron tick is due; production uses the real clock (the default).
	clk := clock.NewFake(time.Date(2026, 1, 5, 9, 59, 0, 0, time.UTC))

	// snippet:workflows
	workflows := []workflow.Workflow{
		{Name: "resize", Steps: []workflow.Step{{Name: "only", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			ran.hit(ctx)
			return in, nil
		}}}},
		{Name: "hourly-report", Cron: []string{"0 * * * *"}, Steps: []workflow.Step{{Name: "only", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			ticks <- clk.Now()
			return nil, nil
		}}}},
	}
	// snippet:end

	// snippet:workers
	// Every process runs the same start-up: Migrate (safe to run from all of
	// them at once; the engine's lock serialises it), Register, Start.
	startCtx, stopAll := context.WithCancel(ctx)
	defer stopAll()
	clients := make([]*sqlstore.Client, workers)
	for i := range clients {
		c, err := sqlstore.New(db, dialect,
			sqlstore.WithTablePrefix(prefix),
			sqlstore.WithClock(clk),
			sqlstore.WithPollInterval(pollInterval), // 20ms so the demo is quick; the default is 1s
		)
		if err != nil {
			return err
		}
		if err := c.Migrate(ctx); err != nil {
			return err
		}
		for _, wf := range workflows {
			if err := c.Register(wf); err != nil {
				return err
			}
		}
		if err := c.Start(startCtx); err != nil {
			return err
		}
		clients[i] = c
	}
	// snippet:end

	refs := make([]workflow.RunRef, 0, jobs)
	for i := range jobs {
		ref, err := clients[i%workers].Enqueue(ctx, workflow.Job{Workflow: "resize", Payload: fmt.Appendf(nil, `{"image":%d}`, i)})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	if err := waitAll(ctx, clients[0], refs); err != nil {
		return err
	}
	once := 0
	ran.mu.Lock()
	for _, ref := range refs {
		if ran.n[ref.RunID] == 1 {
			once++
		}
	}
	ran.mu.Unlock()
	fmt.Fprintf(out, "%d jobs, %d ran exactly once\n", jobs, once)

	// Move the shared clock past 10:00: all three workers see the tick due,
	// one fires it.
	clk.Advance(time.Minute)
	select {
	case at := <-ticks:
		fmt.Fprintln(out, "hourly-report fired at", at.Format("15:04"))
	case <-time.After(tickTimeout):
		return fmt.Errorf("the cron tick did not fire within %s", tickTimeout)
	}
	select {
	case <-ticks:
		return errors.New("the cron tick fired twice")
	case <-time.After(secondTickWindow):
		fmt.Fprintln(out, "fired once across", workers, "workers")
	}

	// snippet:shutdown
	// Shut down: cancel the Start context (what SIGTERM handling does) and
	// call Stop, which waits until every goroutine has exited. Steps still
	// running are handed back, so another worker takes them at once.
	stopAll()
	for _, c := range clients {
		c.Stop()
	}
	// snippet:end
	if _, err := clients[0].Enqueue(ctx, workflow.Job{Workflow: "resize"}); !errors.Is(err, workflow.ErrNotStarted) {
		return fmt.Errorf("calling Enqueue after Stop: %v, want ErrNotStarted", err)
	}
	fmt.Fprintln(out, "stopped; Enqueue now fails with", workflow.ErrNotStarted)
	return nil
}

// waitAll polls until every run has succeeded.
func waitAll(ctx context.Context, c *sqlstore.Client, refs []workflow.RunRef) error {
	deadline := time.Now().Add(runTimeout)
	for _, ref := range refs {
		for {
			st, err := c.Status(ctx, ref)
			if err != nil {
				return err
			}
			if st == workflow.StatusSucceeded {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("run %s is %s after %s", ref.RunID, st, runTimeout)
			}
			time.Sleep(statusPoll)
		}
	}
	return nil
}

// open returns the database to run on: Postgres when PG_DSN is set (under a
// fresh table prefix whose tables cleanup drops), else a SQLite file.
func open(ctx context.Context) (*sql.DB, sqlb.Dialect, string, func(), error) {
	if dsn := os.Getenv(envDSN); dsn != "" {
		db, err := driverpgx.Open(dsn)
		if err != nil {
			return nil, nil, "", nil, err
		}
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, nil, "", nil, err
		}
		prefix := "cluster_" + hex.EncodeToString(b[:]) + "_"
		return db, sqlb.PostgresDialect{}, prefix, func() {
			// The demo's result is already printed; a failed drop only
			// leaves tables behind, so it is reported, not fatal.
			if err := dropPrefix(ctx, db, prefix); err != nil {
				fmt.Fprintln(os.Stderr, "postgres-cluster: dropping tables under", prefix+":", err)
			}
			db.Close()
		}, nil
	}
	dir, err := os.MkdirTemp("", "postgres-cluster-*")
	if err != nil {
		return nil, nil, "", nil, err
	}
	db, err := driversqlite.Open(filepath.Join(dir, "cluster.db"))
	if err != nil {
		os.RemoveAll(dir)
		return nil, nil, "", nil, err
	}
	return db, sqlb.SQLiteDialect{}, "wf_", func() { db.Close(); os.RemoveAll(dir) }, nil
}

// dropPrefix drops every table under prefix in the current schema, so a
// demo run against a shared Postgres leaves nothing behind. A scan or
// iteration error is returned rather than skipped: a partial list would
// leave tables behind while looking like a clean run.
func dropPrefix(ctx context.Context, db *sql.DB, prefix string) error {
	rows, err := db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE $1`, prefix+"%")
	if err != nil {
		return err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return err
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var errs []error
	for _, n := range names {
		if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS "`+n+`" CASCADE`); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
