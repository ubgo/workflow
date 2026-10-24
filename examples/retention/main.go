// Command retention keeps finished runs of the SQL driver for a while and
// then deletes them: an age per status, a count per workflow, an archive
// hook that copies each chunk to cold storage first, and the guard that
// keeps runs whose end no observer has seen. A fake clock stands in for the
// days passing.
//
// It is the program behind docs/operations/retention.md and the
// housekeeping part of docs/operations/tuning.md: the guides copy its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/sqlstore"
)

const (
	// runTimeout bounds the wait for one run, generous for a slow CI host.
	runTimeout = 10 * time.Second
	// statusPoll is how often finish re-reads a run's status.
	statusPoll = 5 * time.Millisecond
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "retention:", err)
		os.Exit(1)
	}
}

// workflows are "email", which fails on the payload "fail", and "report".
var workflows = []workflow.Workflow{
	{Name: "email", Steps: []workflow.Step{{Name: "send", Run: func(_ context.Context, in []byte) ([]byte, error) {
		if string(in) == `"fail"` {
			return nil, workflow.Permanent(errors.New("mailbox does not exist"))
		}
		return []byte(`"sent"`), nil
	}}}},
	{Name: "report", Steps: []workflow.Step{{Name: "build", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}},
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	dir, err := os.MkdirTemp("", "retention-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := keepAndPrune(ctx, out, filepath.Join(dir, "app.db")); err != nil {
		return err
	}
	return heldUndelivered(ctx, out, filepath.Join(dir, "held.db"))
}

// keepAndPrune runs the rules and the archive hook over a few runs.
func keepAndPrune(ctx context.Context, out io.Writer, path string) error {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	clk := clock.NewFake(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC))
	var coldStorage []string // stands in for an object store or an archive table

	// snippet:retention
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
		sqlstore.WithRetention(sqlstore.Retention{
			Succeeded: 24 * time.Hour,     // keep successes a day
			Failed:    7 * 24 * time.Hour, // failures a week; Cancelled: zero, forever
			PerWorkflow: map[string]sqlstore.Retention{
				"report": {KeepLast: 2}, // replaces the rules above for "report", whole
			},
			Undelivered: true, // no client of this cluster calls OnRunFinished
		}),
		sqlstore.WithArchive(func(ctx context.Context, runs []sqlstore.FinishedRun) error {
			for _, r := range runs {
				coldStorage = append(coldStorage, r.Workflow+" "+string(r.Status)+" "+string(r.Payload))
			}
			return nil // an error here keeps the chunk and stops the prune
		}),
		sqlstore.WithPruneChunk(2),  // runs per delete transaction and per archive call (default 100)
		sqlstore.WithoutAutoPrune(), // the app calls Prune itself
		sqlstore.WithClock(clk),
	)
	// snippet:end
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	jobs := []workflow.Job{
		{Workflow: "email", Payload: []byte(`"a"`)},
		{Workflow: "email", Payload: []byte(`"b"`)},
		{Workflow: "email", Payload: []byte(`"fail"`)},
	}
	for i := 1; i <= 5; i++ {
		jobs = append(jobs, workflow.Job{Workflow: "report", Payload: fmt.Appendf(nil, `"r%d"`, i)})
	}
	for _, job := range jobs {
		// One minute apart, so KeepLast's "newest by finish time" is clear.
		if err := finish(ctx, c, job); err != nil {
			return err
		}
		clk.Advance(time.Minute)
	}

	// snippet:prune
	res, err := c.Prune(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "now: deleted %d, held undelivered %d, idempotency %d, batch %d\n",
		res.Deleted, res.HeldUndelivered, res.HeldIdempotency, res.HeldBatch)
	// snippet:end

	clk.Advance(25 * time.Hour)
	res, err = c.Prune(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "a day later: deleted %d\n", res.Deleted)

	clk.Advance(7 * 24 * time.Hour)
	res, err = c.Prune(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "a week later: deleted %d\n", res.Deleted)

	sort.Strings(coldStorage)
	fmt.Fprintln(out, "archived:", strings.Join(coldStorage, ", "))
	return nil
}

// heldUndelivered shows the guard that keeps runs whose end was never
// delivered to an OnRunFinished observer, and what Prune reports.
func heldUndelivered(ctx context.Context, out io.Writer, path string) error {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	clk := clock.NewFake(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC))

	c, err := client(ctx, db, clk, sqlstore.Retention{Succeeded: time.Hour}) // Undelivered left false
	if err != nil {
		return err
	}
	if err := finish(ctx, c, workflow.Job{Workflow: "email", Payload: []byte(`"a"`)}); err != nil {
		return err
	}
	clk.Advance(2 * time.Hour)
	res, err := c.Prune(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "no observer, Undelivered false: deleted %d, held undelivered %d\n", res.Deleted, res.HeldUndelivered)
	c.Stop()

	c, err = client(ctx, db, clk, sqlstore.Retention{Succeeded: time.Hour, Undelivered: true})
	if err != nil {
		return err
	}
	defer c.Stop()
	res, err = c.Prune(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Undelivered true: deleted %d\n", res.Deleted)
	return nil
}

// client builds, migrates and starts a client with retention r and no
// background prune.
func client(ctx context.Context, db *sql.DB, clk clock.Clock, r sqlstore.Retention) (*sqlstore.Client, error) {
	opts := append(housekeeping(), sqlstore.WithRetention(r), sqlstore.WithoutAutoPrune(), sqlstore.WithClock(clk))
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, opts...)
	if err != nil {
		return nil, err
	}
	if err := c.Migrate(ctx); err != nil {
		return nil, err
	}
	for _, wf := range workflows {
		if err := c.Register(wf); err != nil {
			return nil, err
		}
	}
	return c, c.Start(ctx)
}

// housekeeping is the dispatcher's clean-up cadence, which runs whatever
// the retention: fired cron tick rows, consumed signals, expired
// uniqueness locks and finished debounce bursts.
func housekeeping() []sqlstore.Option {
	// snippet:housekeeping
	return []sqlstore.Option{
		// A tick row stops a second node firing the same cron tick; keep it
		// well above the longest pause a node can have (default 24h).
		sqlstore.WithTickRetention(48 * time.Hour),
		// Clean up every 20 dispatcher passes instead of 50: for a database
		// where signals or unique locks pile up fast.
		sqlstore.WithHousekeepingEvery(20),
	}
	// snippet:end
}

// finish enqueues job and waits until its run has ended.
func finish(ctx context.Context, c *sqlstore.Client, job workflow.Job) error {
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(runTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s is %s after %s", ref.RunID, st, runTimeout)
		}
		time.Sleep(statusPoll)
	}
}
