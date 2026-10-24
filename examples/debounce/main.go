// Command debounce folds a burst of jobs with the same key into one run: in
// one process with the default in-memory store, then across two processes
// (two sqlstore clients on one SQLite file) through sqlstore.DebounceStore.
//
// It is the program behind docs/reliability/debounce.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sqlstore"
)

const (
	// pollInterval is short so the two-process demo finishes quickly;
	// production keeps the driver's default.
	pollInterval = 20 * time.Millisecond
	// statusPoll is how often waitTerminal re-reads a run's status.
	statusPoll = 20 * time.Millisecond
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "debounce:", err)
		os.Exit(1)
	}
}

// byProduct is the debounce key: every change to one product within the
// window collapses into one reindex.
func byProduct(j workflow.Job) string { return j.Labels["product"] }

// reindex is the debounced workflow; its step reports the payload it ran
// with, so the output shows which job of the burst won.
func reindex(ran chan<- string) workflow.Workflow {
	return workflow.Workflow{Name: "reindex", Steps: []workflow.Step{{
		Name: "index",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			ran <- string(in)
			return nil, nil
		},
	}}}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := inOneProcess(ctx, out); err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "debounce-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	return acrossProcesses(ctx, out, filepath.Join(dir, "queue.db"))
}

// inOneProcess debounces on the memory driver with a fake clock, so the
// ten-second window passes at once.
func inOneProcess(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "-- Debounce a burst")
	ran := make(chan string, 3)

	// snippet:debounce
	clk := clock.NewFake(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))
	mc := memory.New(memory.WithClock(clk))
	if err := mc.Register(reindex(ran)); err != nil {
		return err
	}
	// Each job waits 10s of quiet; a key that never goes quiet still runs
	// every minute. Debounce needs CapDelayedStart and CapCancel.
	debounce, err := middleware.Debounce(mc, clk, byProduct, 10*time.Second, time.Minute)
	if err != nil {
		return err
	}
	c := middleware.WrapClient(mc, debounce)
	// snippet:end

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := mc.Start(ctx); err != nil {
		return err
	}

	// snippet:burst
	var refs []workflow.RunRef
	for n := 1; n <= 3; n++ {
		ref, err := c.Enqueue(ctx, workflow.Job{
			Workflow: "reindex",
			Payload:  fmt.Appendf(nil, `{"change":%d}`, n),
			Labels:   map[string]string{"product": "p-42"},
		})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
		clk.Advance(2 * time.Second) // the next change comes 2s later
	}
	// snippet:end

	for i, ref := range refs[:2] {
		st, err := mc.Status(ctx, ref)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "job %d: %s\n", i+1, st)
	}
	clk.Advance(10 * time.Second) // quiet for the window
	fmt.Fprintln(out, "ran with", <-ran)
	return nil
}

// acrossProcesses runs two sqlstore clients on one SQLite file, standing
// in for two servers on one Postgres or MySQL database. Each has its own
// Debounce middleware; they share the bursts through the debounce table.
func acrossProcesses(ctx context.Context, out io.Writer, path string) error {
	fmt.Fprintln(out, "-- A store shared by every process")
	ran := make(chan string, 3)

	type process struct {
		sql *sqlstore.Client
		c   workflow.Client
	}
	newProcess := func() (process, func(), error) {
		db, err := driversqlite.Open(path)
		if err != nil {
			return process{}, nil, err
		}
		sc, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
		if err != nil {
			return process{}, nil, err
		}
		if err := sc.Migrate(ctx); err != nil { // also creates wf_debounce
			return process{}, nil, err
		}
		if err := sc.Register(reindex(ran)); err != nil {
			return process{}, nil, err
		}
		// snippet:sql-store
		debounce, err := middleware.Debounce(sc, nil, byProduct, time.Second, 0,
			middleware.WithDebounceStore(sc.DebounceStore()))
		if err != nil {
			return process{}, nil, err
		}
		c := middleware.WrapClient(sc, debounce)
		// snippet:end
		pctx, cancel := context.WithCancel(ctx)
		if err := sc.Start(pctx); err != nil {
			cancel()
			return process{}, nil, err
		}
		return process{sql: sc, c: c}, func() { cancel(); sc.Stop(); _ = db.Close() }, nil
	}

	a, closeA, err := newProcess()
	if err != nil {
		return err
	}
	defer closeA()
	b, closeB, err := newProcess()
	if err != nil {
		return err
	}
	defer closeB()

	// The burst alternates between the two processes.
	var refs []workflow.RunRef
	for n, p := range []process{a, b, a} {
		ref, err := p.c.Enqueue(ctx, workflow.Job{
			Workflow: "reindex",
			Payload:  fmt.Appendf(nil, `{"change":%d}`, n+1),
			Labels:   map[string]string{"product": "p-42"},
		})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	fmt.Fprintln(out, "ran with", <-ran)
	for i, ref := range refs {
		st, err := waitTerminal(ctx, b.sql, ref)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "job %d (from process %s): %s\n", i+1, []string{"A", "B", "A"}[i], st)
	}
	return nil
}

// waitTerminal polls a run's status until it is terminal.
func waitTerminal(ctx context.Context, c workflow.Client, ref workflow.RunRef) (workflow.Status, error) {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st.IsTerminal() {
			return st, err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(statusPoll):
		}
	}
}
