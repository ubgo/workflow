// Command worker-recycling runs a worker that stops itself after a limit
// so a supervisor can start a fresh process: after a number of finished
// jobs on the SQL driver (the backlog survives in the database and the
// next generation picks it up), and above a memory threshold on the
// in-memory driver, read through an injected reader.
//
// It is the program behind docs/operations/recycling.md.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// jobs is the backlog the first program run enqueues.
const jobs = 5

// runTimeout bounds each generation, generous for a slow CI host.
const runTimeout = 20 * time.Second

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "worker-recycling:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	dir, err := os.MkdirTemp("", "worker-recycling-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := runMaxJobs(ctx, out, filepath.Join(dir, "app.db")); err != nil {
		return fmt.Errorf("max jobs: %w", err)
	}
	if err := runMaxMemory(ctx, out); err != nil {
		return fmt.Errorf("max memory: %w", err)
	}
	return nil
}

// thumbnail is the job every generation works on.
func thumbnail(done *atomic.Int64) workflow.Workflow {
	return workflow.Workflow{Name: "thumbnail", Queue: "media", Steps: []workflow.Step{{
		Name: "resize",
		Run: func(context.Context, []byte) ([]byte, error) {
			done.Add(1)
			return nil, nil
		},
	}}}
}

// runMaxJobs enqueues a backlog, then plays a supervisor: it starts a
// worker generation, which serves until it has finished two jobs, and
// starts another until a generation finds the queue empty.
func runMaxJobs(ctx context.Context, out io.Writer, path string) error {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	var done atomic.Int64
	if err := enqueueBacklog(ctx, db, &done); err != nil {
		return err
	}

	for gen := 1; ; gen++ {
		before := done.Load()
		// snippet:max-jobs
		c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
			sqlstore.WithWorkers(1),
			sqlstore.WithPollInterval(20*time.Millisecond),
			sqlstore.WithRecycle(workflow.Recycle{MaxJobs: 2}), // like --max-jobs=2
			sqlstore.WithStopWhenIdle(200*time.Millisecond),    // like --stop-when-empty
		)
		if err != nil {
			return err
		}
		if err := c.Register(thumbnail(&done)); err != nil {
			return err
		}
		genCtx, cancel := context.WithTimeout(ctx, runTimeout)
		err = workflow.Serve(genCtx, c) // Start, then block until the client stops
		cancel()
		// snippet:end
		// snippet:supervisor
		var rec *workflow.RecycleError
		switch {
		case errors.As(err, &rec):
			fmt.Fprintf(out, "generation %d: recycled (%s), jobs done: %d\n", gen, rec.Reason, rec.Jobs)
			continue // a real worker exits 0 here and its supervisor starts a new process
		case err != nil:
			return err
		}
		fmt.Fprintf(out, "generation %d: queue empty, jobs done: %d, exit\n", gen, done.Load()-before)
		// snippet:end
		break
	}
	fmt.Fprintf(out, "done: %d of %d\n", done.Load(), jobs)
	return nil
}

// enqueueBacklog stands in for the web process that enqueues the work. It
// serves only its own queue, so the media jobs wait for the workers.
func enqueueBacklog(ctx context.Context, db *sql.DB, done *atomic.Int64) error {
	web, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithQueues(workflow.QueueConfig{Name: "web"}))
	if err != nil {
		return err
	}
	if err := web.Migrate(ctx); err != nil {
		return err
	}
	if err := web.Register(thumbnail(done)); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := web.Start(ctx); err != nil {
		return err
	}
	defer web.Stop()
	for i := 0; i < jobs; i++ {
		if _, err := web.Enqueue(ctx, workflow.Job{Workflow: "thumbnail"}); err != nil {
			return err
		}
	}
	return nil
}

// runMaxMemory recycles an in-memory worker whose heap reading crosses a
// threshold, read through an injected function and checked on a fake
// clock.
func runMaxMemory(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(start)
	var heap atomic.Uint64
	heap.Store(200 << 20)

	// snippet:max-memory
	c := memory.New(memory.WithClock(clk), memory.WithRecycle(workflow.Recycle{
		MaxMemory:  512 << 20,                            // like --memory=512
		Memory:     func() uint64 { return heap.Load() }, // nil reads the Go heap
		CheckEvery: 5 * time.Second,                      // between jobs too
		MaxTime:    time.Hour,                            // like --max-time=3600
	}))
	// snippet:end
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	clk.Advance(5 * time.Second)
	heap.Store(600 << 20) // a leak, or a burst that left the heap high
	deadline := time.After(runTimeout)
	for {
		select {
		case <-c.Done():
			err := c.StopCause()
			var rec *workflow.RecycleError
			if !errors.As(err, &rec) {
				return fmt.Errorf("stop cause %v, want a RecycleError", err)
			}
			fmt.Fprintf(out, "memory worker: recycled (%s) at %d MiB, ErrRecycle %v\n",
				rec.Reason, rec.Memory>>20, errors.Is(err, workflow.ErrRecycle))
			return nil
		case <-time.After(20 * time.Millisecond):
			clk.Advance(5 * time.Second) // the next check
		case <-deadline:
			return fmt.Errorf("no recycle within %s", runTimeout)
		}
	}
}
