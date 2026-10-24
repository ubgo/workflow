// Command functions shows a durable closure: a Go function registered
// under a name when the process starts and enqueued by that name with a
// JSON argument. It retries and runs a catch function on the in-memory
// driver, refuses a name nobody registered, then survives a restart on
// the SQL driver over a SQLite file: one client enqueues a delayed run
// and stops, a second client on the same file registers the same name
// and runs it.
//
// It is the program behind docs/jobs/functions.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// runTimeout bounds each wait for a run end, generous for a slow CI host.
const runTimeout = 10 * time.Second

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "functions:", err)
		os.Exit(1)
	}
}

// snippet:define
// welcome is the argument; it travels as JSON, so it survives a restart.
type welcome struct {
	Email string
}

// sendWelcome is built once, at package level or in main, and registered in
// every process that runs workers. The name is what is stored with a run.
func sendWelcome(sent *atomic.Int64, caught chan<- string) workflow.Func[welcome] {
	return workflow.NewFunc("send-welcome",
		func(ctx context.Context, in welcome) error {
			if strings.HasSuffix(in.Email, ".invalid") {
				return workflow.Permanent(errors.New("no such domain")) // no retry
			}
			if sent.Add(1) == 1 {
				return errors.New("mail server busy") // retried under FuncRetry
			}
			return nil // send the mail here
		},
		workflow.FuncRetry(&workflow.RetryPolicy{Max: 2}),
		workflow.FuncTimeout(30*time.Second),
		workflow.FuncCatch(func(ctx context.Context, in welcome) error {
			caught <- in.Email // runs once when a run fails for good
			return nil
		}),
	)
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := runMemory(ctx, out); err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	dir, err := os.MkdirTemp("", "functions-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := runRestart(ctx, out, filepath.Join(dir, "app.db")); err != nil {
		return fmt.Errorf("sqlite: %w", err)
	}
	return nil
}

// runMemory enqueues the Func on the in-memory driver: once with a retry,
// once failing for good so the catch runs, and once by an unknown name.
func runMemory(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := memory.New()
	defer c.Stop()
	ends := make(chan workflow.RunEvent, 4)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	var sent atomic.Int64
	caught := make(chan string, 1)

	// snippet:register
	fn := sendWelcome(&sent, caught)
	if err := workflow.RegisterFunc(c, fn); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	if _, err := fn.Enqueue(ctx, c, welcome{Email: "ann@example.com"}); err != nil {
		return err
	}
	// snippet:end
	ev := <-ends
	fmt.Fprintf(out, "%s: %s after %d attempts, step %q\n", ev.Workflow, ev.Status, sent.Load(), workflow.FuncStep)

	if _, err := fn.Enqueue(ctx, c, welcome{Email: "eve@example.invalid"}); err != nil {
		return err
	}
	ev = <-ends
	fmt.Fprintf(out, "bad address: %s, catch ran for %q\n", ev.Status, <-caught)

	// snippet:by-name
	job, err := workflow.FuncJob("send-invoice", map[string]string{"Order": "A-1"})
	if err != nil {
		return err
	}
	_, err = c.Enqueue(ctx, job) // nothing registered "send-invoice" here
	// snippet:end
	fmt.Fprintf(out, "unregistered name: ErrNotFound %v\n", errors.Is(err, workflow.ErrNotFound))
	return nil
}

// runRestart enqueues a delayed run with one SQL client, stops it, and
// lets a second client on the same database file run it.
func runRestart(ctx context.Context, out io.Writer, path string) error {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	clk := clock.NewFake(start)
	var sent atomic.Int64
	caught := make(chan string, 1)
	fn := sendWelcome(&sent, caught)
	sent.Store(1) // skip the demo's first-attempt failure here

	// The first process: enqueue for an hour from now, then stop.
	first, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithClock(clk))
	if err != nil {
		return err
	}
	if err := first.Migrate(ctx); err != nil {
		return err
	}
	if err := workflow.RegisterFunc(first, fn); err != nil {
		return err
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	if err := first.Start(firstCtx); err != nil {
		return err
	}
	// snippet:delayed
	job, err := fn.Job(welcome{Email: "bob@example.com"})
	if err != nil {
		return err
	}
	job.RunAt = clk.Now().Add(time.Hour)
	job.IdempotencyKey = "welcome:bob@example.com"
	ref, err := first.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	// snippet:end
	first.Stop()
	fmt.Fprintln(out, "first process stopped with the run waiting")

	// The second process: register the same name and start.
	second, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithClock(clk), sqlstore.WithPollInterval(50*time.Millisecond))
	if err != nil {
		return err
	}
	ends := make(chan workflow.RunEvent, 1)
	if err := second.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := workflow.RegisterFunc(second, fn); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := second.Start(ctx); err != nil {
		return err
	}
	defer second.Stop()
	clk.Advance(time.Hour)
	deadline := time.After(runTimeout)
	for {
		select {
		case ev := <-ends:
			if ev.Ref.ScheduledID != ref.ScheduledID {
				continue
			}
			fmt.Fprintf(out, "second process ran %s: %s\n", ev.Workflow, ev.Status)
			return nil
		case <-time.After(100 * time.Millisecond):
			clk.Advance(time.Second) // move the fake clock so the poller ticks
		case <-deadline:
			return fmt.Errorf("the run did not finish within %s", runTimeout)
		}
	}
}
