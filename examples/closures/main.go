// Command closures shows a Go function run as a job on the in-memory
// driver: a closure that retries, one delayed and cancelled, how a
// closure run shows up in Status, and the refusal on the SQL and Hatchet
// drivers, which cannot store or send a function.
//
// It is the program behind docs/jobs/closures.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))
	ends := make(chan workflow.RunEvent, 8)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	// snippet:closure
	var attempts atomic.Int64
	ref, err := workflow.EnqueueClosure(ctx, c, workflow.ClosureJob{
		Name: "warm-price-cache",
		Fn: func(ctx context.Context) error {
			if attempts.Add(1) == 1 {
				return errors.New("cache not ready")
			}
			return nil // warm the cache here
		},
		Retry: &workflow.RetryPolicy{Max: 2}, // no backoff: retry at once
	})
	if err != nil {
		return err
	}
	// snippet:end
	ev := <-ends
	fmt.Fprintf(out, "closure %s: %s after %d attempts, workflow %q, label %q\n",
		ev.Labels[workflow.LabelClosure], ev.Status, attempts.Load(), ev.Workflow, workflow.LabelClosure)

	// snippet:status
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "Status: %s, step %q\n", st, workflow.ClosureStep)

	// snippet:delayed
	later, err := workflow.EnqueueClosure(ctx, c, workflow.ClosureJob{
		Name:  "nudge",
		Fn:    func(context.Context) error { return nil },
		RunAt: start.Add(time.Hour),
	})
	if err != nil {
		return err
	}
	if err := c.Cancel(ctx, later); err != nil { // a closure run cancels like any run
		return err
	}
	// snippet:end
	ev = <-ends
	fmt.Fprintf(out, "closure %s: %s before it started\n", ev.Labels[workflow.LabelClosure], ev.Status)

	_, err = workflow.EnqueueClosure(ctx, c, workflow.ClosureJob{Name: "no-fn"})
	fmt.Fprintf(out, "closure without Fn: ErrInvalid %v\n", errors.Is(err, workflow.ErrInvalid))

	db, err := driversqlite.Memory()
	if err != nil {
		return err
	}
	defer db.Close()
	sqlc, err := sqlstore.New(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	// snippet:refused
	job := workflow.ClosureJob{Name: "warm-price-cache", Fn: func(context.Context) error { return nil }}
	for _, d := range []struct {
		name string
		c    workflow.Client
	}{{"sqlstore", sqlc}, {"hatchet", hatchet.New(hatchet.Config{})}} {
		_, err := workflow.EnqueueClosure(ctx, d.c, job)
		var unsupported *workflow.UnsupportedError
		if errors.As(err, &unsupported) {
			fmt.Fprintf(out, "%s refuses: missing %s, ErrUnsupported %v\n", d.name, unsupported.Missing, errors.Is(err, workflow.ErrUnsupported))
		}
	}
	// snippet:end
	return nil
}
