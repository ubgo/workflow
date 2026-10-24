// Command manage-runs shows the operator's view of runs on the in-memory
// driver: list failed runs with their error and step, re-run one from the
// step that failed (the steps that succeeded do not run again), forget a
// finished run, cancel every waiting run of a workflow at once, and pause
// a workflow.
//
// It is the program behind docs/jobs/manage-runs.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
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

	var warehouseDown atomic.Bool
	warehouseDown.Store(true)
	var charges, ships atomic.Int64
	order := workflow.Workflow{Name: "fulfil-order", Steps: []workflow.Step{
		{Name: "charge", Run: func(_ context.Context, in []byte) ([]byte, error) {
			charges.Add(1)
			return in, nil
		}},
		{Name: "ship", After: []string{"charge"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			ships.Add(1)
			if warehouseDown.Load() {
				return nil, errors.New("warehouse API: 503")
			}
			return in, nil
		}},
	}}

	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))
	if err := c.Register(order); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()

	failed, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil-order", Payload: []byte(`"o-1"`), Labels: map[string]string{"tenant": "acme"}})
	if err != nil {
		return err
	}
	if err := waitFor(ctx, c, failed, workflow.StatusFailed); err != nil {
		return err
	}

	// snippet:list
	page, err := c.Runs(ctx, workflow.RunQuery{
		Workflow: "fulfil-order",
		Statuses: []workflow.Status{workflow.StatusFailed},
		Labels:   map[string]string{"tenant": "acme"},
		Limit:    50,
	})
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		fmt.Fprintf(out, "failed: %s at step %q: %s\n", r.Ref.RunID, r.FailedStep, r.Error)
	}
	// page.Next is empty on the last page; pass it as Cursor for the next one.
	// snippet:end

	// snippet:retry
	warehouseDown.Store(false) // the outage is over
	ref, err := c.Retry(ctx, failed)
	if err != nil {
		return err
	}
	// snippet:end
	if err := waitFor(ctx, c, ref, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintf(out, "retried %s: succeeded; charge ran %d time(s), ship %d\n", ref.RunID, charges.Load(), ships.Load())

	// snippet:delete
	err = c.Delete(ctx, ref)
	_, statusErr := c.Status(ctx, ref)
	fmt.Fprintln(out, "deleted:", err == nil, "then Status is ErrNotFound:", errors.Is(statusErr, workflow.ErrNotFound))
	// snippet:end

	for _, o := range []string{"o-2", "o-3"} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil-order", Payload: []byte(`"` + o + `"`), RunAt: start.Add(time.Hour)}); err != nil {
			return err
		}
	}
	// snippet:cancel-all
	n, err := c.CancelAll(ctx, workflow.RunQuery{Workflow: "fulfil-order"}) // runs not yet started
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "cancelled waiting runs:", n)
	// snippet:end

	// snippet:pause
	if err := c.Pause(ctx, workflow.PauseWorkflow("fulfil-order")); err != nil {
		return err
	}
	held, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil-order", Payload: []byte(`"o-4"`)})
	if err != nil {
		return err
	}
	st, err := c.Status(ctx, held)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "while paused:", st)
	if err := c.Resume(ctx, workflow.PauseWorkflow("fulfil-order")); err != nil {
		return err
	}
	// snippet:end
	if err := waitFor(ctx, c, held, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintln(out, "after Resume: succeeded")
	return nil
}

// waitFor polls until ref reaches want, and fails on another terminal
// status. An app would rather observe run ends (workflow.RunNotifier).
func waitFor(ctx context.Context, c *memory.Client, ref workflow.RunRef, want workflow.Status) error {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if st.IsTerminal() {
			return fmt.Errorf("run %s ended %s, want %s", ref.RunID, st, want)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}
