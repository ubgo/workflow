// Command manage-runs shows the operator's view of runs on the in-memory
// driver: list failed runs with their error and step, re-run one from the
// step that failed (the steps that succeeded do not run again), forget a
// finished run, cancel every waiting run of a workflow at once, pause a
// workflow, and reset a failed run from an earlier step. Each operation is
// reached the way an app reaches it: a capability check and a type
// assertion on the workflow.Client it holds.
//
// It is the program behind docs/jobs/manage-runs.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

const (
	// pollInterval is how often waitFor re-reads a run's status: the
	// in-memory driver ends these runs within milliseconds.
	pollInterval = time.Millisecond
	// waitingDelay puts o-2 and o-3 in the future on the fake clock, which
	// never moves, so they stay waiting until CancelAll cancels them.
	waitingDelay = time.Hour
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
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
	// An app holds the interface, not the driver type: the operations below
	// are optional interfaces beside workflow.Client, found by assertion.
	var cl workflow.Client = c

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
		Limit:    50, // one page; pass page.Next as Cursor for the next
	})
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		fmt.Printf("failed: %s at step %q: %s\n", r.Ref.RunID, r.FailedStep, r.Error)
	}
	// page.Next is empty on the last page; pass it as Cursor for the next one.
	// snippet:end

	// snippet:retry
	retrier, ok := cl.(workflow.RunRetrier)
	if !ok || !cl.Capabilities().Has(workflow.CapRetryRun) {
		return errors.New("this client cannot retry runs")
	}
	warehouseDown.Store(false) // the outage is over
	ref, err := retrier.Retry(ctx, failed)
	if err != nil {
		return err
	}
	// snippet:end
	if err := waitFor(ctx, c, ref, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Printf("retried %s: succeeded; charge ran %d time(s), ship %d\n", ref.RunID, charges.Load(), ships.Load())
	// snippet:retry-again
	_, err = retrier.Retry(ctx, ref) // it succeeded: only a failed run re-opens
	fmt.Println("retry a succeeded run: ErrWrongState", errors.Is(err, workflow.ErrWrongState))
	// snippet:end

	// snippet:delete
	deleter, ok := cl.(workflow.RunDeleter)
	if !ok || !cl.Capabilities().Has(workflow.CapDeleteRun) {
		return errors.New("this client cannot delete runs")
	}
	err = deleter.Delete(ctx, ref)
	_, statusErr := cl.Status(ctx, ref)
	fmt.Println("deleted:", err == nil, "then Status is ErrNotFound:", errors.Is(statusErr, workflow.ErrNotFound))
	// snippet:end

	for _, o := range []string{"o-2", "o-3"} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil-order", Payload: []byte(`"` + o + `"`), RunAt: start.Add(waitingDelay)}); err != nil {
			return err
		}
	}
	// snippet:cancel-all
	bulk, ok := cl.(workflow.BulkCanceller)
	if !ok || !cl.Capabilities().Has(workflow.CapCancelAll) {
		return errors.New("this client cannot cancel in bulk")
	}
	q := workflow.RunQuery{Workflow: "fulfil-order"} // no Statuses: runs not yet started
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return err // a terminal status, a Limit or a Cursor
	}
	n, err := bulk.CancelAll(ctx, q)
	if err != nil {
		return err
	}
	fmt.Println("cancelled waiting runs:", n)
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
	fmt.Println("while paused:", st)
	if err := c.Resume(ctx, workflow.PauseWorkflow("fulfil-order")); err != nil {
		return err
	}
	// snippet:end
	if err := waitFor(ctx, c, held, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Println("after Resume: succeeded")

	warehouseDown.Store(true) // a second outage, and this time the input was wrong too
	broken, err := cl.Enqueue(ctx, workflow.Job{Workflow: "fulfil-order", Payload: []byte(`"o-5"`)})
	if err != nil {
		return err
	}
	if err := waitFor(ctx, c, broken, workflow.StatusFailed); err != nil {
		return err
	}
	before := charges.Load()
	// snippet:reset
	if !workflow.Supports(cl, workflow.OpResetRun) { // no capability bit: ask Supports
		return errors.New("this client cannot reset runs")
	}
	resetter := cl.(workflow.RunResetter)
	fmt.Println("steps that run again:", workflow.StepsFrom(order, "charge"))
	warehouseDown.Store(false)
	again, err := resetter.ResetRun(ctx, broken, "charge") // not only "ship", the step that failed
	if err != nil {
		return err // ErrWrongState unless the run failed; ErrInvalid for an unknown step
	}
	// snippet:end
	if err := waitFor(ctx, c, again, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Printf("reset %s from charge: succeeded, same ref %v; charge ran %d more time(s)\n",
		again.RunID, again == broken, charges.Load()-before)
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
		case <-time.After(pollInterval):
		}
	}
}
