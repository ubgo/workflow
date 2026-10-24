// Command run-all shows memory.Client.RunAll, the test helper that runs
// everything now: a job delayed by a day, a step whose retry waits ten
// minutes and a step that sleeps a week all finish in one call on a fake
// clock nobody advances; a run waiting for an event is reported as
// waiting until the event is signalled; a paused workflow makes RunAll
// give up with ErrRunAllStalled instead of hanging.
//
// It is the program behind docs/testing/run-all.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
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

var errFlaky = errors.New("smtp: try again")

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "run-all:", err)
		os.Exit(1)
	}
}

// start is the fake clock's first instant.
var start = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start) // never advanced below

	c := memory.New(memory.WithClock(clk))
	var sends atomic.Int32
	wfs := []workflow.Workflow{
		{Name: "welcome", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
			if sends.Add(1) == 1 {
				return nil, errFlaky
			}
			return nil, nil
		}, Retry: &workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: 10 * time.Minute}}}}},
		{Name: "trial-ending", Steps: []workflow.Step{
			{Name: "wait-a-week", Sleep: 7 * 24 * time.Hour},
			{Name: "remind", After: []string{"wait-a-week"}, Run: noop},
		}},
		{Name: "approve", Steps: []workflow.Step{
			{Name: "approval", Wait: &workflow.WaitSpec{Event: "approved", Key: "order-7"}},
			{Name: "ship", After: []string{"approval"}, Run: noop},
		}},
		{Name: "report", Steps: []workflow.Step{{Name: "build", Run: noop}}},
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// snippet:runall
	jobs := []workflow.Job{
		{Workflow: "welcome", RunAt: clk.Now().Add(24 * time.Hour)}, // delayed a day
		{Workflow: "trial-ending"},                                  // sleeps a week
		{Workflow: "approve"},                                       // waits for an event
	}
	for _, job := range jobs {
		if _, err := c.Enqueue(ctx, job); err != nil {
			return err
		}
	}
	rep, err := c.RunAll(ctx) // returns when no run can move any further
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "succeeded=%d failed=%d waiting=%d sends=%d clock=%s\n",
		rep.Succeeded, rep.Failed, rep.Waiting, sends.Load(), clk.Now().Format("Jan 2 15:04"))
	// snippet:end

	// snippet:signal
	// RunAll cannot send the event a run waits for; the test does, then
	// runs everything again.
	if err := c.Signal(ctx, workflow.Event{Name: "approved", Key: "order-7"}); err != nil {
		return err
	}
	rep, err = c.RunAll(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "after the signal: succeeded=%d waiting=%d\n", rep.Succeeded, rep.Waiting)
	// snippet:end

	// snippet:stalled
	if err := c.Pause(ctx, workflow.PauseWorkflow("report")); err != nil {
		return err
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "report"}); err != nil {
		return err
	}
	rep, err = c.RunAll(ctx) // a paused run never moves: RunAll gives up
	fmt.Fprintf(out, "paused: stalled=%t succeeded=%d\n", errors.Is(err, memory.ErrRunAllStalled), rep.Succeeded)
	// snippet:end
	return nil
}

func noop(context.Context, []byte) ([]byte, error) { return nil, nil }
