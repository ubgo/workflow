// Command lifecycle-events watches runs while they happen: the lifecycle
// feed of the memory driver (inserted, started, retrying, progress,
// finished) on a fake clock, the RunError a failed run carries, read with
// workflow.RunErrorOf, and middleware.Stuck reporting an attempt that runs
// far longer than expected without touching it.
//
// It is the program behind docs/observability/lifecycle.md and
// docs/observability/stuck-jobs.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
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
	"github.com/ubgo/workflow/middleware"
)

var (
	errBusy     = errors.New("database busy")
	errDeclined = errors.New("card declined")
)

// start is the fake clock's first instant, so every printed time is fixed.
var start = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "lifecycle-events:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := lifecycle(ctx, out); err != nil {
		return err
	}
	return stuck(ctx, out)
}

// lifecycle runs an export that fails once, reports progress and succeeds,
// and a charge that fails for good, printing every lifecycle event and the
// failed run's RunError.
func lifecycle(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk), memory.WithNodeID("worker-1"))

	var calls atomic.Int32
	err := c.Register(workflow.Workflow{
		Name: "export",
		Steps: []workflow.Step{{
			Name:  "rows",
			Retry: &workflow.RetryPolicy{Max: 2}, // no backoff: the retry runs at once
			Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				if calls.Add(1) == 1 {
					return nil, errBusy
				}
				if err := workflow.ReportProgress(ctx, workflow.Progress{Percent: 50, Message: "500 of 1000 rows"}); err != nil {
					return nil, err
				}
				return []byte(`"export.csv"`), nil
			},
		}},
	})
	if err != nil {
		return err
	}
	err = c.Register(workflow.Workflow{
		Name: "charge",
		Steps: []workflow.Step{{Name: "card", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, workflow.Permanent(errDeclined)
		}}},
	})
	if err != nil {
		return err
	}

	// snippet:observe
	events := make(chan workflow.LifecycleEvent, 32)
	ln, ok := workflow.Client(c).(workflow.LifecycleNotifier)
	if !ok {
		return errors.New("this driver has no lifecycle feed")
	}
	// Register before Start. The observer runs on a driver goroutine:
	// return quickly, hand anything slow to a channel.
	if err := ln.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) {
		events <- ev
	}); err != nil {
		return err
	}
	// snippet:end

	// snippet:runerror
	failures := make(chan string, 1)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
		if ev.Status != workflow.StatusFailed {
			return
		}
		re, ok := workflow.RunErrorOf(ev.Err) // which run, workflow, step and attempt failed
		if !ok {
			failures <- "failed: " + ev.Err.Error()
			return
		}
		failures <- fmt.Sprintf("failed: %s\n  step=%s attempt=%d declined=%t\n  Error(): %s",
			re.Describe(), re.Step, re.Attempt, errors.Is(ev.Err, errDeclined), ev.Err)
	}); err != nil {
		return err
	}
	// snippet:end

	if err := c.Start(ctx); err != nil {
		return err
	}
	fmt.Fprintln(out, "-- lifecycle")
	for _, wf := range []string{"export", "charge"} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf}); err != nil {
			return err
		}
		for ev := range events {
			fmt.Fprintln(out, " ", describe(ev))
			if ev.Kind == workflow.LifecycleFinished {
				break
			}
		}
	}
	fmt.Fprintln(out, "-- run error")
	fmt.Fprintln(out, " ", <-failures)
	return nil
}

// describe is one lifecycle event on one line.
func describe(ev workflow.LifecycleEvent) string {
	s := fmt.Sprintf("%s %-9s %s", ev.At.Format("15:04:05"), ev.Kind, ev.Workflow)
	if ev.Attempt > 0 {
		s += fmt.Sprintf(" step=%s attempt=%d", ev.Step, ev.Attempt)
	}
	s += fmt.Sprintf(" status=%s node=%s", ev.Status, ev.Node)
	if ev.Progress != nil {
		s += fmt.Sprintf(" progress=%.0f%% %q", ev.Progress.Percent, ev.Progress.Message)
	}
	if ev.Err != nil {
		s += " err=" + ev.Err.Error()
	}
	return s
}

// stuck runs a step that hangs until released and shows Stuck reporting it
// once, after six minutes on its own fake clock, while it keeps running.
func stuck(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	watch := clock.NewFake(start)
	reports := make(chan middleware.StuckEvent, 1)

	// snippet:stuck
	mw := middleware.Stuck(middleware.StuckConfig{
		// After decides for every step; without it, Default applies to all.
		After: func(info workflow.StepInfo) time.Duration {
			if info.Workflow == "nightly-import" {
				return 2 * time.Hour // a step known to be slow
			}
			return 5 * time.Minute // zero or less would mean: never report
		},
		OnStuck: func(_ context.Context, ev middleware.StuckEvent) {
			// Report only: the attempt keeps running. Page someone, log,
			// or count it; a Timeout is what ends an attempt.
			reports <- ev
		},
		Clock: watch,
	})
	c := memory.New(memory.WithMiddleware(mw), memory.WithNodeID("worker-1"))
	// snippet:end

	release := make(chan struct{})
	finished := make(chan workflow.Status, 1)
	err := c.Register(workflow.Workflow{
		Name: "report",
		Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			select {
			case <-release: // a hung call that finally answers
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}},
	})
	if err != nil {
		return err
	}
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { finished <- ev.Status }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "report"}); err != nil {
		return err
	}
	if err := watch.BlockUntil(ctx, 1); err != nil { // the attempt started and Stuck armed its timer
		return err
	}
	watch.Advance(6 * time.Minute)
	ev := <-reports
	fmt.Fprintln(out, "-- stuck")
	fmt.Fprintf(out, "  %s step=%s attempt=%d node=%s started=%s running=%s\n",
		ev.Info.Workflow, ev.Info.Step, ev.Info.Attempt, ev.Info.Node, ev.Started.Format("15:04"), ev.Running)
	close(release)
	fmt.Fprintln(out, "  the attempt kept running and ended", <-finished)
	return nil
}
