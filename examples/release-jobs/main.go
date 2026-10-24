// Command release-jobs puts a step back for a while instead of running it
// (middleware.Release), and frees a unique job's
// lock as soon as its run starts rather than when it ends
// (middleware.UniqueUntilStarted).
// Both run on the in-memory driver with a fake clock, so the output never
// varies.
//
// It is the program behind the "Release a step for later" section of
// docs/middleware/step-middleware.md and the "Unique until a step starts"
// section of docs/middleware/enqueue-middleware.md: the guides copy its
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
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "release-jobs:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "-- release while the API is in maintenance")
	if err := release(ctx, out); err != nil {
		return err
	}
	fmt.Fprintln(out, "-- unique until a step starts")
	return untilStarted(ctx, out)
}

// release runs a sync step that is put back for ten minutes while a
// maintenance flag is set, once until the flag clears and once until the
// retries run out.
func release(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC))
	// snippet:release
	var maintenance atomic.Bool // set by the app while the partner API is down
	inMaintenance := func(context.Context, []byte) bool { return maintenance.Load() }

	c := memory.New(memory.WithClock(clk), memory.WithMiddleware(
		attempts(out), // outermost, so it sees each release as a failed attempt
		middleware.Release(inMaintenance, 10*time.Minute),
	))
	err := c.Register(workflow.Workflow{
		Name: "sync",
		Retry: workflow.RetryPolicy{
			Max:      3, // a release is an attempt: allow a few
			Backoff:  workflow.Backoff{Initial: time.Hour},
			DelayFor: workflow.RetryAfterDelay, // honour the release's 10 minutes, not the hour
		},
		Steps: []workflow.Step{{Name: "push", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, nil
		}}},
	})
	// snippet:end
	if err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer c.Stop()

	maintenance.Store(true)
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync"})
	if err != nil {
		return err
	}
	if err := waitTimer(clk); err != nil { // attempt 1 was released
		return err
	}
	clk.Advance(10 * time.Minute)
	if err := waitTimer(clk); err != nil { // attempt 2 was released
		return err
	}
	maintenance.Store(false)
	fmt.Fprintln(out, "maintenance over at", clk.Now().Format("15:04"))
	clk.Advance(10 * time.Minute)
	st, err := waitDone(ctx, c, ref)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "run: %s at %s\n", st, clk.Now().Format("15:04"))

	// snippet:release-runs-out
	maintenance.Store(true)
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "sync"})
	if err != nil {
		return err
	}
	for range 3 { // three retries, each released again
		if err := waitTimer(clk); err != nil {
			return err
		}
		clk.Advance(10 * time.Minute)
	}
	st, err = waitDone(ctx, c, ref)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "run: %s at %s\n", st, clk.Now().Format("15:04"))
	// snippet:end
	return nil
}

// attempts prints each attempt's outcome. It reads the delay a release
// asked for with workflow.RetryAfterDelay, the same call the retry policy
// makes.
func attempts(out io.Writer) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			res, err := next(ctx, in)
			switch d, _ := workflow.RetryAfterDelay(err, info.Attempt); {
			case errors.Is(err, middleware.ErrReleased):
				fmt.Fprintf(out, "attempt %d: released for %s\n", info.Attempt, d)
			case err != nil:
				fmt.Fprintf(out, "attempt %d: %v\n", info.Attempt, err)
			default:
				fmt.Fprintf(out, "attempt %d: ran\n", info.Attempt)
			}
			return res, err
		}
	}
}

// untilStarted shows the lock of a delayed job: held while the run waits,
// free once its step has started, though the step is still running.
func untilStarted(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC))
	// snippet:until-started
	byReport := func(j workflow.Job) string { return "report:" + j.Labels["account"] }
	store := middleware.NewMemoryUnique(clk) // a shared SQL store across processes
	uniqueEnqueue, uniqueStep := middleware.UniqueUntilStarted(byReport, store, time.Hour)

	mem := memory.New(memory.WithClock(clk), memory.WithMiddleware(uniqueStep)) // the worker's half
	c := middleware.WrapClient(mem, uniqueEnqueue)                              // the enqueuer's half
	// snippet:end

	started, finish := make(chan struct{}, 1), make(chan struct{})
	err := c.Register(workflow.Workflow{Name: "report", Steps: []workflow.Step{{Name: "build", Run: func(context.Context, []byte) ([]byte, error) {
		started <- struct{}{}
		<-finish // a long build
		return nil, nil
	}}}})
	if err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer mem.Stop()
	defer close(finish)

	verdict := func(name string, err error) {
		switch {
		case err == nil:
			fmt.Fprintf(out, "%s: accepted\n", name)
		case errors.Is(err, workflow.ErrDuplicate):
			fmt.Fprintf(out, "%s: refused (ErrDuplicate)\n", name)
		default:
			fmt.Fprintf(out, "%s: %v\n", name, err)
		}
	}
	// snippet:until-started-run
	job := workflow.Job{Workflow: "report", Labels: map[string]string{"account": "a-1"}, RunAt: clk.Now().Add(5 * time.Minute)}
	_, err = c.Enqueue(ctx, job)
	verdict("first, runs in 5 minutes", err)
	_, err = c.Enqueue(ctx, job)
	verdict("second, while the first waits", err)

	clk.Advance(5 * time.Minute)
	<-started // the first run's step is running now
	job.RunAt = time.Time{}
	_, err = c.Enqueue(ctx, job)
	verdict("third, while the first runs", err)
	// snippet:end
	return nil
}

// waitTimer waits until the driver has armed exactly one timer (a retry
// or a delayed start), so Advance never runs ahead of the driver.
func waitTimer(clk *clock.Fake) error {
	deadline := time.Now().Add(5 * time.Second)
	for clk.Pending() != 1 {
		if time.Now().After(deadline) {
			return errors.New("no timer armed in 5s")
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// waitDone polls the run's status until it is terminal.
func waitDone(ctx context.Context, c workflow.Client, ref workflow.RunRef) (workflow.Status, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st.IsTerminal() {
			return st, err
		}
		if time.Now().After(deadline) {
			return st, errors.New("run did not finish in 5s")
		}
		time.Sleep(time.Millisecond)
	}
}
