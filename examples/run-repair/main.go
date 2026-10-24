// Command run-repair shows what an operator has to fix a run by hand, on
// the in-memory driver: the error of every failed attempt kept on the run
// (with a stack for a panic caught by middleware.Recover), a failed run
// marked succeeded after the work was done by hand, and a stuck run marked
// failed with a reason, without a retry and without its OnFailure handler.
//
// It is the program behind the "Error history" and "Force a run to an end"
// sections of docs/jobs/manage-runs.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "run-repair:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	// snippet:client
	c := memory.New(
		memory.WithErrorHistory(10), // keep the newest 10 failed attempts per run; 0 turns it off
		memory.WithMiddleware(middleware.Recover(slog.New(slog.DiscardHandler))),
	)
	// snippet:end
	if !c.Capabilities().Has(workflow.CapErrorHistory | workflow.CapForceRun) {
		return errors.New("the driver lacks error history or force")
	}

	// send fails twice, then panics: Recover turns the panic into an
	// error that carries its stack.
	var attempt atomic.Int32
	err := c.Register(workflow.Workflow{
		Name:  "send-email",
		Retry: workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: time.Millisecond}},
		Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
			if attempt.Add(1) < 3 {
				return nil, errors.New("smtp 451: try again later")
			}
			panic("nil template")
		}}},
	})
	if err != nil {
		return err
	}
	var onFailure atomic.Int32
	started := make(chan struct{}, 1)
	err = c.Register(workflow.Workflow{
		Name: "import-feed",
		Steps: []workflow.Step{{Name: "fetch", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			started <- struct{}{}
			<-ctx.Done() // stuck on a feed that never answers
			return nil, context.Cause(ctx)
		}}},
		OnFailure: func(context.Context, []byte) ([]byte, error) {
			onFailure.Add(1)
			return nil, nil
		},
	})
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer c.Stop()

	fmt.Fprintln(out, "-- error history")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-email"})
	if err != nil {
		return err
	}
	if err := waitStatus(ctx, c, ref, workflow.StatusFailed); err != nil {
		return err
	}
	// snippet:history
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	for _, e := range page.Runs[0].Errors { // oldest first
		fmt.Fprintf(out, "  attempt %d of %q: %s (stack: %t)\n", e.Attempt, e.Step, e.Error, e.Stack != "")
	}
	// snippet:end

	fmt.Fprintln(out, "-- force complete a failed run")
	// snippet:complete
	// The email went out by hand: record the run as done.
	if err := c.ForceComplete(ctx, ref); err != nil {
		return err
	}
	// snippet:end
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "  send-email:", st)

	fmt.Fprintln(out, "-- force fail a stuck run")
	stuck, err := c.Enqueue(ctx, workflow.Job{Workflow: "import-feed"})
	if err != nil {
		return err
	}
	<-started
	// snippet:fail
	err = c.ForceFail(ctx, stuck, "feed provider is down until Monday")
	if err != nil {
		return err
	}
	// snippet:end
	if err := waitStatus(ctx, c, stuck, workflow.StatusFailed); err != nil {
		return err
	}
	page, err = c.Runs(ctx, workflow.RunQuery{Ref: stuck})
	if err != nil {
		return err
	}
	r := page.Runs[0]
	fmt.Fprintf(out, "  import-feed: %s, %q, OnFailure ran %d times\n", r.Status, r.Error, onFailure.Load())

	fmt.Fprintln(out, "-- a finished run cannot be forced")
	err = c.ForceFail(ctx, ref, "too late")
	fmt.Fprintln(out, "  ErrWrongState:", errors.Is(err, workflow.ErrWrongState))
	return nil
}

// waitStatus polls until the run reaches want.
func waitStatus(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s is %s, want %s", ref.RunID, st, want)
		}
		time.Sleep(time.Millisecond)
	}
}
