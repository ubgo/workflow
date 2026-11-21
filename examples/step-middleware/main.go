// Command step-middleware shows the four small step middleware of the
// middleware package one at a time, each on a client of its own on the
// in-memory driver: Recover turns a panic into a failed attempt, Timeout
// caps every attempt, FailOn makes chosen errors permanent, and Skip
// completes a step without calling it.
//
// It is the program behind the Recover, Timeout, FailOn and Skip sections
// of docs/middleware/step-middleware.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"bytes"
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

// stepTimeout is the cap the Timeout section puts on every attempt. It is
// short so the program ends fast; an app sets the longest any of its
// steps may legitimately take.
const stepTimeout = 50 * time.Millisecond

// wellBefore is the bound the timeout section checks the run's real time
// against: far above stepTimeout, far below the hour the step would wait.
const wellBefore = time.Minute

// fastRetry is the backoff of the workflows that retry, short so the
// program ends fast; the delay itself is not what the sections show.
var fastRetry = workflow.Backoff{Initial: time.Millisecond}

// errCardDeclined is an error no retry can fix: the FailOn section makes
// it permanent for every workflow on the client.
var errCardDeclined = errors.New("card declined")

// errGatewayBusy is a passing error the FailOn section leaves retryable.
var errGatewayBusy = errors.New("gateway busy")

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "step-middleware:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code
// and reads what it wrote to w.
func run(ctx context.Context, w io.Writer) error {
	for _, section := range []func(context.Context, io.Writer) error{recoverPanics, timeOut, failOn, skip} {
		if err := section(ctx, w); err != nil {
			return err
		}
	}
	return nil
}

// recoverPanics shows middleware.Recover: the step panics, the attempt
// fails with an error wrapping middleware.ErrPanic, and the retry policy
// decides not to retry it.
func recoverPanics(ctx context.Context, w io.Writer) error {
	fmt.Fprintln(w, "-- recover")
	// The logger drops the time and the stack only so the output is the
	// same on every run; an app keeps both.
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{ReplaceAttr: dropAttrs(slog.TimeKey, "stack")}))
	var attempts atomic.Int32

	// snippet:recover
	c := memory.New(memory.WithMiddleware(middleware.Recover(logger)))
	err := c.Register(workflow.Workflow{
		Name: "import",
		Retry: workflow.RetryPolicy{
			Max:     3, // what an outage would get
			Backoff: fastRetry,
			// A panic is a bug, not an outage: fail at once instead of
			// spending the three retries.
			NoRetryOn: func(err error) bool { return errors.Is(err, middleware.ErrPanic) },
		},
		Steps: []workflow.Step{{Name: "parse", Run: func(context.Context, []byte) ([]byte, error) {
			attempts.Add(1)
			var rows map[string]int
			rows["total"]++ // panics: assignment to entry in nil map
			return nil, nil
		}}},
	})
	// snippet:end
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runJob, err := start(ctx, c)
	if err != nil {
		return err
	}
	ref, ev, err := runJob(workflow.Job{Workflow: "import"})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "run: %s after %d attempt(s): %v\n", ev.Status, attempts.Load(), ev.Err)

	// snippet:recover-stack
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	for _, e := range page.Runs[0].Errors {
		// The stack is kept apart from the message, never inside it.
		fmt.Fprintf(w, "history: attempt %d %q, stack kept: %t\n", e.Attempt, e.Error, e.Stack != "")
	}
	// snippet:end
	return nil
}

// timeOut shows middleware.Timeout: a step that waits on its context is
// stopped after stepTimeout, on every workflow of the client.
func timeOut(ctx context.Context, w io.Writer) error {
	fmt.Fprintln(w, "-- timeout")
	// snippet:timeout
	c := memory.New(memory.WithMiddleware(middleware.Timeout(stepTimeout)))
	err := c.Register(workflow.Workflow{
		Name: "fetch-feed",
		Steps: []workflow.Step{{Name: "download", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			select {
			case <-time.After(time.Hour): // a feed server that never answers
				return []byte("feed"), nil
			case <-ctx.Done(): // the timeout is cooperative: watch ctx
				return nil, ctx.Err()
			}
		}}},
	})
	// snippet:end
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runJob, err := start(ctx, c)
	if err != nil {
		return err
	}
	start := time.Now()
	_, ev, err := runJob(workflow.Job{Workflow: "fetch-feed"})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "run: %s: %v\n", ev.Status, ev.Err)
	fmt.Fprintf(w, "stopped well before the hour: %t\n", time.Since(start) < wellBefore)
	return nil
}

// failOn shows middleware.FailOn: a declined card fails at once, while a
// busy gateway still gets its retries.
func failOn(ctx context.Context, w io.Writer) error {
	fmt.Fprintln(w, "-- fail on")
	var attempts atomic.Int32
	// snippet:fail-on
	// declined picks the errors FailOn makes permanent.
	declined := func(err error) bool { return errors.Is(err, errCardDeclined) }
	c := memory.New(memory.WithMiddleware(middleware.FailOn(declined)))
	err := c.Register(workflow.Workflow{
		Name:  "charge",
		Retry: workflow.RetryPolicy{Max: 2, Backoff: fastRetry}, // two retries for a passing error
		Steps: []workflow.Step{{Name: "card", Run: func(_ context.Context, in []byte) ([]byte, error) {
			attempts.Add(1)
			if string(in) == "declined" {
				return nil, fmt.Errorf("charge order: %w", errCardDeclined) // permanent: no retry
			}
			return nil, errGatewayBusy // retried twice, as the policy says
		}}},
	})
	// snippet:end
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runJob, err := start(ctx, c)
	if err != nil {
		return err
	}
	for _, payload := range []string{"declined", "busy"} {
		attempts.Store(0)
		_, ev, err := runJob(workflow.Job{Workflow: "charge", Payload: []byte(payload)})
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s: %s after %d attempt(s)\n", payload, ev.Status, attempts.Load())
	}
	return nil
}

// skip shows middleware.Skip: the notify step is not called for a run
// whose payload asks for quiet, and the run still succeeds.
func skip(ctx context.Context, w io.Writer) error {
	fmt.Fprintln(w, "-- skip")
	var sent atomic.Int32
	// snippet:skip
	// quiet picks the step calls Skip completes without running them.
	quiet := func(ctx context.Context, in []byte) bool {
		info, _ := workflow.StepInfoFrom(ctx) // which step is about to run
		return info.Step == "notify" && bytes.Contains(in, []byte(`"quiet":true`))
	}
	c := memory.New(memory.WithMiddleware(middleware.Skip(quiet)))
	err := c.Register(workflow.Workflow{
		Name: "ship",
		Steps: []workflow.Step{{Name: "notify", Run: func(context.Context, []byte) ([]byte, error) {
			sent.Add(1)
			return nil, nil
		}}},
	})
	// snippet:end
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runJob, err := start(ctx, c)
	if err != nil {
		return err
	}
	for _, payload := range []string{`{"order":1}`, `{"order":2,"quiet":true}`} {
		sent.Store(0)
		_, ev, err := runJob(workflow.Job{Workflow: "ship", Payload: []byte(payload)})
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s: %s, notify called %d time(s)\n", payload, ev.Status, sent.Load())
	}
	return nil
}

// runner enqueues a job and returns its ref and the event that ended
// the run.
type runner func(job workflow.Job) (workflow.RunRef, workflow.RunEvent, error)

// start starts c until ctx ends and returns a runner for it. Each
// section starts its own client once, after registering its workflow.
func start(ctx context.Context, c *memory.Client) (runner, error) {
	ended := make(chan workflow.RunEvent, 1)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ended <- ev }); err != nil {
		return nil, err
	}
	if err := c.Start(ctx); err != nil {
		return nil, err
	}
	return func(job workflow.Job) (workflow.RunRef, workflow.RunEvent, error) {
		ref, err := c.Enqueue(ctx, job)
		if err != nil {
			return workflow.RunRef{}, workflow.RunEvent{}, err
		}
		for {
			select {
			case ev := <-ended:
				if ev.Ref.RunID == ref.RunID {
					return ref, ev, nil
				}
			case <-ctx.Done():
				return workflow.RunRef{}, workflow.RunEvent{}, ctx.Err()
			}
		}
	}, nil
}

// dropAttrs returns a slog ReplaceAttr that removes the named keys.
func dropAttrs(keys ...string) func([]string, slog.Attr) slog.Attr {
	return func(_ []string, a slog.Attr) slog.Attr {
		for _, k := range keys {
			if a.Key == k {
				return slog.Attr{}
			}
		}
		return a
	}
}
