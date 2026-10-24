// Command retries shows how a failed step is tried again: an exponential
// backoff with jitter, a delay named by the error (a 429's Retry-After),
// errors that must not retry, a step that opts out of the workflow's
// default, and how a driver that cannot honour a policy refuses it.
//
// It runs on the in-memory driver with a fake clock, so a backoff of
// seconds or minutes passes in microseconds and the output is the same on
// every run. It is the program behind docs/jobs/retries.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// half is the jitter draw the example pins, so the jittered delays are the
// same on every run. Production leaves the source at its default
// (math/rand/v2).
func half() float64 { return 0.5 }

// errNotFound is an app error the "lookup" workflow's policy refuses to
// retry through NoRetryOn.
var errNotFound = errors.New("customer not found")

// attempt is what a step handler reports to the program, which prints it
// from one goroutine and moves the fake clock past the backoff.
type attempt struct {
	step   string
	n      int
	at     time.Time
	err    error
	policy workflow.RetryPolicy
}

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "retries:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	printBackoff(out)

	clk := clock.NewFake(start)
	attempts := make(chan attempt, 16)
	finished := make(chan workflow.RunEvent, 16)

	// step returns a handler that fails with fail(n) on attempt n, or
	// succeeds when fail returns nil, and reports every attempt.
	step := func(name string, policy workflow.RetryPolicy, fail func(n int) error) workflow.Handler {
		return func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			err := fail(info.Attempt)
			attempts <- attempt{step: name, n: info.Attempt, at: clk.Now(), err: err, policy: policy}
			return nil, err
		}
	}

	// snippet:policy
	policy := workflow.RetryPolicy{
		Max: 3, // three retries after the first attempt
		Backoff: workflow.Backoff{
			Initial: time.Second,
			Factor:  2,
			Max:     time.Minute,
			Jitter:  0.5, // each delay is drawn from [d/2, d]
		},
	}
	// snippet:end

	// snippet:retry-after
	rateLimited := workflow.RetryPolicy{
		Max:      2,
		Backoff:  workflow.Backoff{Initial: time.Second},
		DelayFor: workflow.RetryAfterDelay, // honour RetryAfter(err, d)
	}
	callAPI := func(n int) error {
		if n == 1 {
			// The API answered 429 with Retry-After: 30.
			return workflow.RetryAfter(errors.New("429 too many requests"), 30*time.Second)
		}
		return nil
	}
	// snippet:end

	// snippet:no-retry
	noRetry := workflow.RetryPolicy{
		Max:       5,
		Backoff:   workflow.Backoff{Initial: time.Second},
		NoRetryOn: func(err error) bool { return errors.Is(err, errNotFound) },
	}
	charge := func(int) error {
		return workflow.Permanent(errors.New("card declined")) // retrying cannot help
	}
	lookup := func(int) error { return fmt.Errorf("lookup: %w", errNotFound) }
	// snippet:end

	flaky := func(n int) error {
		if n < 3 {
			return errors.New("upstream returned 503")
		}
		return nil
	}

	// snippet:client
	c := memory.New(
		memory.WithClock(clk),
		memory.WithJitterSource(half), // pin the jitter draws for a repeatable demo
	)
	// snippet:end
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { finished <- ev }); err != nil {
		return err
	}

	// snippet:per-step
	signupDefault := workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Second}}
	signup := workflow.Workflow{
		Name:  "signup",
		Retry: signupDefault, // the default for every step that leaves Retry nil
		Steps: []workflow.Step{
			{Name: "create", Run: step("create", signupDefault, flaky)},
			{
				Name:  "welcome",
				After: []string{"create"},
				Retry: workflow.RetriesNone(), // a welcome email is sent once or not at all
				Run:   step("welcome", workflow.RetryPolicy{}, func(int) error { return errors.New("smtp refused") }),
			},
		},
	}
	// snippet:end

	for _, wf := range []workflow.Workflow{
		{Name: "sync", Retry: policy, Steps: []workflow.Step{{Name: "call", Run: step("call", policy, flaky)}}},
		{Name: "import", Retry: rateLimited, Steps: []workflow.Step{{Name: "fetch", Run: step("fetch", rateLimited, callAPI)}}},
		{Name: "charge", Retry: noRetry, Steps: []workflow.Step{{Name: "charge", Run: step("charge", noRetry, charge)}}},
		{Name: "lookup", Retry: noRetry, Steps: []workflow.Step{{Name: "lookup", Run: step("lookup", noRetry, lookup)}}},
		signup,
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}

	for _, wf := range []string{"sync", "import", "charge", "lookup", "signup"} {
		fmt.Fprintf(out, "\n%s:\n", wf)
		ev, err := drive(ctx, c, clk, wf, attempts, finished, out)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "  run %s", ev.Status)
		if ev.Err != nil {
			fmt.Fprintf(out, " (permanent: %t): %v", workflow.IsPermanent(ev.Err), ev.Err)
		}
		fmt.Fprintln(out)
	}

	return refusals(out)
}

// drive enqueues one run of wf and prints each attempt. After a failed
// attempt the policy allows to retry, it waits for the driver to arm the
// backoff timer and moves the fake clock by exactly the delay the policy
// computes, which is the delay the driver computed too: both call
// RetryPolicy.NextDelay with the same jitter source.
func drive(ctx context.Context, c workflow.Client, clk *clock.Fake, wf string,
	attempts <-chan attempt, finished <-chan workflow.RunEvent, out io.Writer) (workflow.RunEvent, error) {
	t0 := clk.Now()
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf}); err != nil {
		return workflow.RunEvent{}, err
	}
	for {
		select {
		case a := <-attempts:
			fmt.Fprintf(out, "  %s attempt %d at +%s", a.step, a.n, a.at.Sub(t0))
			if a.err == nil {
				fmt.Fprintln(out, ": ok")
				continue
			}
			fmt.Fprintf(out, ": %v\n", a.err)
			if !a.policy.Retryable(a.err, a.n) {
				continue
			}
			// snippet:next-delay
			d := a.policy.NextDelay(a.err, a.n, half)
			// snippet:end
			if err := clk.BlockUntil(ctx, 1); err != nil {
				return workflow.RunEvent{}, err
			}
			clk.Advance(d)
		case ev := <-finished:
			return ev, nil
		case <-ctx.Done():
			return workflow.RunEvent{}, ctx.Err()
		}
	}
}

// printBackoff prints the delays a policy produces, with and without
// jitter. Delay and JitteredDelay are pure functions, so this needs no
// client.
func printBackoff(out io.Writer) {
	// snippet:delays
	b := workflow.Backoff{Initial: time.Second, Factor: 2, Max: 10 * time.Second, Jitter: 0.5}
	for n := 1; n <= 5; n++ {
		// JitteredDelay takes the uniform draw in [0, 1) as an argument.
		fmt.Fprintf(out, "retry %d: delay %s, with draw 0.5: %s\n", n, b.Delay(n), b.JitteredDelay(n, 0.5))
	}
	// snippet:end
}

// refusals shows that a driver which cannot honour a retry policy refuses
// it when the workflow is registered, instead of retrying differently.
// The Hatchet client connects only at Start, so Register runs without a
// server.
func refusals(out io.Writer) error {
	fmt.Fprintln(out, "\nrefusals:")
	// snippet:refusal
	wf := workflow.Workflow{
		Name: "sync",
		Retry: workflow.RetryPolicy{
			Max:     3,
			Backoff: workflow.Backoff{Initial: 2 * time.Second, Factor: 2, Jitter: 0.5},
		},
		Steps: []workflow.Step{{Name: "call", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	}
	fmt.Fprintln(out, "  needs:", workflow.Requires(wf))
	fmt.Fprintln(out, "  memory:", memory.New().Register(wf))
	err := hatchet.New(hatchet.Config{}).Register(wf)
	fmt.Fprintln(out, "  hatchet:", err, "| unsupported:", errors.Is(err, workflow.ErrUnsupported))

	wf.Retry.Backoff.Jitter = 0
	wf.Retry.DelayFor = workflow.RetryAfterDelay
	fmt.Fprintln(out, "  hatchet:", hatchet.New(hatchet.Config{}).Register(wf))
	// snippet:end
	return nil
}
