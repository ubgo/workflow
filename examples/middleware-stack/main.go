// Command middleware-stack wraps every step and every enqueue of an
// in-memory driver with the stock middleware: the order a chain runs in,
// Logging with a level per attempt, Recover, Timeout, FailOn, Skip,
// ThrottleErrors with a store of its own, Route, Unique, and the refusal a
// package gives when its middleware is put in one chain twice.
//
// It is the program behind docs/middleware/step-middleware.md,
// docs/middleware/enqueue-middleware.md and docs/observability/logging.md:
// the guides copy its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// errBadInput is an error no retry can fix; FailOn makes it permanent.
var errBadInput = errors.New("bad input")

// errDown is what the flaky downstream returns while it is down.
var errDown = errors.New("payments api down")

// retries is each workflow's retry policy Max; the log level function
// compares the attempt with it, since middleware cannot see the policy.
var retries = map[string]int{"charge": 2, "validate": 2}

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "middleware-stack:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := showChainOrder(ctx, out); err != nil {
		return err
	}

	// The log lines go to out without the time and the random run ID, so
	// the output is the same on every run.
	logger := slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey || a.Key == "run_id" || a.Key == "stack" {
				return slog.Attr{}
			}
			return a
		},
	}))
	// Logging measures durations on this clock, and ThrottleErrors counts
	// its window and cooldown on it; a fake clock makes both exact.
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	// snippet:log-level
	level := func(_ context.Context, ev middleware.StepLogEvent) slog.Level {
		switch {
		case ev.Phase != middleware.StepFailed:
			return slog.LevelDebug
		case workflow.IsPermanent(ev.Err) || ev.Info.Attempt > retries[ev.Info.Workflow]:
			return slog.LevelError
		default:
			return slog.LevelWarn // a retry will follow
		}
	}
	logging := middleware.Logging(logger, clk, middleware.WithLogLevel(level))
	// snippet:end

	// snippet:throttle
	circuits := &announcingCircuit{MemoryCircuit: middleware.NewMemoryCircuit(clk), out: out}
	throttle, err := middleware.ThrottleErrors(middleware.ThrottleConfig{
		Clock:     clk,
		Store:     circuits, // nil means a MemoryCircuit of its own
		MaxErrors: 2,
		Window:    time.Minute,
		Cooldown:  5 * time.Minute,
	})
	if err != nil {
		return err
	}
	// snippet:end

	// snippet:stack
	c := memory.New(memory.WithLogger(logger), memory.WithMiddleware(
		logging, // outermost: it sees a recovered panic as a failed attempt
		middleware.Recover(logger),
		middleware.Timeout(100*time.Millisecond),
		middleware.Skip(func(_ context.Context, in []byte) bool { return string(in) == `"skip"` }),
		throttle, // circuits keyed by workflow name
		middleware.FailOn(func(err error) bool { return errors.Is(err, errBadInput) }), // inside throttle: bad input never trips a circuit
	))
	// snippet:end

	ends := make(chan workflow.RunEvent, 16)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	calls := map[string]int{}
	handler := func(name string, h workflow.Handler) workflow.Workflow {
		return workflow.Workflow{
			Name:  name,
			Retry: workflow.RetryPolicy{Max: retries[name]},
			Steps: []workflow.Step{{Name: "run", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				calls[name]++ // the memory driver runs these steps one at a time here
				return h(ctx, in)
			}}},
		}
	}
	flaky := 0
	for _, wf := range []workflow.Workflow{
		handler("charge", func(context.Context, []byte) ([]byte, error) {
			flaky++
			if flaky == 1 {
				return nil, errDown
			}
			return []byte(`"charged"`), nil
		}),
		handler("panics", func(context.Context, []byte) ([]byte, error) { panic("nil map") }),
		handler("validate", func(context.Context, []byte) ([]byte, error) {
			return nil, fmt.Errorf("field amount: %w", errBadInput)
		}),
		handler("slow", func(ctx context.Context, _ []byte) ([]byte, error) {
			<-ctx.Done() // Timeout is cooperative: the step must watch ctx
			return nil, ctx.Err()
		}),
		handler("call-api", func(context.Context, []byte) ([]byte, error) { return nil, errDown }),
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	runOne := func(cl workflow.Client, job workflow.Job) error {
		ref, err := cl.Enqueue(ctx, job)
		if err != nil {
			return err
		}
		for ev := range ends {
			if ev.Ref.RunID != ref.RunID {
				continue
			}
			fmt.Fprintf(out, "run %s: %s", job.Workflow, ev.Status)
			if ev.Err != nil {
				fmt.Fprintf(out, " (%v)", ev.Err)
			}
			fmt.Fprintf(out, ", %d handler call(s)\n", calls[job.Workflow])
			calls[job.Workflow] = 0
			return nil
		}
		return ctx.Err()
	}

	fmt.Fprintln(out, "-- step middleware")
	for _, job := range []workflow.Job{
		{Workflow: "charge", Payload: []byte(`"order-1"`)},
		{Workflow: "panics"},
		{Workflow: "validate"},
		{Workflow: "slow"},
		{Workflow: "charge", Payload: []byte(`"skip"`)},
	} {
		if err := runOne(c, job); err != nil {
			return err
		}
	}

	fmt.Fprintln(out, "-- throttle")
	for range 3 {
		if err := runOne(c, workflow.Job{Workflow: "call-api"}); err != nil {
			return err
		}
	}
	clk.Advance(5 * time.Minute) // the cooldown ends; the next call reaches the step again
	if err := runOne(c, workflow.Job{Workflow: "call-api"}); err != nil {
		return err
	}

	return enqueueMiddleware(ctx, out, c, runOne)
}

// announcingCircuit is a CircuitStore of the app's own: it keeps the
// circuits in a MemoryCircuit and prints when one opens. A store shared by
// every worker (a SQL table, Redis) implements the same three methods.
//
// snippet:circuit-store
type announcingCircuit struct {
	*middleware.MemoryCircuit
	out io.Writer
}

func (a *announcingCircuit) RecordError(ctx context.Context, key string, p middleware.CircuitPolicy) error {
	if err := a.MemoryCircuit.RecordError(ctx, key, p); err != nil {
		return err
	}
	open, err := a.IsOpen(ctx, key)
	if err != nil {
		return err
	}
	if open {
		fmt.Fprintf(a.out, "circuit %q open for %s\n", key, p.Cooldown)
	}
	return nil
}

// snippet:end

// showChainOrder prints the order Chain runs its middleware in.
func showChainOrder(ctx context.Context, out io.Writer) error {
	// snippet:chain
	tag := func(name string) workflow.Middleware {
		return func(next workflow.Handler) workflow.Handler {
			return func(ctx context.Context, in []byte) ([]byte, error) {
				fmt.Fprintln(out, name, "before")
				res, err := next(ctx, in)
				fmt.Fprintln(out, name, "after")
				return res, err
			}
		}
	}
	h := middleware.Chain(tag("a"), tag("b"))(func(context.Context, []byte) ([]byte, error) {
		fmt.Fprintln(out, "handler")
		return nil, nil
	})
	_, err := h(ctx, nil)
	// snippet:end
	return err
}

// enqueueMiddleware wraps the client's Enqueue: a middleware of the app's
// own, Route and Unique, then the refusal of Unique put in twice.
func enqueueMiddleware(ctx context.Context, out io.Writer, c workflow.Client, runOne func(workflow.Client, workflow.Job) error) error {
	fmt.Fprintln(out, "-- enqueue middleware")

	// snippet:enqueue-own
	audit := func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			ref, err := next(ctx, job)
			fmt.Fprintf(out, "enqueue %s tier=%q queue=%q err=%v\n", job.Workflow, job.Labels["tier"], job.Labels["queue"], err)
			return ref, err
		}
	}
	// snippet:end

	// snippet:wrap
	byOrder := func(j workflow.Job) string { return j.Labels["order"] }
	unique := middleware.Unique(byOrder, middleware.NewMemoryUnique(nil), time.Minute)
	client := middleware.WrapClient(c,
		middleware.Route(middleware.RouteRule{
			Match:  func(j workflow.Job) bool { return j.Labels["tier"] == "bulk" },
			Labels: map[string]string{"queue": "bulk"},
		}),
		audit, // sees the job after Route, before Unique
		unique,
	)
	// snippet:end

	if err := runOne(client, workflow.Job{Workflow: "charge", Payload: []byte(`"skip"`), Labels: map[string]string{"tier": "bulk", "order": "o-7"}}); err != nil {
		return err
	}
	// snippet:duplicate
	_, err := client.Enqueue(ctx, workflow.Job{Workflow: "charge", Labels: map[string]string{"order": "o-7"}})
	fmt.Fprintln(out, "same order again:", errors.Is(err, workflow.ErrDuplicate))
	// snippet:end

	// snippet:twice
	twice := middleware.WrapClient(middleware.WrapClient(c, unique), unique)
	_, err = twice.Enqueue(ctx, workflow.Job{Workflow: "charge", Labels: map[string]string{"order": "o-8"}})
	fmt.Fprintln(out, "unique twice:", errors.Is(err, workflow.ErrInvalid), err)
	// snippet:end
	return nil
}
