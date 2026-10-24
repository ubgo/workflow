// Command failover joins two clients into one: jobs go to the primary, and
// to the secondary while the primary is unavailable. It shows the key
// requirement, run references that name their backend, the cool-off, the
// observer, and how the classifier decides what counts as an outage.
//
// Both backends are the in-memory driver here; in production the primary
// is typically Hatchet and the secondary a sqlstore client.
//
// It is the program behind docs/reliability/failover.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// statusPoll is how often waitTerminal re-reads a run's status: short, so
// the demo finishes quickly; a real app waits on the run-finished hook.
const statusPoll = 5 * time.Millisecond

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "failover:", err)
		os.Exit(1)
	}
}

var email = workflow.Workflow{Name: "email", Steps: []workflow.Step{{
	Name: "send",
	Run:  func(ctx context.Context, in []byte) ([]byte, error) { return nil, nil },
}}}

// switchable wraps a memory client so it fails every enqueue with fail()
// while down is set: a stand-in for an engine outage.
func switchable(down *atomic.Bool, fail func() error) workflow.Client {
	return middleware.WrapClient(memory.New(), func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if down.Load() {
				return workflow.RunRef{}, fail()
			}
			return next(ctx, job)
		}
	})
}

// side is the backend a RunRef names: the part before the first colon.
func side(ref workflow.RunRef) string {
	s, _, _ := strings.Cut(string(ref.RunID), ":")
	return s
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))
	var down atomic.Bool
	// A driver marks its outages with workflow.Unavailable.
	primary := switchable(&down, func() error { return workflow.Unavailable(errors.New("connection refused")) })
	secondary := memory.New()

	fmt.Fprintln(out, "-- Fail over to a second backend")
	// snippet:new
	c := failover.New(primary, secondary,
		failover.WithCoolOff(2, time.Minute), // skip the primary for 1m after 2 failovers in a row
		failover.WithClock(clk),
		failover.WithObserver(func(ctx context.Context, ev failover.Event) {
			fmt.Fprintln(out, "  event:", ev.Kind, ev.Workflow)
		}))
	if err := c.Register(email); err != nil { // registers on both backends
		return err
	}
	if err := c.Start(ctx); err != nil { // starts both
		return err
	}
	// snippet:end

	// snippet:key-required
	_, err := c.Enqueue(ctx, workflow.Job{Workflow: "email"})
	fmt.Fprintln(out, "no key:", errors.Is(err, failover.ErrKeyRequired), errors.Is(err, workflow.ErrInvalid))
	// snippet:end

	enqueue := func(key string) (workflow.RunRef, error) {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "email", IdempotencyKey: key})
		if err == nil {
			fmt.Fprintf(out, "%s -> %s\n", key, side(ref))
		}
		return ref, err
	}
	if _, err := enqueue("order-1"); err != nil {
		return err
	}
	fmt.Fprintln(out, "-- the primary goes down")
	down.Store(true)
	ref, err := enqueue("order-2")
	if err != nil {
		return err
	}
	if _, err := enqueue("order-3"); err != nil { // the second failover in a row
		return err
	}
	if _, err := enqueue("order-4"); err != nil { // cooling off: not even tried
		return err
	}

	// snippet:status
	st, err := waitTerminal(ctx, c, ref) // the ref names its backend, so Status asks the secondary
	// snippet:end
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "order-2 on the secondary:", st)

	fmt.Fprintln(out, "-- the primary is back and the cool-off ends")
	down.Store(false)
	clk.Advance(time.Minute)
	if _, err := enqueue("order-5"); err != nil {
		return err
	}
	fmt.Fprintln(out, "capabilities include Cron:", c.Capabilities().Has(workflow.CapCron))

	fmt.Fprintln(out, "-- Decide what counts as an outage")
	return classifiers(ctx, out)
}

// classifiers sends one job whose primary fails with an unmarked error:
// DefaultClassifier fails over, StrictClassifier returns the error.
func classifiers(ctx context.Context, out io.Writer) error {
	for _, tc := range []struct {
		name string
		opts []failover.Option
	}{
		{"default", nil},
		{"strict", []failover.Option{failover.WithClassifier(failover.StrictClassifier)}},
	} {
		var down atomic.Bool
		down.Store(true)
		// An error no driver marked as an outage: a bug or an unknown failure.
		primary := switchable(&down, func() error { return errors.New("unexpected reply") })
		c := failover.New(primary, memory.New(), tc.opts...)
		if err := c.Register(email); err != nil {
			return err
		}
		if err := c.Start(ctx); err != nil {
			return err
		}
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "email", IdempotencyKey: "order-9"})
		if err != nil {
			fmt.Fprintf(out, "%s classifier: returned %q\n", tc.name, err)
			continue
		}
		fmt.Fprintf(out, "%s classifier: failed over to %s\n", tc.name, side(ref))
	}
	return nil
}

// waitTerminal polls a run's status until it is terminal.
func waitTerminal(ctx context.Context, c workflow.Client, ref workflow.RunRef) (workflow.Status, error) {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st.IsTerminal() {
			return st, err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(statusPoll):
		}
	}
}
