// Command flow-replay shows deterministic replay: a refund written as one
// plain Go function with local variables, which waits three days for the
// parcel, then for an approver, then a day before closing the ticket,
// without being split into steps and without holding a worker while it
// waits. Every call that touches the world goes through the replay
// context and is recorded; on each wake the function runs again from the
// top, the recorded calls return their results without running again,
// and the code carries on where it stopped. A second version of the code
// branches on a value it did not record and fails with the
// nondeterminism error.
//
// It runs on the in-memory driver with a fake clock, so the three days
// pass in microseconds and the output is the same on every run.
//
// It is the program behind docs/workflows/replay.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "flow-replay:", err)
		os.Exit(1)
	}
}

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// counter counts how often each piece of code ran.
type counter struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *counter) add(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[name]++
}

func (c *counter) get(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[name]
}

// order is the refund workflow's payload.
type order struct {
	ID string `json:"id"`
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:client
	clk := clock.NewFake(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)) // clock.Real{} in production
	journal := workflow.NewMemoryJournal()                            // the run's history
	c, err := workflow.Use(memory.New(memory.WithClock(clk)), workflow.Journal(journal))
	if err != nil {
		return err
	}
	// snippet:end
	promises := workflow.NewMemoryPromises()
	var calls counter

	refund, err := refundWorkflow(c, clk, promises, &calls)
	if err != nil {
		return err
	}
	var creditFirst atomic.Bool
	drift, err := driftWorkflow(c, clk, &creditFirst)
	if err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{refund, drift} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	// On an in-process driver the run-finished hook carries the step's
	// error itself, so errors.As finds the NondeterminismError.
	ended := make(chan error, 1)
	if err := c.(workflow.RunNotifier).OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
		if ev.Workflow == "refund-drift" {
			ended <- ev.Err
		}
	}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	if err := refundRun(ctx, c, clk, promises, &calls); err != nil {
		return err
	}
	if err := driftRun(ctx, c, clk, &creditFirst, ended); err != nil {
		return err
	}

	// The Journal plugin forgets a run's history when the run finishes.
	for range 500 {
		if len(journal.Runs()) == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	fmt.Printf("journal:\n  runs holding history after both runs finished: %d\n", len(journal.Runs()))
	return nil
}

// refundWorkflow is the refund as one replayed handler.
func refundWorkflow(c workflow.Client, clk clock.Clock, promises workflow.PromiseStore, calls *counter) (workflow.Workflow, error) {
	// snippet:refund
	refund := func(rc *workflow.ReplayCtx, in []byte) ([]byte, error) {
		calls.add("body")
		var o order
		if err := json.Unmarshal(in, &o); err != nil {
			return nil, workflow.Permanent(err)
		}
		amount, err := workflow.ReplayStep(rc, "load-order", func(ctx context.Context) (int, error) {
			calls.add("load-order")
			return 40, nil // read from the orders table
		})
		if err != nil {
			return nil, err
		}
		if !rc.Replaying() { // log once, not on every wake
			fmt.Printf("  order %s loaded: %d EUR\n", o.ID, amount)
		}

		// Three days for the parcel to come back; no worker is held.
		if err := rc.Sleep("return-window", 72*time.Hour); err != nil {
			return nil, err
		}
		approver, err := rc.AwaitPromise("approval", promises, "refund-"+o.ID)
		if err != nil {
			return nil, err
		}
		if !rc.Replaying() {
			fmt.Printf("  approved by %s\n", approver)
		}

		method := "card"
		_, err = workflow.ReplayStep(rc, "refund-card", func(ctx context.Context) (string, error) {
			calls.add("refund-card")
			return "", workflow.Permanent(errors.New("card expired")) // the payment provider's answer
		})
		if err != nil && !workflow.IsPermanent(err) {
			return nil, err // not recorded: the step's retry calls the provider again
		}
		if err != nil { // recorded: every wake takes this branch again
			if !rc.Replaying() {
				fmt.Printf("  card refund refused (%v), paying store credit\n", err)
			}
			credit, err := workflow.ReplayStep(rc, "store-credit", func(ctx context.Context) (string, error) {
				calls.add("store-credit")
				return "credit-7", nil
			})
			if err != nil {
				return nil, err
			}
			method = "store credit " + credit
		}

		// Keep the ticket open a day, then close it.
		if err := rc.Sleep("close-after", 24*time.Hour); err != nil {
			return nil, err
		}
		closed, err := rc.Now("closed-at")
		if err != nil {
			return nil, err
		}
		return json.Marshal(fmt.Sprintf("%s: %d EUR as %s, approved by %s, closed %s",
			o.ID, amount, method, approver, closed.Format(time.RFC3339)))
	}
	// snippet:end

	// snippet:register
	handler, err := workflow.NewReplay(refund, workflow.ReplayOptions{
		Client: c,                // refused here without CapSnooze
		Clock:  clk,              // the time of Sleep and Now; the driver's clock
		Poll:   15 * time.Minute, // how often an unsettled promise is looked at
	})
	if err != nil {
		return workflow.Workflow{}, err
	}
	wf := workflow.Workflow{Name: "refund", Steps: []workflow.Step{{Name: "orchestrate", Run: handler}}}
	// snippet:end
	return wf, nil
}

// refundRun drives one refund through its waits and prints what ran.
func refundRun(ctx context.Context, c workflow.Client, clk *clock.Fake, promises workflow.PromiseStore, calls *counter) error {
	fmt.Println("refund:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "refund", Payload: []byte(`{"id":"A-1"}`)})
	if err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil { // the handler suspended in the return window
		return err
	}
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Printf("  in the return window the run is %s, holding no worker\n", st)

	clk.Advance(72 * time.Hour)
	if err := clk.BlockUntil(ctx, 1); err != nil { // suspended again, on the approval
		return err
	}
	fmt.Println("  three days later it waits for the approver")
	// snippet:approve
	if err := workflow.ResolvePromise(ctx, c, promises, "refund-A-1", []byte("dana")); err != nil {
		return err
	}
	// snippet:end
	clk.Advance(15 * time.Minute)                  // the next look at the promise
	if err := clk.BlockUntil(ctx, 1); err != nil { // suspended a third time, before closing
		return err
	}
	clk.Advance(24 * time.Hour)

	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	var result string
	if err := json.Unmarshal(out, &result); err != nil {
		return err
	}
	fmt.Println("  result:", result)
	fmt.Printf("  the body ran %d times; load-order ran %d, refund-card %d, store-credit %d\n",
		calls.get("body"), calls.get("load-order"), calls.get("refund-card"), calls.get("store-credit"))
	return nil
}

// driftWorkflow is a version of the code that branches on a value it
// does not record: a feature flag read on every run.
func driftWorkflow(c workflow.Client, clk clock.Clock, creditFirst *atomic.Bool) (workflow.Workflow, error) {
	// snippet:drift
	drift := func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		step := "refund-card"
		if creditFirst.Load() { // wrong: read again on every run, never recorded
			step = "store-credit"
		}
		if _, err := workflow.ReplayStep(rc, step, func(ctx context.Context) (bool, error) { return true, nil }); err != nil {
			return nil, err
		}
		return nil, rc.Sleep("close-after", 24*time.Hour)
	}
	// snippet:end
	handler, err := workflow.NewReplay(drift, workflow.ReplayOptions{Client: c, Clock: clk})
	if err != nil {
		return workflow.Workflow{}, err
	}
	return workflow.Workflow{Name: "refund-drift", Steps: []workflow.Step{{Name: "orchestrate", Run: handler}}}, nil
}

// driftRun flips the flag while the run sleeps and prints how it ends.
func driftRun(ctx context.Context, c workflow.Client, clk *clock.Fake, creditFirst *atomic.Bool, ended <-chan error) error {
	fmt.Println("nondeterminism:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "refund-drift"})
	if err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil { // recorded refund-card, then suspended
		return err
	}
	creditFirst.Store(true) // the flag changes while the run sleeps
	clk.Advance(24 * time.Hour)

	_, err = workflow.AwaitResult(ctx, c, ref, await)
	var failed *workflow.RunFailedError
	if !errors.As(err, &failed) {
		return fmt.Errorf("the drifting run did not fail: %v", err)
	}
	fmt.Printf("  the run ended %s: %s\n", failed.Status, failed.Err)
	// snippet:drift-error
	stepErr := <-ended // RunEvent.Err from the run-finished hook
	var nd *workflow.NondeterminismError
	if errors.As(stepErr, &nd) {
		fmt.Printf("  position %d: recorded %q, the code called %q; permanent %v\n",
			nd.Position, nd.Recorded, nd.Got, workflow.IsPermanent(stepErr))
	}
	// snippet:end
	fmt.Println("  matches ErrNondeterminism:", errors.Is(stepErr, workflow.ErrNondeterminism))
	return nil
}
