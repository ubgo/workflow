// Command flow-replay shows deterministic replay: a refund written as one
// plain Go function with local variables, which waits three days for the
// parcel, then for an approver, then a day before closing the ticket,
// without being split into steps and without holding a worker while it
// waits. Every call that touches the world goes through the replay
// context and is recorded; on each wake the function runs again from the
// top, the recorded calls return their results without running again,
// and the code carries on where it stopped. A second version of the code
// branches on a value it did not record and fails with the
// nondeterminism error. A payout shows the two calls the refund does not
// use: a child run awaited with ReplayChild, spawned once however often
// the body runs, and SleepUntil a fixed pay day.
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

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "flow-replay:", err)
		os.Exit(1)
	}
}

// awaitPoll is how often the program looks at a run it waits for, and at
// the journal while it empties: the memory driver answers at once, so a
// short poll keeps the run fast.
const awaitPoll = 2 * time.Millisecond

// journalPolls bounds the wait for the journal to forget every run (one
// awaitPoll each), so a broken plugin fails the program instead of
// hanging it.
const journalPolls = 500

// The refund's waits, as its snippet writes them inline: refundRun moves
// the fake clock by exactly these amounts to wake each one.
const (
	returnWindow = 72 * time.Hour   // rc.Sleep("return-window", ...)
	promisePoll  = 15 * time.Minute // ReplayOptions.Poll: the next look at the promise
	closeAfter   = 24 * time.Hour   // rc.Sleep("close-after", ...), also in the drift
)

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: awaitPoll}

// counter counts how often each piece of code ran.
type counter struct {
	mu sync.Mutex
	n  map[string]int
}

// add counts one more run of the code called name.
func (c *counter) add(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[name]++
}

// get returns how often the code called name ran.
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
	payout, err := payoutWorkflow(c, clk, &calls)
	if err != nil {
		return err
	}
	release := make(chan struct{}) // closed when the invoice may be issued
	for _, wf := range []workflow.Workflow{refund, drift, payout, invoiceWorkflow(release, &calls)} {
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
	if err := payoutRun(ctx, c, clk, release, &calls); err != nil {
		return err
	}

	// The Journal plugin forgets a run's history when the run finishes.
	for range journalPolls {
		if len(journal.Runs()) == 0 {
			break
		}
		time.Sleep(awaitPoll)
	}
	fmt.Printf("journal:\n  runs holding history after every run finished: %d\n", len(journal.Runs()))
	return nil
}

// refundWorkflow is the refund as one replayed handler.
func refundWorkflow(c workflow.Client, clk clock.Clock, promises workflow.PromiseStore, calls *counter) (workflow.Workflow, error) {
	// snippet:refund
	// refund is the whole refund as one function: each wake runs it from
	// the top, and the recorded calls return without running again.
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

	clk.Advance(returnWindow)
	if err := clk.BlockUntil(ctx, 1); err != nil { // suspended again, on the approval
		return err
	}
	fmt.Println("  three days later it waits for the approver")
	// snippet:approve
	if err := workflow.ResolvePromise(ctx, c, promises, "refund-A-1", []byte("dana")); err != nil {
		return err
	}
	// snippet:end
	clk.Advance(promisePoll)                       // the next look at the promise
	if err := clk.BlockUntil(ctx, 1); err != nil { // suspended a third time, before closing
		return err
	}
	clk.Advance(closeAfter)

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

// payDay is when the seller is paid: a fixed instant, so SleepUntil has a
// wake time that does not depend on when the run started.
var payDay = time.Date(2026, 3, 31, 17, 0, 0, 0, time.UTC)

// childPoll is how often the payout looks at its invoice child while the
// child runs. On the fake clock the program advances it by hand.
const childPoll = time.Minute

// invoiceWorkflow is the child the payout starts: it issues one invoice
// once release is closed, so the payout is sure to find it still running
// on its first look and the output is the same on every run.
func invoiceWorkflow(release <-chan struct{}, calls *counter) workflow.Workflow {
	return workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{Name: "issue", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		calls.add("invoice")
		var seller string
		if err := json.Unmarshal(in, &seller); err != nil {
			return nil, workflow.Permanent(err)
		}
		return json.Marshal("INV-" + seller)
	}}}}
}

// payoutWorkflow pays a seller on pay day, after an invoice child run.
func payoutWorkflow(c workflow.Client, clk clock.Clock, calls *counter) (workflow.Workflow, error) {
	// snippet:calls
	// payout pays a seller after an invoice child run, on pay day.
	payout := func(rc *workflow.ReplayCtx, in []byte) ([]byte, error) {
		calls.add("payout body")
		// The child is spawned once; later wakes look at it until it ended.
		invoice, err := workflow.ReplayChild(rc, "invoice", c,
			workflow.Job{Workflow: "invoice", Payload: in}, workflow.ChildOptions{Poll: childPoll})
		if err != nil {
			return nil, err // a suspension while the child runs, or its failure
		}
		if !rc.Replaying() {
			fmt.Printf("  invoice child returned %s\n", invoice)
		}
		// A fixed instant, unlike Sleep's duration from the first time here.
		if err := rc.SleepUntil("pay-day", payDay); err != nil {
			return nil, err
		}
		paidAt, err := rc.Now("paid-at")
		if err != nil {
			return nil, err
		}
		return json.Marshal(fmt.Sprintf("paid %s at %s", invoice, paidAt.Format(time.RFC3339)))
	}
	// snippet:end
	handler, err := workflow.NewReplay(payout, workflow.ReplayOptions{Client: c, Clock: clk})
	if err != nil {
		return workflow.Workflow{}, err
	}
	return workflow.Workflow{Name: "payout", Steps: []workflow.Step{{Name: "orchestrate", Run: handler}}}, nil
}

// payoutRun drives the payout through its three runs of the body: the
// first spawns the invoice and suspends while it runs, the second records
// the invoice and suspends until pay day, the third finishes.
func payoutRun(ctx context.Context, c workflow.Client, clk *clock.Fake, release chan<- struct{}, calls *counter) error {
	fmt.Println("child and pay day:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "payout", Payload: []byte(`"s-9"`)})
	if err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil { // suspended: the child runs
		return err
	}
	close(release)
	page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "invoice"})
	if err != nil {
		return err
	}
	if len(page.Runs) != 1 {
		return fmt.Errorf("invoice runs: %d, want 1", len(page.Runs))
	}
	if _, err := workflow.AwaitResult(ctx, c, page.Runs[0].Ref, await); err != nil {
		return err
	}
	clk.Advance(childPoll)                         // the next look at the child
	if err := clk.BlockUntil(ctx, 1); err != nil { // suspended until pay day
		return err
	}
	clk.Advance(payDay.Sub(clk.Now()))
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	var result string
	if err := json.Unmarshal(out, &result); err != nil {
		return err
	}
	fmt.Println("  result:", result)
	fmt.Printf("  the body ran %d times; the invoice child ran %d\n",
		calls.get("payout body"), calls.get("invoice"))
	return nil
}

// driftWorkflow is a version of the code that branches on a value it
// does not record: a feature flag read on every run.
func driftWorkflow(c workflow.Client, clk clock.Clock, creditFirst *atomic.Bool) (workflow.Workflow, error) {
	// snippet:drift
	// drift records one call whose name depends on an unrecorded flag.
	drift := func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		step := "refund-card"
		if creditFirst.Load() { // wrong: read again on every run, never recorded
			step = "store-credit"
		}
		if _, err := workflow.ReplayStep(rc, step, func(ctx context.Context) (bool, error) { return true, nil }); err != nil {
			return nil, err
		}
		return nil, rc.Sleep("close-after", 24*time.Hour) // a day, as in the refund
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
	clk.Advance(closeAfter)

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
