// Command flow-promises shows the signal helpers of the core package: a
// promise a run waits on and an API call resolves, Update (a signal that
// returns the run's answer), SignalWithStart (start a run if it is not
// running, then signal it) and ContinueAsNew (a run that loops for ever as
// a chain of short runs). It runs on the in-memory driver, so nothing
// needs to be installed.
//
// It is the program behind docs/workflows/promises.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "flow-promises:", err)
		os.Exit(1)
	}
}

// Timing tunables: in-memory runs move in microseconds, so short polls
// keep the program fast without spinning.
const (
	// pollEvery is how often the program looks at a run it waits for.
	pollEvery = 2 * time.Millisecond
	// settle is how long the refund run gets to reach its promise before
	// the program checks that it is still waiting.
	settle = 20 * time.Millisecond
	// chainTimeout bounds the wait for the poller's chain of runs.
	chainTimeout = 5 * time.Second
)

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: pollEvery}

// cartKey makes every cart run wait on its own key.
var cartKey = workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["cart"] })

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		return err
	}
	promises := workflow.NewMemoryPromises()

	// snippet:await-promise
	refund := workflow.Workflow{Name: "refund", Steps: []workflow.Step{{Name: "approval", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var requestID string
		if err := json.Unmarshal(in, &requestID); err != nil {
			return nil, workflow.Permanent(err)
		}
		return workflow.AwaitPromise(ctx, promises, requestID, 5*time.Millisecond) // snoozes and looks again this often
	}}}}
	// snippet:end

	// snippet:update-handler
	cart := workflow.Workflow{Name: "cart", Steps: []workflow.Step{
		{Name: "add", Wait: &workflow.WaitSpec{Event: "cart.add", KeyFrom: cartKey}},
		{Name: "price", After: []string{"add"}, Run: workflow.UpdateHandler(promises, func(_ context.Context, args []byte) ([]byte, error) {
			var item string
			if err := json.Unmarshal(args, &item); err != nil {
				return nil, workflow.Permanent(err)
			}
			return json.Marshal(item + " added, total 3 items")
		})},
	}}
	// snippet:end

	// snippet:continue
	const rounds = 3 // the poller stops after its third round
	var client workflow.Client = c
	poller := workflow.Workflow{Name: "poller", Steps: []workflow.Step{{Name: "round", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var round int
		if err := json.Unmarshal(in, &round); err != nil {
			return nil, workflow.Permanent(err)
		}
		// A real poller checks its source here; this one only counts rounds.
		if round < rounds {
			if _, err := workflow.ContinueAsNew(ctx, client, []byte(fmt.Sprint(round+1))); err != nil {
				return nil, err
			}
		}
		return json.Marshal(round)
	}}}}
	// snippet:end

	for _, wf := range []workflow.Workflow{refund, cart, poller} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	fmt.Println("promise:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "refund", Payload: []byte(`"req-42"`)})
	if err != nil {
		return err
	}
	time.Sleep(settle)
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Println("  before approval the run has ended:", st.IsTerminal())
	// snippet:resolve
	if err := workflow.ResolvePromise(ctx, c, promises, "req-42", []byte(`"approved by finance"`)); err != nil {
		return err
	}
	// snippet:end
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Println("  after approval:", string(out))
	err = workflow.RejectPromise(ctx, c, promises, "req-42", "too late")
	fmt.Println("  a second, different outcome is refused:", err != nil)

	fmt.Println("signal with start and update:")
	// snippet:signal-with-start
	job := workflow.Job{Workflow: "cart", IdempotencyKey: "cart-7", Labels: map[string]string{"cart": "cart-7"}}
	first, err := workflow.SignalWithStart(ctx, c, job, workflow.Event{Name: "cart.add", Key: "cart-7", Payload: []byte(`"socks"`)})
	if err != nil {
		return err
	}
	// snippet:end
	out, err = workflow.AwaitResult(ctx, c, first, await)
	if err != nil {
		return err
	}
	fmt.Println("  started and signalled:", string(out))
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "cart", Labels: map[string]string{"cart": "cart-8"}}); err != nil {
		return err
	}
	// snippet:update
	updCtx, cancelUpd := context.WithTimeout(ctx, 10*time.Second) // a run that never takes the signal never answers
	defer cancelUpd()
	answer, err := workflow.Update(updCtx, c, promises, workflow.Event{Name: "cart.add", Key: "cart-8", Payload: []byte(`"hat"`)}, await)
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Println("  update answered:", string(answer))

	fmt.Println("continue as new:")
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "poller", Payload: []byte("1")}); err != nil {
		return err
	}
	deadline := time.Now().Add(chainTimeout)
	for {
		page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "poller", Statuses: []workflow.Status{workflow.StatusSucceeded}})
		if err != nil {
			return err
		}
		if len(page.Runs) == rounds || time.Now().After(deadline) {
			fmt.Println("  rounds run as separate runs:", len(page.Runs))
			break
		}
		time.Sleep(pollEvery)
	}
	return nil
}
