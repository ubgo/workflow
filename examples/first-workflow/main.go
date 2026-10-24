// Command first-workflow runs a four-step order workflow on the in-memory
// driver: a payment step that fails once and is retried, two steps that
// run in parallel after it, and a last step that joins their outputs. A
// second order fails for good and runs the workflow's OnFailure handler.
//
// It is the program behind docs/start/first-workflow.md and
// docs/concepts/client.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "first-workflow:", err)
		os.Exit(1)
	}
}

// snippet:types
// order is the job's payload. Each step's input and output are JSON.
type order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

// joined is the input of a step with several parents: one field per
// parent, named after the parent step.
type joined struct {
	Receipt string `json:"receipt"`
	Ship    string `json:"ship"`
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	var mu sync.Mutex
	say := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(out, format+"\n", args...)
	}
	var charges int

	// snippet:workflow
	wf := workflow.Workflow{
		Name: "order",
		// Every step may be retried twice, 10ms apart.
		Retry: workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: 10 * time.Millisecond}},
		Steps: []workflow.Step{
			{Name: "charge", Run: workflow.Define(func(ctx context.Context, o order) (order, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				if o.Amount <= 0 {
					// Permanent: retrying the same input can never succeed.
					return o, workflow.Permanent(fmt.Errorf("order %s: nothing to charge", o.ID))
				}
				charges++
				if charges == 1 {
					say("charge %s: attempt %d failed", o.ID, info.Attempt)
					return o, errors.New("card network timeout")
				}
				say("charge %s: attempt %d charged %d", o.ID, info.Attempt, o.Amount)
				return o, nil
			})},
			// receipt and ship both start once charge succeeded, in
			// parallel, each with charge's output as input.
			{Name: "receipt", After: []string{"charge"}, Run: workflow.Define(func(ctx context.Context, o order) (string, error) {
				return "receipt for " + o.ID, nil
			})},
			{Name: "ship", After: []string{"charge"}, Run: workflow.Define(func(ctx context.Context, o order) (string, error) {
				return "parcel for " + o.ID, nil
			})},
			// notify has two parents: its input is a JSON object keyed by
			// their names.
			{Name: "notify", After: []string{"receipt", "ship"}, Run: workflow.Define(func(ctx context.Context, in joined) (struct{}, error) {
				say("notify: %s, %s", in.Receipt, in.Ship)
				return struct{}{}, nil
			})},
		},
		// OnFailure runs once when a run fails for good, with the failing
		// step's input.
		OnFailure: func(ctx context.Context, in []byte) ([]byte, error) {
			say("on failure: %s", in)
			return nil, nil
		},
	}
	// snippet:end

	// snippet:start
	c := memory.New()
	ended := make(chan workflow.RunEvent, 2)
	// Observers are registered before Start; the memory driver reports
	// every run that reaches a terminal status.
	if err := c.OnRunFinished(func(ctx context.Context, ev workflow.RunEvent) { ended <- ev }); err != nil {
		return err
	}
	if err := c.Register(wf); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // stops every worker when run returns
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:end

	for _, o := range []string{`{"id":"A-1","amount":30}`, `{"id":"A-2","amount":0}`} {
		// snippet:enqueue
		ref, err := c.Enqueue(ctx, workflow.Job{
			Workflow: "order",
			Payload:  []byte(o),
			Labels:   map[string]string{"tenant": "acme"},
		})
		if err != nil {
			return err
		}
		ev := <-ended
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		say("run %s: %s (status %s, err %v)", ev.Workflow, ev.Status, st, ev.Err)
		// snippet:end
	}
	return nil
}
