// Command escalation shows signals addressed to one run by its ID: a
// wait step with WaitSpec.OwnRun that only SignalRun for its own run
// resumes, a child escalating to its parent with SignalParent while it
// goes on, and OnSignals handlers that hear a run's signals between its
// steps, here while the parent joins its children. It runs on the
// in-memory driver, so nothing needs to be installed.
//
// It is the program behind docs/workflows/escalation.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "escalation:", err)
		os.Exit(1)
	}
}

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// notes is what the migration's signal handler heard.
type notes struct {
	mu   sync.Mutex
	seen []string
}

func (n *notes) add(s string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seen = append(n.seen, s)
}

func (n *notes) sorted() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Sorted(slices.Values(n.seen))
}

func run(ctx context.Context) error {
	var c workflow.Client
	heard := &notes{}

	// snippet:address
	order := workflow.Workflow{Name: "order", Steps: []workflow.Step{
		{Name: "await-payment", Wait: &workflow.WaitSpec{Event: "payment", OwnRun: true}},
		{Name: "ship", After: []string{"await-payment"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			return in, nil
		}},
	}}
	// snippet:end

	// snippet:escalate
	checkReceipt := workflow.Workflow{Name: "check-receipt", Steps: []workflow.Step{{Name: "check",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var amount int
			if err := json.Unmarshal(in, &amount); err != nil {
				return nil, workflow.Permanent(err)
			}
			if amount > 1000 {
				note, _ := json.Marshal(fmt.Sprintf("receipt over limit: %d", amount))
				if err := workflow.SignalParent(ctx, c, "escalation", note); err != nil {
					return nil, err
				}
			}
			return []byte(`"checked"`), nil // the child goes on after escalating
		}}}}
	expense := workflow.Workflow{Name: "expense", Steps: []workflow.Step{
		{Name: "check", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			_, err := workflow.Spawn(ctx, c, "receipt", []workflow.Job{{Workflow: "check-receipt", Payload: in}})
			return nil, err
		}},
		{Name: "escalated", After: []string{"check"}, Wait: &workflow.WaitSpec{Event: "escalation", OwnRun: true, Timeout: time.Minute}},
		{Name: "review", After: []string{"escalated"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			return in, nil
		}},
	}}
	// snippet:end

	shard := workflow.Workflow{Name: "shard", Steps: []workflow.Step{{Name: "copy",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var n int
			if err := json.Unmarshal(in, &n); err != nil {
				return nil, workflow.Permanent(err)
			}
			if n%2 == 1 {
				if err := workflow.SignalParent(ctx, c, "slow", fmt.Appendf(nil, "shard %d is slow", n)); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}}}}
	migrate := workflow.Workflow{Name: "migrate", Steps: []workflow.Step{
		{Name: "shards", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			jobs := []workflow.Job{{Workflow: "shard", Payload: []byte("1")}, {Workflow: "shard", Payload: []byte("2")}, {Workflow: "shard", Payload: []byte("3")}}
			res, err := workflow.JoinChildren(ctx, c, "shards", jobs, workflow.JoinSpec{Poll: 5 * time.Millisecond})
			if err != nil {
				return nil, err // a Snooze while shards run; the handlers run on every look
			}
			return json.Marshal(len(res.Outputs))
		}},
		{Name: "report", After: []string{"shards"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			return json.Marshal(map[string]any{"shards": json.RawMessage(in), "heard": heard.sorted()})
		}},
	}}

	// snippet:handlers
	onSlow := workflow.OnSignals(workflow.SignalHandlers{Workflow: "migrate", On: map[string]workflow.SignalHandler{
		"slow": func(_ context.Context, s workflow.RunSignal) error {
			heard.add(string(s.Payload)) // runs before a step of the migration, then the step goes on
			return nil
		},
	}})
	c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()), onSlow)
	// snippet:end
	if err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{order, checkReceipt, expense, shard, migrate} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}

	fmt.Println("signal a run by its ID:")
	first, err := c.Enqueue(ctx, workflow.Job{Workflow: "order"})
	if err != nil {
		return err
	}
	second, err := c.Enqueue(ctx, workflow.Job{Workflow: "order"})
	if err != nil {
		return err
	}
	if err := waitFor(ctx, c, second, workflow.StatusWaiting); err != nil {
		return err
	}
	// snippet:send
	if err := workflow.SignalRun(ctx, c, second, "payment", []byte(`"card 4242"`)); err != nil {
		return err
	}
	// snippet:end
	out, err := workflow.AwaitResult(ctx, c, second, await)
	if err != nil {
		return err
	}
	fmt.Printf("  second order: paid %s\n", out)
	st, err := c.Status(ctx, first)
	if err != nil {
		return err
	}
	fmt.Printf("  first order: %s\n", st)

	fmt.Println("escalation to the parent:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "expense", Payload: []byte("1200")})
	if err != nil {
		return err
	}
	if out, err = workflow.AwaitResult(ctx, c, ref, await); err != nil {
		return err
	}
	fmt.Printf("  parent took: %s\n", out)

	fmt.Println("handlers between steps:")
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "migrate"})
	if err != nil {
		return err
	}
	if out, err = workflow.AwaitResult(ctx, c, ref, await); err != nil {
		return err
	}
	fmt.Printf("  %s\n", out)
	return nil
}

// waitFor polls until ref reaches want.
func waitFor(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st == want {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("run %s is %s, want %s: %w", ref.RunID, st, want, ctx.Err())
		case <-time.After(2 * time.Millisecond):
		}
	}
}
