// Command escalation shows signals addressed to one run by its ID: a
// wait step with WaitSpec.OwnRun that only SignalRun for its own run
// resumes, a child escalating to its parent with SignalParent while it
// goes on, and OnSignals handlers that hear a run's signals between its
// steps, here while the parent joins its children. Approvals, races and
// updates address one run the same way: ApproveRun, RejectRun,
// SignalChoiceRun and UpdateRun answer only the run they name. It runs on
// the in-memory driver, so nothing needs to be installed.
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

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "escalation:", err)
		os.Exit(1)
	}
}

// Polling tunables: in-memory runs move in microseconds, so short polls
// keep the program fast without spinning.
const (
	// pollEvery is how often the program looks at a run it waits for.
	pollEvery = 2 * time.Millisecond
	// joinPoll is how often the migration's join looks at its shards.
	joinPoll = 5 * time.Millisecond
	// waitTimeout bounds waitFor, so a run stuck before its wait fails
	// the program instead of hanging it.
	waitTimeout = 10 * time.Second
	// refundCount is how many refunds wait on approval: one is approved,
	// one rejected.
	refundCount = 2
)

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: pollEvery}

// migrationReport is the migration's output: how many shards it joined
// and the slow-shard signals its handler heard.
type migrationReport struct {
	Heard  []string        `json:"heard"`
	Shards json.RawMessage `json:"shards"`
}

// notes is what the migration's signal handler heard.
type notes struct {
	mu   sync.Mutex
	seen []string
}

// add records one signal payload.
func (n *notes) add(s string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seen = append(n.seen, s)
}

// sorted returns what was heard in sorted order, so the output does not
// depend on which shard signalled first.
func (n *notes) sorted() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Sorted(slices.Values(n.seen))
}

// run is main without the process exit, so the test drives the same code.
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
	const receiptLimit = 1000 // the most a receipt may claim before a reviewer looks
	checkReceipt := workflow.Workflow{Name: "check-receipt", Steps: []workflow.Step{{Name: "check",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var amount int
			if err := json.Unmarshal(in, &amount); err != nil {
				return nil, workflow.Permanent(err)
			}
			if amount > receiptLimit {
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
			if n%2 == 1 { // odd shards report themselves slow
				if err := workflow.SignalParent(ctx, c, "slow", fmt.Appendf(nil, "shard %d is slow", n)); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}}}}
	migrate := workflow.Workflow{Name: "migrate", Steps: []workflow.Step{
		{Name: "shards", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			jobs := []workflow.Job{{Workflow: "shard", Payload: []byte("1")}, {Workflow: "shard", Payload: []byte("2")}, {Workflow: "shard", Payload: []byte("3")}}
			res, err := workflow.JoinChildren(ctx, c, "shards", jobs, workflow.JoinSpec{Poll: joinPoll})
			if err != nil {
				return nil, err // a Snooze while shards run; the handlers run on every look
			}
			return json.Marshal(len(res.Outputs))
		}},
		{Name: "report", After: []string{"shards"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			return json.Marshal(migrationReport{Heard: heard.sorted(), Shards: in})
		}},
	}}

	// snippet:by-run-steps
	approval, err := workflow.ApprovalStep(workflow.ApprovalSpec{Name: "approve", OwnRun: true})
	if err != nil {
		return err
	}
	refund := workflow.Workflow{Name: "refund", Steps: []workflow.Step{approval,
		{Name: "pay", After: []string{"approve"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			d, err := workflow.DecisionOf(in)
			if err != nil {
				return nil, err
			}
			return json.Marshal(fmt.Sprintf("approved=%v by %s", d.Approved, d.By))
		}}}}
	race, err := workflow.Race(workflow.RaceSpec{Name: "carrier", Event: "carrier", OwnRun: true, Branches: []workflow.Branch{
		{Name: "express", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
		{Name: "ground", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
	}})
	if err != nil {
		return err
	}
	dispatch := workflow.Workflow{Name: "dispatch", Steps: race}
	promises := workflow.NewMemoryPromises() // the update's answer travels through it
	cart := workflow.Workflow{Name: "cart", Steps: []workflow.Step{
		{Name: "ask", Wait: &workflow.WaitSpec{Event: "add-item", OwnRun: true}},
		{Name: "answer", After: []string{"ask"}, Run: workflow.UpdateHandler(promises, func(_ context.Context, in []byte) ([]byte, error) {
			return json.Marshal("added " + string(in))
		})},
	}}
	// snippet:end

	// snippet:handlers
	onSlow := workflow.OnSignals(workflow.SignalHandlers{Workflow: "migrate", On: map[string]workflow.SignalHandler{
		"slow": func(_ context.Context, s workflow.RunSignal) error {
			heard.add(string(s.Payload)) // runs before a step of the migration, then the step goes on
			return nil
		},
	}})
	c, err = workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()), onSlow)
	// snippet:end
	if err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{order, checkReceipt, expense, shard, migrate, refund, dispatch, cart} {
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

	fmt.Println("approvals, races and updates by run ID:")
	var refunds [refundCount]workflow.RunRef
	for i := range refunds {
		if refunds[i], err = c.Enqueue(ctx, workflow.Job{Workflow: "refund"}); err != nil {
			return err
		}
		if err := waitFor(ctx, c, refunds[i], workflow.StatusWaiting); err != nil {
			return err
		}
	}
	// snippet:approve-run
	if err := workflow.ApproveRun(ctx, c, refunds[0], "ana", "receipt checked"); err != nil {
		return err
	}
	if err := workflow.RejectRun(ctx, c, refunds[1], "bo", "duplicate"); err != nil {
		return err
	}
	// snippet:end
	for i, r := range refunds {
		if out, err = workflow.AwaitResult(ctx, c, r, await); err != nil {
			return err
		}
		fmt.Printf("  refund %d: %s\n", i+1, out)
	}

	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "dispatch"})
	if err != nil {
		return err
	}
	// snippet:choice-run
	if err := workflow.SignalChoiceRun(ctx, c, ref, "carrier", "express", []byte(`"next day"`)); err != nil {
		return err
	}
	// snippet:end
	if out, err = workflow.AwaitResult(ctx, c, ref, await); err != nil {
		return err
	}
	fmt.Printf("  dispatch took: %s\n", out)

	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "cart"})
	if err != nil {
		return err
	}
	// snippet:update-run
	uctx, stop := context.WithTimeout(ctx, 10*time.Second) // a run that never reaches its wait never answers
	defer stop()
	answer, err := workflow.UpdateRun(uctx, c, promises, ref, "add-item", []byte("socks"), await)
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Printf("  cart answered: %s\n", answer)
	return nil
}

// waitFor polls until ref reaches want.
func waitFor(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	ctx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st == want {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("run %s is %s, want %s: %w", ref.RunID, st, want, ctx.Err())
		case <-time.After(pollEvery):
		}
	}
}
