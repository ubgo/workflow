// Command signals-and-sleep builds workflows of several steps: a step that
// waits for an event matched to its run, a durable sleep, a wait that
// gives up after a timeout, parallel steps joined by a later one, a step
// timeout, and a handler that runs when a run fails.
//
// It runs on the in-memory driver with a fake clock, so "sleep a day" and
// "wait three days" pass when the program moves the clock. It is the
// program behind docs/workflows/steps.md, signals.md and sleep.md: the
// guides copy its regions (marked "snippet:") and task docs:snippets fails
// when they drift.
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

// start is the fake clock's first instant: Monday 5 January 2026, 09:00 UTC.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// layout prints a fake-clock time.
const layout = "Mon 15:04"

// order is the payload of the "order" workflow.
type order struct {
	ID    string
	Total int
}

// payment is the payload of the payment.succeeded event.
type payment struct {
	Order string
	Paid  int
}

// snippet:quote-in

// quoteIn is the input of a step with two parents: StepInput builds a JSON
// object keyed by parent name, which Define decodes into these fields.
type quoteIn struct {
	Price int `json:"price"` // the output of step "price"
	Stock int `json:"stock"` // the output of step "stock"
}

// snippet:end

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "signals-and-sleep:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(start)
	lines := make(chan string, 16)
	finished := make(chan workflow.RunEvent, 16)

	c := memory.New(memory.WithClock(clk))
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { finished <- ev }); err != nil {
		return err
	}

	// snippet:order
	orderFlow := workflow.Workflow{
		Name: "order",
		Steps: []workflow.Step{
			{
				Name: "reserve",
				Run: workflow.Define(func(ctx context.Context, in order) (order, error) {
					return in, nil // reserve stock, pass the order on
				}),
			},
			{
				Name:  "await-payment",
				After: []string{"reserve"},
				Wait: &workflow.WaitSpec{
					Event: "payment.succeeded",
					// Each run waits for the payment of its own order.
					KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["order"] }),
				},
			},
			{
				Name:  "cool-off",
				After: []string{"await-payment"},
				Sleep: 24 * time.Hour, // holds no worker; passes its input on
			},
			{
				Name:  "ship",
				After: []string{"cool-off"},
				Run: workflow.Define(func(ctx context.Context, p payment) (string, error) {
					lines <- fmt.Sprintf("ship %s (paid %d) at %s", p.Order, p.Paid, clk.Now().Format(layout))
					return "shipped", nil
				}),
			},
		},
	}
	// snippet:end

	// snippet:approval
	approval := workflow.Workflow{
		Name: "approval",
		Steps: []workflow.Step{{
			Name: "await-approval",
			Wait: &workflow.WaitSpec{
				Event:   "expense.approved",
				Key:     "finance", // a fixed key: every run waits for the same one
				Timeout: 72 * time.Hour,
			},
		}},
		// OnFailure runs once when a run fails for good, with the input of
		// the step that failed.
		OnFailure: func(ctx context.Context, in []byte) ([]byte, error) {
			lines <- fmt.Sprintf("on failure: escalate %s at %s", in, clk.Now().Format(layout))
			return nil, nil
		},
	}
	// snippet:end

	// snippet:fan-in
	quote := workflow.Workflow{
		Name: "quote",
		Steps: []workflow.Step{
			// price and stock have no After: both start with the run.
			{Name: "price", Run: workflow.Define(func(context.Context, string) (int, error) { return 120, nil })},
			{Name: "stock", Run: workflow.Define(func(context.Context, string) (int, error) { return 7, nil })},
			{
				Name:  "quote",
				After: []string{"price", "stock"}, // starts when both succeeded
				Run: workflow.Define(func(ctx context.Context, in quoteIn) (string, error) {
					info, _ := workflow.StepInfoFrom(ctx)
					lines <- fmt.Sprintf("%s/%s attempt %d: price %d, stock %d",
						info.Workflow, info.Step, info.Attempt, in.Price, in.Stock)
					return "ok", nil
				}),
			},
		},
	}
	// snippet:end

	// snippet:timeout
	export := workflow.Workflow{
		Name:    "export",
		Timeout: 30 * time.Second, // the default for every step; Step.Timeout overrides it
		Steps: []workflow.Step{{
			Name: "write",
			Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				<-ctx.Done() // a stuck call: the timeout cancels its ctx
				return nil, ctx.Err()
			},
		}},
	}
	// snippet:end

	for _, wf := range []workflow.Workflow{orderFlow, approval, quote, export} {
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
	// status is for printing only: an error reads as an empty status,
	// which the expected output in main_test.go catches.
	status := func(ref workflow.RunRef) workflow.Status {
		st, _ := c.Status(ctx, ref)
		return st
	}
	// until polls the run's status: a step moves on its own goroutine.
	until := func(ref workflow.RunRef, want workflow.Status) error {
		for range 5000 {
			if status(ref) == want {
				return nil
			}
			time.Sleep(time.Millisecond)
		}
		return fmt.Errorf("run %s never reached %s", ref.RunID, want)
	}

	fmt.Fprintln(out, "wait and sleep:")
	// snippet:enqueue-orders
	refs := map[string]workflow.RunRef{}
	for _, id := range []string{"A-1", "A-2"} {
		ref, err := c.Enqueue(ctx, workflow.Job{
			Workflow: "order",
			Payload:  fmt.Appendf(nil, `{"ID":%q,"Total":40}`, id),
			Labels:   map[string]string{"order": id}, // the key the wait step resolves
		})
		if err != nil {
			return err
		}
		refs[id] = ref
	}
	// snippet:end
	for _, id := range []string{"A-1", "A-2"} {
		if err := until(refs[id], workflow.StatusWaiting); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "  both orders %s for payment\n", status(refs["A-1"]))

	// snippet:signal
	err := c.Signal(ctx, workflow.Event{
		Name:    "payment.succeeded",
		Key:     "A-2", // resumes only the run waiting on A-2
		Payload: []byte(`{"Order":"A-2","Paid":40}`),
	})
	if err != nil {
		return err
	}
	// snippet:end
	if err := clk.BlockUntil(ctx, 1); err != nil { // A-2 is in its 24h cool-off
		return err
	}
	fmt.Fprintf(out, "  paid A-2 at %s: A-2 %s (sleeping), A-1 %s\n", clk.Now().Format(layout), status(refs["A-2"]), status(refs["A-1"]))
	clk.Advance(24 * time.Hour)
	fmt.Fprintln(out, " ", <-lines)
	<-finished
	fmt.Fprintf(out, "  A-2 %s, A-1 still %s\n", status(refs["A-2"]), status(refs["A-1"]))

	if err := c.Signal(ctx, workflow.Event{Name: "payment.succeeded", Key: "A-1", Payload: []byte(`{"Order":"A-1","Paid":40}`)}); err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil {
		return err
	}
	clk.Advance(24 * time.Hour)
	fmt.Fprintln(out, " ", <-lines)
	<-finished

	fmt.Fprintln(out, "give up after a timeout:")
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "approval", Payload: []byte(`"expense 77"`)}); err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil { // the wait armed its 72h timeout
		return err
	}
	clk.Advance(72 * time.Hour)
	fmt.Fprintln(out, " ", <-lines)
	ev := <-finished
	fmt.Fprintf(out, "  run %s, deadline exceeded %t\n", ev.Status, errors.Is(ev.Err, context.DeadlineExceeded))

	fmt.Fprintln(out, "steps that depend on other steps:")
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "quote", Payload: []byte(`"sku-9"`)}); err != nil {
		return err
	}
	fmt.Fprintln(out, " ", <-lines)
	<-finished

	fmt.Fprintln(out, "step timeout:")
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "export"}); err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil { // the attempt armed its 30s timeout
		return err
	}
	clk.Advance(30 * time.Second)
	ev = <-finished
	fmt.Fprintf(out, "  run %s: %v\n", ev.Status, ev.Err)

	return refusals(ctx, out)
}

// refusals shows the per-run key form each driver refuses: the memory
// driver cannot evaluate an expression and refuses it at Start; the
// Hatchet driver cannot evaluate an expression to a key either and
// refuses it at Register (it connects only at Start, so this runs
// without a server).
func refusals(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "refusals:")
	// snippet:refusal
	byExpr := workflow.Workflow{
		Name: "order",
		Steps: []workflow.Step{{
			Name: "await-payment",
			Wait: &workflow.WaitSpec{Event: "payment.succeeded", KeyFrom: workflow.KeyExpr("input.ID")},
		}},
	}
	m := memory.New()
	if err := m.Register(byExpr); err != nil { // accepted here; Start validates the key
		return err
	}
	fmt.Fprintln(out, "  memory:", m.Start(ctx))
	m.Stop()
	fmt.Fprintln(out, "  hatchet:", hatchet.New(hatchet.Config{}).Register(byExpr))
	// snippet:end
	return nil
}
