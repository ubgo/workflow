// Command signals-and-sleep builds workflows of several steps: a step that
// waits for an event matched to its run, a durable sleep, a wait that
// gives up after a timeout, parallel steps joined by a later one, a step
// timeout, and a handler that runs when a run fails. It then resolves
// the key a wait step waits on, and calls a typed Define handler and a
// handler reading its StepInfo directly, as a unit test would.
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

// The fake-clock moves, each equal to a duration a snippet spells out, so
// the program crosses exactly that wait.
const (
	coolOff         = 24 * time.Hour   // the order's cool-off Sleep
	approvalTimeout = 72 * time.Hour   // the approval wait's Timeout
	stepTimeout     = 30 * time.Second // the export workflow's Timeout
)

const (
	// eventBuffer holds the steps' lines and the run ends until the
	// program reads them, so no step or observer blocks.
	eventBuffer = 16
	// untilTries and untilPoll bound the wait for a run's status: five
	// thousand looks a millisecond apart, about five seconds.
	untilTries = 5000
	untilPoll  = time.Millisecond
)

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

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "signals-and-sleep:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	clk := clock.NewFake(start)
	lines := make(chan string, eventBuffer)
	finished := make(chan workflow.RunEvent, eventBuffer)

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
				Key:     "finance",      // a fixed key: every run waits for the same one
				Timeout: 72 * time.Hour, // three days, then the run fails
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
		for range untilTries {
			if status(ref) == want {
				return nil
			}
			time.Sleep(untilPoll)
		}
		return fmt.Errorf("run %s never reached %s", ref.RunID, want)
	}

	fmt.Println("wait and sleep:")
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
	fmt.Printf("  both orders %s for payment\n", status(refs["A-1"]))

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
	fmt.Printf("  paid A-2 at %s: A-2 %s (sleeping), A-1 %s\n", clk.Now().Format(layout), status(refs["A-2"]), status(refs["A-1"]))
	clk.Advance(coolOff)
	fmt.Println(" ", <-lines)
	<-finished
	fmt.Printf("  A-2 %s, A-1 still %s\n", status(refs["A-2"]), status(refs["A-1"]))

	if err := c.Signal(ctx, workflow.Event{Name: "payment.succeeded", Key: "A-1", Payload: []byte(`{"Order":"A-1","Paid":40}`)}); err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil {
		return err
	}
	clk.Advance(coolOff)
	fmt.Println(" ", <-lines)
	<-finished

	fmt.Println("give up after a timeout:")
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "approval", Payload: []byte(`"expense 77"`)}); err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil { // the wait armed its 72h timeout
		return err
	}
	clk.Advance(approvalTimeout)
	fmt.Println(" ", <-lines)
	ev := <-finished
	fmt.Printf("  run %s, deadline exceeded %t\n", ev.Status, errors.Is(ev.Err, context.DeadlineExceeded))

	fmt.Println("steps that depend on other steps:")
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "quote", Payload: []byte(`"sku-9"`)}); err != nil {
		return err
	}
	fmt.Println(" ", <-lines)
	<-finished

	fmt.Println("step timeout:")
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "export"}); err != nil {
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil { // the attempt armed its 30s timeout
		return err
	}
	clk.Advance(stepTimeout)
	ev = <-finished
	fmt.Printf("  run %s: %v\n", ev.Status, ev.Err)

	if err := refusals(ctx); err != nil {
		return err
	}
	if err := waitKeys(*orderFlow.Steps[1].Wait, *approval.Steps[0].Wait); err != nil {
		return err
	}
	return callHandlers(ctx)
}

// refusals shows the per-run key form each driver refuses: the memory
// driver cannot evaluate an expression and refuses it at Start; the
// Hatchet driver cannot evaluate an expression to a key either and
// refuses it at Register (it connects only at Start, so this runs
// without a server).
func refusals(ctx context.Context) error {
	fmt.Println("refusals:")
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
	fmt.Println("  memory:", m.Start(ctx))
	m.Stop()
	fmt.Println("  hatchet:", hatchet.New(hatchet.Config{}).Register(byExpr))
	// snippet:end
	return nil
}

// waitKeys asks two wait steps of the program which key a run waits on:
// the order's KeyFrom derives it from the run's job, the approval's Key is
// fixed. It is what a driver does when a wait step begins.
func waitKeys(byLabel, fixed workflow.WaitSpec) error {
	fmt.Println("wait keys:")
	// snippet:resolve-key
	job := workflow.Job{Workflow: "order", Labels: map[string]string{"order": "A-7"}}
	perRun, err := byLabel.ResolveKey(job) // KeyFrom: this run's own key
	if err != nil {
		return err
	}
	shared, err := fixed.ResolveKey(job) // Key: the same for every run
	if err != nil {
		return err
	}
	fmt.Printf("  order A-7 waits on %q, every approval on %q\n", perRun, shared)
	byExpr := workflow.WaitSpec{Event: "payment.succeeded", KeyFrom: workflow.KeyExpr("input.ID")}
	_, err = byExpr.ResolveKey(job)
	fmt.Println("  an expression is the engine's to evaluate:", errors.Is(err, workflow.ErrUnsupported))
	// snippet:end
	return nil
}

// callHandlers calls handlers directly, as a unit test of one handler
// would: Define decoding typed input, and a handler reading the StepInfo
// a driver (here WithStepInfo) put on its ctx.
func callHandlers(ctx context.Context) error {
	fmt.Println("call a handler directly:")
	// snippet:define
	type greeting struct{ Name string }
	greet := workflow.Define(func(ctx context.Context, in greeting) (string, error) {
		return "hello " + in.Name, nil
	})
	out, err := greet(ctx, []byte(`{"Name":"Ada"}`))
	if err != nil {
		return err
	}
	fmt.Printf("  typed: %s\n", out)
	out, err = greet(ctx, nil) // empty input decodes to the zero greeting
	if err != nil {
		return err
	}
	fmt.Printf("  empty input: %s\n", out)
	_, err = greet(ctx, []byte(`not json`))
	fmt.Println("  bad input is permanent:", workflow.IsPermanent(err))
	// snippet:end

	// snippet:step-info
	report := func(ctx context.Context, _ []byte) ([]byte, error) {
		info, ok := workflow.StepInfoFrom(ctx)
		if !ok {
			return nil, workflow.Permanent(errors.New("not called from a step"))
		}
		return fmt.Appendf(nil, "%s/%s, run %s, attempt %d", info.Workflow, info.Step, info.RunID, info.Attempt), nil
	}
	_, err = report(ctx, nil)
	fmt.Println("  outside a step:", err)
	stepCtx := workflow.WithStepInfo(ctx, workflow.StepInfo{Workflow: "report", Step: "send", RunID: "run-1", Attempt: 2})
	out, err = report(stepCtx, nil)
	if err != nil {
		return err
	}
	fmt.Printf("  inside a step: %s\n", out)
	// snippet:end
	return nil
}
