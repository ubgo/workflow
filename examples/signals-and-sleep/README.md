# signals-and-sleep

Workflows of several steps: a wait for an event matched to its run, a durable sleep, a wait that gives up after a timeout and runs `OnFailure`, two parallel steps joined by a third, a step timeout, the per-run key form each driver refuses, the key a wait step resolves, and handlers called directly (a typed `Define` handler and one reading its `StepInfo`). It runs on the in-memory driver with a fake clock that starts on Monday at 09:00 UTC, so a day's sleep and a three-day wait pass when the program moves the clock.

```sh
cd examples
go run ./signals-and-sleep
```

```text
wait and sleep:
  both orders waiting for payment
  paid A-2 at Mon 09:00: A-2 waiting (sleeping), A-1 waiting
  ship A-2 (paid 40) at Tue 09:00
  A-2 succeeded, A-1 still waiting
  ship A-1 (paid 40) at Wed 09:00
give up after a timeout:
  on failure: escalate "expense 77" at Sat 09:00
  run failed, deadline exceeded true
steps that depend on other steps:
  quote/quote attempt 1: price 120, stock 7
step timeout:
  run failed: memory: step "write" failed after 1 attempt(s): workflow: step attempt timed out: memory: step "write" attempt 1 after 30s: context deadline exceeded
refusals:
  memory: workflow: WaitSpec.KeyFrom.Expr unsupported by this driver: the memory driver evaluates only KeyFunc keys; workflow order, step await-payment
  hatchet: workflow: WaitSpec.KeyFrom.Expr unsupported by this driver: workflow order step await-payment: Hatchet has no API that evaluates a CEL expression to a string for the driver; use workflow.KeyFunc
wait keys:
  order A-7 waits on "A-7", every approval on "finance"
  an expression is the engine's to evaluate: true
call a handler directly:
  typed: "hello Ada"
  empty input: "hello "
  bad input is permanent: true
  outside a step: not called from a step
  inside a step: report/send, run run-1, attempt 2
```

## The code

An order that waits for its payment, sleeps a day, then ships:

<!-- snippet: examples/signals-and-sleep/main.go#order -->
```go
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
```

Two orders, each waiting on its own key:

<!-- snippet: examples/signals-and-sleep/main.go#enqueue-orders -->
```go
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
```

The webhook's signal resumes one of them:

<!-- snippet: examples/signals-and-sleep/main.go#signal -->
```go
err := c.Signal(ctx, workflow.Event{
	Name:    "payment.succeeded",
	Key:     "A-2", // resumes only the run waiting on A-2
	Payload: []byte(`{"Order":"A-2","Paid":40}`),
})
if err != nil {
	return err
}
```

An approval that gives up after 72 hours:

<!-- snippet: examples/signals-and-sleep/main.go#approval -->
```go
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
```

Two parallel steps joined by a third:

<!-- snippet: examples/signals-and-sleep/main.go#fan-in -->
```go
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
```

The joined step's input:

<!-- snippet: examples/signals-and-sleep/main.go#quote-in -->
```go

// quoteIn is the input of a step with two parents: StepInput builds a JSON
// object keyed by parent name, which Define decodes into these fields.
type quoteIn struct {
	Price int `json:"price"` // the output of step "price"
	Stock int `json:"stock"` // the output of step "stock"
}
```

A step timeout:

<!-- snippet: examples/signals-and-sleep/main.go#timeout -->
```go
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
```

A per-run key expression, refused by both drivers:

<!-- snippet: examples/signals-and-sleep/main.go#refusal -->
```go
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
```

The key a wait step waits on, as a driver resolves it when the step begins:

<!-- snippet: examples/signals-and-sleep/main.go#resolve-key -->
```go
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
```

A typed handler, called directly:

<!-- snippet: examples/signals-and-sleep/main.go#define -->
```go
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
```

A handler reading the `StepInfo` a driver puts on its ctx, and the same handler outside a step:

<!-- snippet: examples/signals-and-sleep/main.go#step-info -->
```go
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
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Workflows and steps](../../docs/workflows/steps.md)
- [Wait for an event](../../docs/workflows/signals.md)
- [Sleep inside a workflow](../../docs/workflows/sleep.md)
