# Workflows and steps

A workflow is a named graph of steps plus the policies that apply to every run of it (retries, timeouts, concurrency, cron). A one-step workflow is a plain background job; several steps make a pipeline, a fan-out and fan-in, or a process that waits for the outside world. The same definition runs on every driver.

## Before you start

- A client to register the workflow on ([`Register`](ds:block?id=memory-client-register-ngcr2sck) on the memory driver). The [quickstart](../start/quickstart.md) builds one on the memory driver.
- The example is [`examples/signals-and-sleep`](../../examples/signals-and-sleep). It registers four workflows on the memory driver with a fake clock: an order with a wait and a sleep, an approval that times out, a quote whose last step joins two parallel ones, and an export whose step times out. Run it with `go run ./signals-and-sleep` from `examples/`.

## Define a workflow

`workflow.Workflow` has a `Name`, which jobs and schedules refer to, and `Steps`. Each `workflow.Step` has a `Name`, unique in the workflow, and exactly one kind:

| Field | Kind | Page |
|---|---|---|
| `Run` | Calls a `workflow.Handler` | this page |
| `Wait` | Suspends until a matching event or a deadline | [signals](signals.md) |
| `Sleep` | Suspends for a duration | [sleep](sleep.md) |

A [`workflow.Handler`](ds:block?id=handler-82jhj4f7) is `func(ctx context.Context, in []byte) ([]byte, error)`: it gets the step's input as bytes and returns the step's output, which the engine stores. `Register` validates the definition ([`workflow.Validate`](ds:block?id=validate-mnthpzkp): unique step names, exactly one kind per step, known parents, no cycles) and fails with an error matching [`workflow.ErrInvalid`](ds:block?id=errinvalid-y3xxpcy2), so a broken workflow fails at start-up rather than mid-run.

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

Register every workflow, then call `Start` once; `Register` after `Start` returns [`workflow.ErrAlreadyStarted`](ds:block?id=erralreadystarted-prauyy24) ([memory `Register`](ds:block?id=memory-client-register-ngcr2sck)).

## Typed handlers with Define

Working with bytes in every step is tedious. [`workflow.Define`](ds:block?id=define-d3ttbgk3) wraps a typed function into a `Handler`: it decodes the input from JSON into `In` and encodes the `Out` it returns as JSON. Empty input decodes to the zero `In`. Input that does not decode is a [`workflow.Permanent`](ds:block?id=permanent-9e6vjm6s) error, so it is not retried: the same bytes can never decode.

The `reserve` and `ship` steps above are both `Define` handlers: `reserve` takes and returns an `order`, `ship` takes a `payment`. A `Define` handler is a plain `Handler`, so a unit test can call it with bytes and no driver at all:

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

The output is the JSON encoding of what the function returned, so the string comes back quoted. An output that does not encode (a channel, a func) is a `workflow.Permanent` error too. An error your function returns is passed on as it is, so the step's retry policy decides about it.

## Steps that depend on other steps

`Step.After` names the steps that must succeed before this one starts. Steps with no `After` start with the run, in parallel, so a plain list with no `After` is not a sequence: chain the steps with `After` to run them in order.

A step's input follows one rule, [`workflow.StepInput`](ds:block?id=stepinput-ykz7ud5d), on every driver:

- no parents: the run's payload;
- one parent: that parent's output, unchanged;
- several parents: a JSON object keyed by parent name, each output embedded as JSON when it is valid JSON and as a JSON string otherwise (a missing or empty output is `null`).

So a step joining two parents can take a struct whose fields are named after them:

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

with `quoteIn` declared as:

<!-- snippet: examples/signals-and-sleep/main.go#quote-in -->
```go

// quoteIn is the input of a step with two parents: StepInput builds a JSON
// object keyed by parent name, which Define decodes into these fields.
type quoteIn struct {
	Price int `json:"price"` // the output of step "price"
	Stock int `json:"stock"` // the output of step "stock"
}
```

When a step fails for good, the run's other steps are cancelled and the run fails ([the memory driver's `fail`](ds:block?id=memory-client-fail-6tqmatzt)).

## What a step knows about its run

Every driver puts a [`workflow.StepInfo`](ds:block?id=stepinfo-kun3n4c3) on the ctx it passes to a handler: the workflow and step names, the `RunID`, the `Attempt` (1 for the first, one more per retry) and the run's `Labels`. Read it with [`workflow.StepInfoFrom(ctx)`](ds:block?id=stepinfofrom-rr9mwh2c); the second result is false outside a step (in an enqueue middleware, for example). The `quote` step above prints its workflow, step and attempt from it. Handlers and middleware must not modify `Labels`; it is the run's own map.

`StepInfo` carries more for middleware: the run's `Queue`, its `Tenant`, the `Node` running the attempt, the workflow `Version`, and `ReadyAt`, when the attempt became ready to claim (now minus `ReadyAt` is how long it waited for a worker). Each is empty or zero on a driver without the matching capability.

[`workflow.WithStepInfo`](ds:block?id=withstepinfo-t26f3eu3) is the setter. Drivers call it before running a step, before the middleware chain; you need it only when writing a driver, or a test that calls a handler directly. The program calls a handler that reads its `StepInfo`, once outside a step and once with a `StepInfo` set:

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

## Timeouts

`Step.Timeout` caps one attempt of a step; `Workflow.Timeout` is the default for steps that leave it zero. Zero everywhere means no timeout. When an attempt runs past its timeout its ctx is cancelled and the attempt fails with an error matching `context.DeadlineExceeded` and [`workflow.ErrStepTimeout`](ds:block?id=errsteptimeout-6b727264) (`context.Cause(ctx)` in the handler is the step timeout; [the memory driver's attempt](ds:block?id=memory-client-exec-g88wsrmp)); the retry policy then applies as for any failure.

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

A timeout only helps a step that watches its ctx: pass `ctx` to every call that takes one.

## Run a handler when a run fails

`Workflow.OnFailure` runs once when a run fails for good, after its retries, with the input of the step that failed. Use it to release what the run held, tell a person, or record the failure:

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

In the example the approval's wait times out after 72 hours, and `OnFailure` receives the run's input, `"expense 77"`. `OnFailure` does not run for a cancelled run ([`fail`](ds:block?id=memory-client-fail-6tqmatzt) skips it for a run a Cancel already ended), and a set `OnTimeout` takes its place when the run failed by a timeout ([`workflow.FailureHandler`](ds:block?id=failurehandler-numvgfuc)).

## Try it

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

`steps that depend on other steps` is the fan-in above, `step timeout` the export, and `call a handler directly` the `Define` and `StepInfo` sections; the wait, sleep and key lines belong to [signals](signals.md) and [sleep](sleep.md).

## Pitfalls

- **Steps without `After` run in parallel.** Two steps listed one after the other both start with the run (a step is ready once every `After` parent has an output: [`readyLocked`](ds:block?id=memory-run-readylocked-amuukcsg)). Chain them with `After`.
- **`OnFailure` runs at least once on a durable driver.** On the SQL driver it runs under a lease, so a crash reruns it on another process ([`recoverFailing`](ds:block?id=sql-client-recoverfailing-q3wafxbw)); make it idempotent.
- **On Hatchet, set `Step.Timeout` near the step's real duration.** A step whose worker process dies is reclaimed only when its execution timeout expires (`Config.StepTimeout`, [24 hours](ds:block?id=hatchet-defaultsteptimeout-j3gvwcvw) by default; a step with its own timeout gets [that timeout plus a few seconds](ds:block?id=hatchet-enginetimeout-cuw8khf9)) and then retried, if it has a retry left. A wait step's execution timeout follows its `Timeout` or `Deadline` instead, and a wait with neither ends after `Config.StepTimeout` ([`waitTimeout`](ds:block?id=hatchet-waittimeout-cwq36qj9)) ([Hatchet limitations](../drivers/hatchet.md#limitations)).
- **Read time from an injected clock in handlers**, never from `time.Now`, so a test or a replay sees the same values (the example reads its fake clock).
- **The graph is fixed at Register.** A running workflow cannot gain steps: an engine declares the graph server-side and the SQL driver finds steps by name after a crash, so steps added at run time would have no code on the node that resumes them. For a fan-out decided at run time, enqueue child runs or a [batch](../batches/batches.md) from a step and wait for them.
- **A `Define` input that does not decode is never retried.** It fails the step for good at once ([`Define`](ds:block?id=define-d3ttbgk3) wraps the decode error in `Permanent`), so a payload whose shape changed fails runs instead of retrying them; version the payload type or accept both shapes.
- **[`StepInfoFrom`](ds:block?id=stepinfofrom-rr9mwh2c) is false outside a step.** An enqueue middleware, or a helper called from `main`, gets the zero `StepInfo`; check the second result.
- **Outputs are stored.** On a durable driver every step's output is written to the database or engine; keep outputs small, or move big ones out with a [claim check](../reliability/claim-check.md).

## Related

- [Wait for an event](signals.md), [sleep](sleep.md)
- [Retries](../jobs/retries.md)
- [Middleware](../middleware/step-middleware.md), which wraps every step
- Example: [`examples/signals-and-sleep`](../../examples/signals-and-sleep)
