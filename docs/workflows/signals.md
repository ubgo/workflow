# Wait for an event

Many processes stop and wait for the outside world: an order waits for its payment, an expense for an approval, a sign-up for an email click. A wait step suspends the run, holding no worker, until your app sends a matching event with `Client.Signal`, or until a deadline passes. The event's payload becomes the wait step's output, so the next step reads what arrived.

## Before you start

- A driver with `workflow.CapWaitForEvent`. The memory, SQL, Redis and Hatchet drivers have it; whether a wait survives a restart is `CapDurable` (SQL, Redis and Hatchet yes, memory no).
- Workflows and steps: [steps](steps.md).
- The example is [`examples/signals-and-sleep`](../../examples/signals-and-sleep). It runs on the memory driver with a fake clock. Run it with `go run ./signals-and-sleep` from `examples/`.

## Wait for an event

A wait step sets `Step.Wait` to a `workflow.WaitSpec` naming the `Event` to wait for. In the order workflow, `await-payment` waits for `payment.succeeded` after `reserve`, and `ship` reads the payment the event carried (through a [sleep](sleep.md), which passes its input on):

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

Your app delivers the event with `Client.Signal`, typically from the payment provider's webhook handler. A `workflow.Event` has a `Name`, a `Key` and a `Payload`:

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

The contract every driver keeps, checked by the shared conformance suite:

- **An event is kept from the run's enqueue.** A run whose wait step begins after the `Signal` still takes it, so a fast webhook that arrives before the run reaches its wait is not lost.
- **An event sent before a run was enqueued never resumes that run.**
- **One run never takes the same event twice**, so two wait steps on the same name and key need two events.
- **One event resumes every run waiting with that name and key**, not only the first.
- **A signal nobody waits for is not an error.**

## Match the event to the run

Many runs of one workflow wait on the same event name for different things: "payment succeeded" for *this* order. The key says which. A run's wait matches an event with the same name and the same key, and there are two ways to give the key:

- `WaitSpec.Key` is a fixed key, the same for every run of the workflow (the approval example waits for `expense.approved` with key `finance`). Empty matches an event with an empty key.
- `WaitSpec.KeyFrom` derives the key per run from the run's job. The driver evaluates it once, when the wait step begins, against the run's original `Payload` and `Labels` (not the step's input). The order workflow takes it from the `order` label:

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

`KeyFrom` is a `workflow.ConcurrencyKey`, the same type concurrency keys use: `workflow.KeyFunc` for a Go function, `workflow.KeyExpr` for an expression in the engine's language. At most one of `Key` and `KeyFrom` is set. `WaitSpec.ResolveKey(job)` returns the key a run waits on (the function's result, or the fixed key), and returns an error matching `workflow.ErrUnsupported` for an expression, which is the engine's to evaluate.

In the example, both orders wait; the event for `A-2` resumes only `A-2`, and `A-1` waits until its own payment arrives.

## Give up after a deadline

A wait with no limit waits forever. Set one of:

- `WaitSpec.Timeout`: how long the wait lasts from when it begins;
- `WaitSpec.Deadline`: the absolute time it ends.

At most one of the two may be set. A wait that reaches its limit fails the run with an error matching `context.DeadlineExceeded`; waits are not retried, so the run fails for good and `OnFailure` runs:

<!-- snippet: examples/signals-and-sleep/main.go#approval -->
```go
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
```

The approval is enqueued on Wednesday at 09:00 (the orders above moved the fake clock two days); with no approval after 72 hours, `OnFailure` escalates it on Saturday at 09:00.

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
```

While a run waits, `Status` reports `StatusWaiting`.

## Pitfalls

- **The key form depends on the driver.** The memory, SQL and Redis drivers evaluate only `KeyFunc` and refuse a `KeyExpr` at `Start`; the Hatchet driver refuses a `KeyExpr` too (it has no API that evaluates CEL to a string for the driver) and takes `KeyFunc`, which its worker evaluates when the wait step begins. So `KeyFunc` is the portable form:

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
fmt.Fprintln(out, "  memory:", m.Start(ctx))
m.Stop()
fmt.Fprintln(out, "  hatchet:", hatchet.New(hatchet.Config{}).Register(byExpr))
```

- **On the memory driver a wait does not survive a restart.** The run, its wait and any kept events are in the process. Use the SQL driver or Hatchet for a wait that may last days.
- **Kept events are pruned.** Events older than the oldest unfinished run are dropped, since no run can take them; an event is not a durable message queue.
- **A wait outputs the event payload.** The step after a wait reads the event, not what the step before the wait returned. Carry what you need through the job's payload or labels, or have the event include it.

## Related

- [Sleep inside a workflow](sleep.md), [steps](steps.md)
- [Concurrency keys](../jobs/concurrency.md#limit-runs-per-key), the same key type
- Example: [`examples/signals-and-sleep`](../../examples/signals-and-sleep)
