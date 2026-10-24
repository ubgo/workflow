# Sleep inside a workflow

Some processes need to pause between steps: a 24-hour cooling-off period before an order ships, a follow-up email three days after a trial starts, a check that runs again in an hour. A sleep step suspends the run for a duration without holding a worker, so a thousand sleeping runs cost a thousand timers or rows, not a thousand workers blocked in `time.Sleep`.

## Before you start

- A driver with `workflow.CapDurableSleep`. The memory, SQL, Redis and Hatchet drivers have it; whether a sleep survives a restart is `CapDurable` (SQL and Hatchet yes, memory no).
- Workflows and steps: [steps](steps.md).
- The example is [`examples/signals-and-sleep`](../../examples/signals-and-sleep). It runs on the memory driver with a fake clock, so a 24-hour sleep ends when the program moves the clock. Run it with `go run ./signals-and-sleep` from `examples/`.

## Sleep inside a workflow

A sleep step sets `Step.Sleep` to a positive duration and nothing else: it is the third step kind (`workflow.StepSleep`), beside `Run` and `Wait`. It passes its input through unchanged, so the step after it reads what the step before it produced. In the order workflow, `cool-off` sleeps a day between the payment and shipping, and `ship` reads the payment event that `await-payment` output:

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

While a run sleeps, `Status` reports `StatusWaiting` and no worker is used. In the example, `A-2` is paid on Monday at 09:00, sleeps, and ships on Tuesday at 09:00 when the program moves the fake clock by a day:

```text
  paid A-2 at Mon 09:00: A-2 waiting (sleeping), A-1 waiting
  ship A-2 (paid 40) at Tue 09:00
```

A sleep is measured on the driver's clock: the injected `clock.Clock` on the memory, SQL and Redis drivers (`WithClock`), which is what lets a test pass `clock.NewFake` and make "sleep three days" take microseconds.

## Try it

```sh
cd examples
go run ./signals-and-sleep
```

The `wait and sleep` part of the output:

```text
wait and sleep:
  both orders waiting for payment
  paid A-2 at Mon 09:00: A-2 waiting (sleeping), A-1 waiting
  ship A-2 (paid 40) at Tue 09:00
  A-2 succeeded, A-1 still waiting
  ship A-1 (paid 40) at Wed 09:00
```

## Pitfalls

- **Never sleep inside a `Run` handler.** `time.Sleep` in a handler holds a worker for the whole duration, ignores the fake clock, and is lost on a crash. Split the work into two steps with a sleep step between them.
- **On the memory driver a sleep does not survive a restart.** The run is lost with the process. Use the SQL driver or Hatchet for sleeps measured in hours or days.
- **A negative `Sleep` fails validation**, and a step that sets `Sleep` together with `Run` or `Wait` is invalid: exactly one kind per step.
- **A sleep is relative to when the step begins**, not to the run's enqueue. For "at 09:00 tomorrow", enqueue a [delayed job](../jobs/delays.md) with `RunAt`, or use a [cron schedule](../schedules/cron.md).

## Related

- [Wait for an event](signals.md), [steps](steps.md)
- [Fake clock](../testing/fake-clock.md)
- Example: [`examples/signals-and-sleep`](../../examples/signals-and-sleep)
