# Idempotency keys

The same job often arrives twice: a client retries an HTTP request that timed out, a broker redelivers a message, a user double-clicks "pay". An idempotency key makes the second enqueue a no-op: while a run holding the key has not finished, `Enqueue` with the same key starts nothing and returns that run's `RunRef`.

## Before you start

- A driver with `workflow.CapIdempotencyKey`. The memory, SQL, Redis and Hatchet drivers all have it.
- The example is [`examples/idempotency`](../../examples/idempotency). Run it with `go run ./idempotency` from `examples/`.

## Idempotency keys

Set `Job.IdempotencyKey` to something that names the work, not the request: the invoice for order 42 is `invoice:order-42` however many times it is asked for.

<!-- snippet: examples/idempotency/main.go#key -->
```go
job := workflow.Job{Workflow: "invoice", IdempotencyKey: "invoice:order-42"}
first, err := c.Enqueue(ctx, job)
if err != nil {
	return err
}
second, err := c.Enqueue(ctx, job) // a retried HTTP request, a redelivered message
if err != nil {
	return err
}
fmt.Fprintln(out, "second enqueue returned the first run:", second == first)
```

The second `Enqueue` returns the first run's `RunRef` and a nil error: the caller cannot tell, and does not need to, whether it started the run or found it. Keys are scoped to the workflow, so `invoice:order-42` on two workflows is two keys.

The key is held until the run is terminal (succeeded, failed or cancelled). After that the same key starts a new run, which is what you want for "rebuild the report" but not for "charge the card":

<!-- snippet: examples/idempotency/main.go#released -->
```go
third, err := c.Enqueue(ctx, job) // the first run has finished: the key is free
if err != nil {
	return err
}
fmt.Fprintln(out, "after the run finished, a new run:", third != first)
```

## Require or ignore keys

Each workflow says how keys are treated, with `Workflow.Idempotency`:

| Policy | Effect |
|---|---|
| `IdempotencyOptional` (the default, also the empty value) | A key is honoured when the job has one |
| `IdempotencyRequired` | An enqueue without a key fails with an error matching `workflow.ErrInvalid` |
| `IdempotencyOff` | Keys are ignored: every enqueue is a run |

<!-- snippet: examples/idempotency/main.go#policies -->
```go
workflows := []workflow.Workflow{
	{
		Name: "invoice", // Idempotency empty: IdempotencyOptional, a key is honoured when given
		Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
			<-release // keep the run unfinished until the program says so
			return nil, nil
		}}},
	},
	{
		Name:        "payout",
		Idempotency: workflow.IdempotencyRequired, // an enqueue without a key fails
		Steps:       []workflow.Step{{Name: "pay", Run: noop}},
	},
	{
		Name:        "ping",
		Idempotency: workflow.IdempotencyOff, // keys are ignored: every enqueue is a run
		Steps:       []workflow.Step{{Name: "ping", Run: noop}},
	},
}
```

Require a key on work that must never run twice by accident (payouts, charges), so a caller that forgot one fails in development:

<!-- snippet: examples/idempotency/main.go#required -->
```go
_, err = c.Enqueue(ctx, workflow.Job{Workflow: "payout"})
fmt.Fprintln(out, "payout without a key:", errors.Is(err, workflow.ErrInvalid))
```

Turn keys off on work where a shared key would be a bug, so a caller that copies a key from another job does not suppress runs:

<!-- snippet: examples/idempotency/main.go#off -->
```go
a, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping", IdempotencyKey: "same"})
if err != nil {
	return err
}
b, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping", IdempotencyKey: "same"})
if err != nil {
	return err
}
fmt.Fprintln(out, "ping with the same key twice, two runs:", a != b)
```

A key is never dropped silently. A driver without `CapIdempotencyKey` refuses a job that carries one, even under `IdempotencyOff`, because the key is the caller's request; and `IdempotencyRequired` makes `Start` refuse such a driver. The example uses the test `Fake`, told to claim every capability but this one, to stand for such a driver:

<!-- snippet: examples/idempotency/main.go#refusal -->
```go
all := memory.New().Capabilities()
f := workflowtest.NewFake(workflowtest.FakeCaps(all &^ workflow.CapIdempotencyKey))
noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
if err := f.Register(workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{Name: "send", Run: noop}}}); err != nil {
	return err
}
if err := f.Start(ctx); err != nil {
	return err
}
_, err := f.Enqueue(ctx, workflow.Job{Workflow: "invoice", IdempotencyKey: "invoice:order-42"})
fmt.Fprintln(out, "no CapIdempotencyKey:", err)
```

## Once per period

Some work must happen once per customer per period: charge customer 42 once a month, send one weekly digest per user. `workflow.OncePerPeriod(job, scope, period, t, loc)` returns the job with the idempotency key `period/<scope>/<period key>`, so every enqueue of the same scope in the same period, from any node and any number of schedule fires, carries the same key. The example is [`examples/upstream-limits`](../../examples/upstream-limits):

<!-- snippet: examples/upstream-limits/main.go#period -->
```go
job, err := workflow.OncePerPeriod(workflow.Job{Workflow: "monthly-charge"}, "customer-42", workflow.PeriodMonth, at, berlin)
if err != nil {
	return err
}
first, err := c.Enqueue(ctx, job)
if err != nil {
	return err
}
second, err := c.Enqueue(ctx, job) // a second fire in the same month, from any node
if err != nil {
	return err
}
```

The period key comes from `workflow.PeriodKey(t, period, loc)`: `"2026-11-01"` for `workflow.PeriodDay`, `"2026-W44"` for `PeriodWeek` (the ISO 8601 week, Monday first, so the last days of December can belong to week 1 of the next year), `"2026-11"` for `PeriodMonth`, `"2026-Q4"` for `PeriodQuarter` and `"2026"` for `PeriodYear`. It is read on `loc`'s wall clock, which is required: `nil` and `time.Local` are refused with `ErrInvalid`, because a period boundary depends on the zone and the host's zone differs from machine to machine. In the example, 23:30 UTC on 31 October is already November in Berlin. `OncePerPeriod` refuses an empty scope, and a job that already has an `IdempotencyKey` rather than overwriting it, both with `ErrInvalid`; it needs `CapIdempotencyKey` like any key.

The key dedups only while the run holding it is unfinished, as every idempotency key does. To keep a later enqueue in the same period from charging again after the first run succeeded, also pass the period key to the payment provider's own idempotency key (`PeriodKey` alone is the stable value for that), and wrap the charge in [`workflow.Memo`](../workflows/memoisation.md#memoise-a-call-with-memo) so a retried step does not charge twice.

## Try it

```sh
cd examples
go run ./idempotency
```

```text
second enqueue returned the first run: true
after the run finished, a new run: true
payout without a key: true
ping with the same key twice, two runs: true
no CapIdempotencyKey: workflow: Job.IdempotencyKey unsupported by this driver (missing IdempotencyKey): job for workflow invoice
```

## Pitfalls

- **A key dedups while the run is unfinished, not forever.** Once the run is terminal the key is free. For "at most once per order, ever", record the fact in your own database inside the step, or use [unique jobs](../reliability/unique-jobs.md) with a time window.
- **A key dedups within one backend.** Two clients on different backends (a [failover](../reliability/failover.md) pair, two databases) each honour the key on their own; that is why the failover client requires a key and dedups only as well as each side does.
- **Hatchet holds a key for at most `Config.IdempotencyMaxHold`**, and refuses `IdempotencyKey` together with `RunAt`.
- **On the SQL driver the key is a unique index**, so it holds across every process sharing the database; on the memory driver it holds within the process.

## Related

- [Enqueue a job](enqueue.md)
- [Unique jobs](../reliability/unique-jobs.md) and [debounce](../reliability/debounce.md), which dedup above the driver
- [Transactional enqueue](../reliability/transactional-enqueue.md)
- Example: [`examples/idempotency`](../../examples/idempotency)
