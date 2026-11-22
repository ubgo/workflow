# Idempotency keys

The same job often arrives twice: a client retries an HTTP request that timed out, a broker redelivers a message, a user double-clicks "pay". An idempotency key makes the second enqueue a no-op: while a run holding the key has not finished, `Enqueue` with the same key starts nothing and returns that run's `RunRef` (on memory, [`enqueueLocked`](ds:block?id=memory-client-enqueuelocked-z3x2cytk)).

## Before you start

- A driver with `workflow.CapIdempotencyKey`. The [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) drivers all have it.
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
fmt.Println("second enqueue returned the first run:", second == first)
```

The second `Enqueue` returns the first run's `RunRef` and a nil error: the caller cannot tell, and does not need to, whether it started the run or found it. Keys are scoped to the workflow, so `invoice:order-42` on two workflows is two keys (the [memory](ds:block?id=memory-client-enqueuelocked-z3x2cytk) driver keys its index by workflow and key, the [SQL](ds:block?id=sql-tabledefs-cgz8s9k6) driver has a unique index on both).

The key is held until the run is terminal (succeeded, failed or cancelled; the SQL driver's `idem_active` column in [`tableDefs`](ds:block?id=sql-tabledefs-cgz8s9k6) is cleared then). After that the same key starts a new run, which is what you want for "rebuild the report" but not for "charge the card":

<!-- snippet: examples/idempotency/main.go#released -->
```go
third, err := c.Enqueue(ctx, job) // the first run has finished: the key is free
if err != nil {
	return err
}
fmt.Println("after the run finished, a new run:", third != first)
```

## Require or ignore keys

Each workflow says how keys are treated, with `Workflow.Idempotency`:

| Policy | Effect |
|---|---|
| [`IdempotencyOptional`](ds:block?id=idempotencyoptional-4p96k58e) (the default, also the empty value) | A key is honoured when the job has one |
| [`IdempotencyRequired`](ds:block?id=idempotencyrequired-g889m7p8) | An enqueue without a key fails with an error matching `workflow.ErrInvalid` (on memory, [`enqueueLocked`](ds:block?id=memory-client-enqueuelocked-z3x2cytk)) |
| [`IdempotencyOff`](ds:block?id=idempotencyoff-n73f93jz) | Keys are ignored: every enqueue is a run |

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
fmt.Println("payout without a key:", errors.Is(err, workflow.ErrInvalid))
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
fmt.Println("ping with the same key twice, two runs:", a != b)
```

A key is never dropped silently. A driver without `CapIdempotencyKey` refuses a job that carries one, even under `IdempotencyOff`, because the key is the caller's request ([`RequiresJob`](ds:block?id=requiresjob-mrfs7vqd)); and `IdempotencyRequired` makes `Start` refuse such a driver ([`Requires`](ds:block?id=requires-6tcjbcfj)). The example uses the test `Fake`, told to claim every capability but this one, to stand for such a driver:

<!-- snippet: examples/idempotency/main.go#refusal -->
```go
all := memory.New().Capabilities()
f := workflowtest.NewFake(workflowtest.FakeCaps(all.Without(workflow.CapIdempotencyKey)))
noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
if err := f.Register(workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{Name: "send", Run: noop}}}); err != nil {
	return err
}
if err := f.Start(ctx); err != nil {
	return err
}
_, err := f.Enqueue(ctx, workflow.Job{Workflow: "invoice", IdempotencyKey: "invoice:order-42"})
fmt.Println("no CapIdempotencyKey:", err)
```

## Once per period

Some work must happen once per customer per period: charge customer 42 once a month, send one weekly digest per user. [`workflow.OncePerPeriod`](ds:block?id=onceperperiod-6wfss2ur)`(job, scope, period, t, loc)` returns the job with the idempotency key `period/<scope>/<period key>`, so every enqueue of the same scope in the same period, from any node and any number of schedule fires, carries the same key. The example is [`examples/upstream-limits`](../../examples/upstream-limits):

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

The period key comes from [`workflow.PeriodKey`](ds:block?id=periodkey-jn3g3p9u)`(t, period, loc)`: `"2026-11-01"` for `workflow.PeriodDay`, `"2026-W44"` for `PeriodWeek` (the ISO 8601 week, Monday first, so the last days of December can belong to week 1 of the next year), `"2026-11"` for `PeriodMonth`, `"2026-Q4"` for `PeriodQuarter` and `"2026"` for `PeriodYear`. It is read on `loc`'s wall clock, which is required: `nil` and `time.Local` are refused with `ErrInvalid`, because a period boundary depends on the zone and the host's zone differs from machine to machine. In the example, 23:30 UTC on 31 October is already November in Berlin. [`OncePerPeriod`](ds:block?id=onceperperiod-6wfss2ur) refuses an empty scope, and a job that already has an `IdempotencyKey` rather than overwriting it, both with `ErrInvalid`; it needs `CapIdempotencyKey` like any key.

The key dedups only while the run holding it is unfinished, as every idempotency key does. To keep a later enqueue in the same period from charging again after the first run succeeded, also pass the period key to the payment provider's own idempotency key ([`PeriodKey`](ds:block?id=periodkey-jn3g3p9u) alone is the stable value for that), and wrap the charge in [`workflow.Memo`](../workflows/memoisation.md#memoise-a-call-with-memo) so a retried step does not charge twice.

## Drop redelivered messages with an inbox

An idempotency key drops a duplicate enqueue while the run holding it is unfinished. A message source that delivers at least once (a broker redelivering after a lost ack, a webhook sender retrying, an outbox relay resending after a crash) can deliver the same message long after that, and code that consumes messages without enqueueing a run has no key at all. The inbox records the IDs each consumer has processed, for as long as you choose, so the work runs once per message ID. `workflow.InboxConsumer` runs a function once per ID ([`InboxConsumer.Once`](ds:block?id=inboxconsumer-once-mta79w6e)):

<!-- snippet: examples/consumer-inbox/main.go#consumer -->
```go
mailer := workflow.InboxConsumer{
	Inbox:    workflow.NewMemoryInbox(), // sqlstore.Inbox or redis.Inbox across processes
	Consumer: "receipt-mailer",
	Keep:     7 * 24 * time.Hour, // remember processed IDs longer than any redelivery
}
for range 3 { // an at-least-once source delivers msg-1 three times
	ran, err := mailer.Once(ctx, "msg-1", sendReceipt)
	if err != nil {
		return err // nack: the message comes back
	}
	fmt.Println("  msg-1 ran:", ran)
}
```

[`Once`](ds:block?id=inboxconsumer-once-mta79w6e) claims the ID for a lease (`Lease`, default [`DefaultInboxLease`](ds:block?id=defaultinboxlease-s3zfehbn), five minutes), runs the function, and marks the ID processed for `Keep` (default [`DefaultInboxKeep`](ds:block?id=defaultinboxkeep-kzbwa4qr), seven days). A redelivery of a processed ID returns false with no error: drop it. When the function fails, the claim is released, so the next delivery runs it again. An ID another worker is processing returns [`ErrInboxBusy`](ds:block?id=errinboxbusy-qtu9arf9), which is not permanent: nack it and it comes back. Each consumer name keeps its own record, so a mailer and an indexer reading one message each process it once. Times come from `InboxConsumer.Clock` (the real clock when nil), so a fake clock drives the keep window in tests.

[`workflow.MemoryInbox`](ds:block?id=memoryinbox-c3zuf7tj) keeps the record in one process. For a fleet, use `sqlstore.Inbox` ([`NewInbox`](ds:block?id=sql-newinbox-rz7tpwct)`(db, dialect)` or `Client.Inbox()`, a row per consumer and message in `<prefix>inbox`, its own migration list, expired rows pruned by the dispatcher's housekeeping or [`PruneExpired`](ds:block?id=sql-inbox-pruneexpired-s434c38w)) or `redis.Inbox` ([`NewInbox`](ds:block?id=redis-newinbox-dk4wc654)`(rdb, prefix)` or `Client.Inbox()`, a hash per message, each step one script). Of two workers claiming one message on either, exactly one wins (the [`Inbox`](ds:block?id=inbox-kq8jz7jg) contract).

`Once` is at least once: a worker that dies after the work and before the record is saved leaves its claim to expire, and the redelivery runs the work again. When the work writes to the database the inbox lives in, make it exactly once with [`sqlstore.Inbox.MarkTx`](ds:block?id=sql-inbox-marktx-s8saadjk), which records the message inside your own transaction, so the record and your writes commit or roll back together:

<!-- snippet: examples/consumer-inbox/main.go#marktx -->
```go
handle := func(ctx context.Context, msgID string, total int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // a no-op after Commit
	key := workflow.InboxKey{Consumer: "order-writer", ID: msgID}
	fresh, err := inbox.MarkTx(ctx, tx, key, 7*24*time.Hour, time.Now())
	if err != nil || !fresh {
		return err // seen before: nothing to do
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO orders (msg_id, total) VALUES (?, ?)", msgID, total); err != nil {
		return err
	}
	return tx.Commit() // the order and the record commit together
}
```

[`MarkTx`](ds:block?id=sql-inbox-marktx-s8saadjk) returns false for a message already recorded, and runs its insert under a savepoint, so a duplicate never aborts your transaction. Of two transactions marking one message, the second waits for the first and then finds the record (on Postgres and MySQL; on SQLite it cannot begin writing until the first ends). An inbound webhook takes an inbox too (see [webhooks](../events/webhooks.md#drop-redeliveries-with-an-inbox)). A store of your own implements [`workflow.Inbox`](ds:block?id=inbox-kq8jz7jg) (`Claim`, `Complete`, `Release`, each given the caller's `now`); [`workflowtest.InboxContract`](ds:block?id=workflowtest-inboxcontract-94cnbp8v)`(t, a, b)` checks it against the contract the shipped stores pass.

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

- **A key dedups while the run is unfinished, not forever.** Once the run is terminal the key is free ([`enqueueLocked`](ds:block?id=memory-client-enqueuelocked-z3x2cytk), [`tableDefs`](ds:block?id=sql-tabledefs-cgz8s9k6)). For "at most once per order, ever", record the fact in your own database inside the step, or use [unique jobs](../reliability/unique-jobs.md) with a time window.
- **A key dedups within one backend.** Two clients on different backends (a [failover](../reliability/failover.md) pair, two databases) each honour the key on their own; that is why the failover client requires a key and dedups only as well as each side does.
- **Hatchet holds a key until the run's first step ends, and at most `Config.IdempotencyMaxHold`** (default [`DefaultIdempotencyMaxHold`](ds:block?id=hatchet-defaultidempotencymaxhold-9vxvg6ux), a day). The engine releases a key when the first task created with the run ends, so during a multi-step run's later steps the same key starts a second run; use [unique jobs](../reliability/unique-jobs.md) when the hold must last until the run ends. A delayed job with a key starts at once there and sleeps in a start gate until `RunAt`, holding the key until then; it is refused when `RunAt` is further ahead than `IdempotencyMaxHold` or the workflow has a `Concurrency` ([`checkDelayedKey`](ds:block?id=hatchet-checkdelayedkey-8s6en9y6)). See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **An inbox forgets an ID after `Keep`** ([`InboxConsumer.Once`](ds:block?id=inboxconsumer-once-mta79w6e)). Set it longer than the longest redelivery your source can make (SQS keeps a message up to 14 days, Pub/Sub up to 31 with retention).
- **On the SQL driver the key is a unique index** ([`tableDefs`](ds:block?id=sql-tabledefs-cgz8s9k6)), so it holds across every process sharing the database; on the memory driver it holds within the process ([`enqueueLocked`](ds:block?id=memory-client-enqueuelocked-z3x2cytk)).

## Related

- [Enqueue a job](enqueue.md)
- [Unique jobs](../reliability/unique-jobs.md) and [debounce](../reliability/debounce.md), which dedup above the driver
- [Transactional enqueue](../reliability/transactional-enqueue.md)
- Examples: [`examples/idempotency`](../../examples/idempotency), [`examples/consumer-inbox`](../../examples/consumer-inbox)
