# Outbox

The transactional outbox makes "save the order and enqueue the job" atomic when the queue is not in the app's database: Hatchet, another database, the memory driver, or any client wrapped in middleware. The app writes the job into an outbox table inside its own transaction ([`Outbox.Write`](ds:block?id=outbox-outbox-write-pxkp6pag)); a relay reads committed entries and enqueues them on the target client ([`Relay.Flush`](ds:block?id=outbox-relay-flush-guc9yfbb)). A rolled-back transaction leaves nothing to relay, so a job is enqueued if and only if the app's writes committed.

When the queue is the same SQL database and no enqueue middleware is needed, [`EnqueueTx`](transactional-enqueue.md) is simpler: it writes the run itself, with no relay.

## Before you start

- The `github.com/ubgo/workflow/sqlstore` module, which holds the `sqlstore/outbox` package, and the app's `*sql.DB` on Postgres, MySQL 8 or SQLite ([`outbox.New`](ds:block?id=outbox-build-vqqgq3hz)). The outbox table needs no sqlstore queue: an app relaying to Hatchet gets only `<prefix>outbox` and the shared migration ledger.
- A target client that declares `CapIdempotencyKey` (memory, SQL, Redis and Hatchet all do: [`memory`](ds:block?id=memory-caps-yxkuq583), [`SQL`](ds:block?id=sql-caps-yu37ef2j), [`Redis`](ds:block?id=redis-caps-bfgdst9t), [`Hatchet`](ds:block?id=hatchet-caps-d6x9fv8z)), or the decision to accept duplicates (see the last section).
- The runnable program for this page is [examples/outbox](../../examples/outbox/).

## Write jobs inside your transaction

[`outbox.New(db, dialect, opts...)`](ds:block?id=outbox-new-2c322ued) returns an [`Outbox`](ds:block?id=outbox-outbox-apbjta4y), which holds no state of its own: every entry is a row. [`Migrate`](ds:block?id=outbox-outbox-migrate-dkanysjn) creates the table through the same versioned ledger sqlstore uses, under its own owner, and is explicit: `Write` and relays never migrate. Then [`Write(ctx, tx, job)`](ds:block?id=outbox-outbox-write-pxkp6pag) adds the job inside the app's transaction and returns the entry's ID:

<!-- snippet: examples/outbox/main.go#write -->
```go
ob, err := outbox.New(db, sqlb.SQLiteDialect{}, outbox.WithClock(clk))
if err != nil {
	return err
}
if err := ob.Migrate(ctx); err != nil { // creates wf_outbox; explicit, versioned
	return err
}

tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer func() {
	if rbErr := tx.Rollback(); !errors.Is(rbErr, sql.ErrTxDone) {
		err = errors.Join(err, rbErr)
	}
}()
if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id, item) VALUES (1, 'book')"); err != nil {
	return err
}
if _, err := ob.Write(ctx, tx, workflow.Job{Workflow: "ship", Payload: []byte(`{"order":1}`)}); err != nil {
	return err
}
if err := tx.Commit(); err != nil {
	return err
}
```

The example's clock is a fake one ([`outbox.WithClock`](ds:block?id=outbox-withclock-bbxuj3eu)), so the backoff below passes on demand. [`WithTablePrefix`](ds:block?id=outbox-withtableprefix-r9udebxk) names the table ([`wf_`](ds:block?id=outbox-defaulttableprefix-rjhz5epr) by default), [`WithSchema`](ds:block?id=outbox-withschema-vyhab4c4) puts it in a Postgres schema, [`WithIDGenerator`](ds:block?id=outbox-withidgenerator-sa4cq8nf) replaces the entry ID format (`obx_` and 32 hex characters: [`DefaultIDGenerator`](ds:block?id=outbox-defaultidgenerator-md5cgbac), [`randomID`](ds:block?id=outbox-randomid-ms2ck98b)), and [`WithLogger`](ds:block?id=outbox-withlogger-32st7x3h) takes what a relay cannot report to a caller. [`outbox.MigrationScripts(dialect, opts...)`](ds:block?id=outbox-migrationscripts-w4t9z546) returns the same migration as SQL text for teams that apply DDL with their own tool.

## Relay entries to any client

[`ob.NewRelay(target, opts...)`](ds:block?id=outbox-outbox-newrelay-5jmb3cm4) returns a relay. Nothing runs until [`Start(ctx)`](ds:block?id=outbox-relay-start-4jvwb54m), which does not block; the loop then relays every due entry, waits for its poll interval, and repeats until `ctx` ends or `Stop` is called ([`loop`](ds:block?id=outbox-relay-loop-hg5c9hfq)). [`Notify()`](ds:block?id=outbox-relay-notify-3s3vb4cd) wakes it now, so call it after a commit that wrote to the outbox:

<!-- snippet: examples/outbox/main.go#relay -->
```go
relay, err := ob.NewRelay(target)
if err != nil {
	return err
}
if err := relay.Start(ctx); err != nil { // nothing is relayed before Start
	return err
}
relay.Notify() // relay now rather than at the next poll
```

[`Flush(ctx)`](ds:block?id=outbox-relay-flush-guc9yfbb) relays every due entry once, without starting the loop, and returns how many the target accepted: for a test, a script, or an app that drives relaying from its own scheduler. Its error is a database failure; a target that refuses an entry is recorded on the entry and retried later, not returned ([`fail`](ds:block?id=outbox-relay-fail-5pyc97na)). After the rolled-back write in the example, `Flush` finds nothing left to relay.

A relay claims a batch of due entries under a lease (`FOR UPDATE SKIP LOCKED` on Postgres and MySQL, one conditional `UPDATE` on SQLite: [`claim`](ds:block?id=outbox-relay-claim-fxnbwdp6)), so any number of relays in any number of processes can share one table without claiming an entry twice. It enqueues each entry in table order and deletes it once the target accepted it ([`deliver`](ds:block?id=outbox-relay-deliver-g2n48m2x)). The options:

| Option | Default | What it sets |
|---|---|---|
| `WithPollInterval(d)` | [`1s`](ds:block?id=outbox-defaultpoll-6usxr8he) | How often an idle relay looks for entries written by other processes or whose backoff ended |
| `WithBatchSize(n)` | [`100`](ds:block?id=outbox-defaultbatch-n8q6sxu8) | How many entries one claim takes |
| `WithLease(d)` | [`1m`](ds:block?id=outbox-defaultlease-re9a7nfj) | How long a claimed batch stays claimed; a relay that dies loses it to another after this long |
| `WithEnqueueTimeout(d)` | [`10s`](ds:block?id=outbox-defaultenqueuetimeout-xx3czv6a) | The bound on one enqueue on the target; must be shorter than the lease |

Delivery is at least once. A relay that dies after the target accepted an entry but before the delete leaves it to be enqueued again once the lease expires. Each enqueue therefore carries an idempotency key, the job's own when it has one and the entry's ID otherwise ([`entry.job`](ds:block?id=outbox-entry-job-867uyy4p)), so a target that honours keys drops the second enqueue while the first run has not finished. A run that already finished runs twice, so handlers must be idempotent, as for any queue.

Enqueue middleware on the target runs at relay time, after the commit, which is how a transactional enqueue gets [unique jobs](unique-jobs.md), [debouncing](debounce.md) or tracing.

## Failed and dead entries

A failed enqueue is retried after a backoff, by default one second doubling to five minutes ([`defaultBackoff`](ds:block?id=outbox-defaultbackoff-2sqvbn62), [`WithBackoff`](ds:block?id=outbox-withbackoff-47cktjec)). By default an entry is retried forever ([`WithMaxAttempts`](ds:block?id=outbox-withmaxattempts-45nh9skd)), because a failure is most often the target being down or a workflow not yet registered during a deploy. [`WithMaxAttempts(n)`](ds:block?id=outbox-withmaxattempts-45nh9skd) moves an entry to the dead status after `n` failed enqueues, and [`WithOnDead(fn)`](ds:block?id=outbox-withondead-ssqqjtsz) is told about it with the last error, for an alert:

<!-- snippet: examples/outbox/main.go#dead -->
```go
relay, err := ob.NewRelay(target,
	outbox.WithBackoff(workflow.Backoff{Initial: time.Minute, Factor: 2, Max: time.Hour}),
	outbox.WithMaxAttempts(2), // dead after two failed enqueues; the default retries forever
	outbox.WithOnDead(func(ctx context.Context, e outbox.DeadEntry, cause error) {
		fmt.Println("dead:", e.Job.Workflow, "after", e.Attempts, "attempts:", cause)
	}))
if err != nil {
	return err
}
```

In the example the target is down, so the entry fails at `Flush`, fails again a minute later on the fake clock, and goes dead. [`ListDead(ctx, DeadQuery{...})`](ds:block?id=outbox-outbox-listdead-pa6u6bwe) returns dead entries oldest first, optionally for one workflow, in pages ([`Limit`](ds:block?id=outbox-deadquery-pn6n9arw) defaults to [`100`](ds:block?id=outbox-defaultdeadpage-vkk9f7rb) and is capped at [`1000`](ds:block?id=outbox-maxdeadpage-8p3g9zrv); `After` continues after the last ID of the previous page). [`Requeue(ctx, id)`](ds:block?id=outbox-outbox-requeue-nd4mvayp) makes an entry pending again with a fresh attempt count, due at once, and keeps its ID, so the idempotency key the relay sends is the same as before:

<!-- snippet: examples/outbox/main.go#list-requeue -->
```go
dead, err := ob.ListDead(ctx, outbox.DeadQuery{Workflow: "ship"})
if err != nil {
	return err
}
for _, e := range dead {
	fmt.Printf("listed: %s attempts=%d last error=%q\n", e.Job.Payload, e.Attempts, e.LastErr)
}
down.Store(false) // the target is back
for _, e := range dead {
	if err := ob.Requeue(ctx, e.ID); err != nil { // same ID, so same idempotency key
		return err
	}
}
n, err = relay.Flush(ctx)
```

## Targets without idempotency keys

The relay's duplicate protection is the key it sends with every enqueue, so [`NewRelay`](ds:block?id=outbox-outbox-newrelay-5jmb3cm4) refuses a target without `CapIdempotencyKey` with an `UnsupportedError`; every keyed entry would otherwise be refused at enqueue time and go dead. [`AllowDuplicates()`](ds:block?id=outbox-allowduplicates-wx53najx) accepts such a target and names the trade-off: the relay then sends only the job's own key (none when the job has none), and an entry whose relay died after the target accepted it, or whose delete failed, runs twice. The example uses a `workflowtest.Fake` that claims every capability except `CapIdempotencyKey`:

<!-- snippet: examples/outbox/main.go#allow-duplicates -->
```go
_, err := ob.NewRelay(noKeys)
fmt.Println("refused without AllowDuplicates:", errors.Is(err, workflow.ErrUnsupported))
relay, err := ob.NewRelay(noKeys, outbox.AllowDuplicates()) // a redelivered entry runs twice
if err != nil {
	return err
}
```

## Try it

```sh
cd examples
go run ./outbox
```

```text
-- Write jobs inside your transaction
-- Relay entries to any client
shipped {"order":1}
entries left after the rollback: 0
-- Failed and dead entries
accepted while down: 0
dead: ship after 2 attempts: engine unreachable
listed: {"order":4} attempts=2 last error="engine unreachable"
accepted after requeue: 1
shipped {"order":4}
-- Targets without idempotency keys
refused without AllowDuplicates: true
accepted: 1, idempotency key sent: ""
```

## Pitfalls

- [`Write`](ds:block?id=outbox-outbox-write-pxkp6pag) needs a transaction open on the database the outbox lives in, and a `Job.Workflow`; anything about the target is checked only at relay time, so a job the target refuses is retried with backoff and, with `WithMaxAttempts`, goes dead.
- A failed generator, or an entry ID that is empty or longer than [`64 bytes`](ds:block?id=outbox-maxidlen-6em72zqk), fails `Write` (`workflow.ErrInvalid` for a bad ID: [`newID`](ds:block?id=outbox-outbox-newid-bdweqjrb)). The ID is also the default idempotency key, so a custom generator must return unique IDs ([`IDGenerator`](ds:block?id=outbox-idgenerator-4qp6wagb)).
- [`NewRelay`](ds:block?id=outbox-outbox-newrelay-5jmb3cm4) refuses a lease that does not exceed the enqueue timeout, with `workflow.ErrInvalid`: a hung enqueue must not outlive its claim. A nil target is `workflow.ErrInvalid` too ([`TestRefusals`](ds:block?id=outboxtest-testrefusals-q659n4ht)).
- Order holds only within one relay pass ([`claim`](ds:block?id=outbox-relay-claim-fxnbwdp6) reads a batch in table order). Across relays, and after a failed entry is retried, there is no order.
- [`Start`](ds:block?id=outbox-relay-start-4jvwb54m) twice returns `workflow.ErrAlreadyStarted`. [`Stop`](ds:block?id=outbox-relay-stop-sgffde6u) waits for the loop and releases entries it claimed but did not enqueue; it is safe before `Start` and more than once.
- The dead hook ([`DeadFunc`](ds:block?id=outbox-deadfunc-qz436ync)) runs on the relay's goroutine: hand slow work (a page, a ticket) off instead of blocking the relay. Without a hook, dead entries are logged ([`WithOnDead`](ds:block?id=outbox-withondead-ssqqjtsz)).
- [`DeadEntry.Job.IdempotencyKey`](ds:block?id=outbox-deadentry-gcwp47z5) is the job's own key; the entry ID the relay sends when the job has none is not filled in.
- [`ListDead`](ds:block?id=outbox-outbox-listdead-pa6u6bwe) with an `After` that names no dead entry, and [`Requeue`](ds:block?id=outbox-outbox-requeue-nd4mvayp) of an ID that is not dead, return `workflow.ErrNotFound`.
- [`Migrate`](ds:block?id=outbox-outbox-migrate-dkanysjn) refuses a database a newer release migrated, with [`outbox.ErrSchemaTooNew`](ds:block?id=outbox-errschematoonew-fqmc6pdh), and never drops anything.

## Related

- [Transactional enqueue](transactional-enqueue.md): no relay, when the queue is the same SQL database.
- [Idempotency keys](../jobs/idempotency.md): what the target does with the key the relay sends.
- [Migrations](../operations/migrations.md): the shared ledger and `MigrationScripts`.
- [Failover](failover.md): another way to keep accepting jobs while an engine is down.
- Example: [examples/outbox](../../examples/outbox/).
