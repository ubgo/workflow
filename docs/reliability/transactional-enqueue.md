# Transactional enqueue

An order is saved and a "ship it" job is enqueued. If the two are separate calls, one can happen without the other: the insert commits and the process dies before the enqueue, or the enqueue succeeds and the transaction rolls back, and a worker ships an order that does not exist. When the queue lives in the same SQL database as the app's data, `sqlstore.Client.EnqueueTx` writes the run inside the app's own transaction, so the run commits or rolls back with the app's writes.

When the queue is somewhere else (Hatchet, another database, the memory driver), or the enqueue must go through middleware, use the [outbox](outbox.md) instead.

## Before you start

- The SQL driver (`github.com/ubgo/workflow/sqlstore`) on the same `*sql.DB` your app writes through, migrated, with the workflow registered and the client started.
- On Postgres, the default `READ COMMITTED` isolation (see Pitfalls).
- The runnable program for this page is [examples/transactional-enqueue](../../examples/transactional-enqueue/).

## Enqueue inside your transaction

Begin a transaction on the app's pool, do the app's writes, call `EnqueueTx(ctx, tx, job)` and commit. A committed run is picked up by the workers of every client registering the workflow. The run is written but not admitted inside the transaction (admission may take a lock row, which would then be held for as long as the app's transaction runs), so the dispatcher finds it at its next poll, within `WithPollInterval` (a second by default). `Wake()` after the commit makes this client look at once:

<!-- snippet: examples/transactional-enqueue/main.go#commit -->
```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer tx.Rollback() // a no-op once committed
if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id, item) VALUES (1, 'book')"); err != nil {
	return err
}
ref, err := c.EnqueueTx(ctx, tx, workflow.Job{
	Workflow:       "ship",
	Payload:        []byte(`{"order":1}`),
	IdempotencyKey: "order-1",
})
if err != nil {
	return err
}
if err := tx.Commit(); err != nil {
	return err
}
c.Wake() // admit it now instead of at the next poll
```

The example sets the poll interval to an hour, so the order ships quickly only because of `Wake`.

When the app decides not to commit, nothing is left behind: not the order, and not the run. `Status` on the ref the rolled-back call returned answers `workflow.ErrNotFound`:

<!-- snippet: examples/transactional-enqueue/main.go#rollback -->
```go
tx, err = db.BeginTx(ctx, nil)
if err != nil {
	return err
}
if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id, item) VALUES (2, 'lamp')"); err != nil {
	return err
}
ref, err = c.EnqueueTx(ctx, tx, workflow.Job{Workflow: "ship", Payload: []byte(`{"order":2}`)})
if err != nil {
	return err
}
if err := tx.Rollback(); err != nil { // the app decides not to commit
	return err
}
_, err = c.Status(ctx, ref)
fmt.Fprintln(out, "run after rollback is ErrNotFound:", errors.Is(err, workflow.ErrNotFound))
```

Idempotency keys follow the workflow's policy, as for `Enqueue`. A key held by an unfinished run, committed or written earlier in the same transaction, returns that run's `RunRef` and writes nothing. The keyed insert runs under a savepoint, so a duplicate key never aborts the caller's transaction, which stays usable for the app's next write:

<!-- snippet: examples/transactional-enqueue/main.go#same-key -->
```go
job := workflow.Job{Workflow: "ship", Payload: []byte(`{"order":3}`), IdempotencyKey: "order-3"}
first, err := c.EnqueueTx(ctx, tx, job)
if err != nil {
	return err
}
second, err := c.EnqueueTx(ctx, tx, job) // returns the run written above
if err != nil {
	return err
}
```

## Try it

```sh
cd examples
go run ./transactional-enqueue
```

```text
-- commit
shipped {"order":1}
run: succeeded
-- roll back
run after rollback is ErrNotFound: true
orders stored: 1
-- same key twice in one transaction
second call returned the first run: true
transaction still usable: true
shipped {"order":3}
```

## Inside a pgx transaction

If your transactions are pgx transactions rather than `database/sql` ones, wrap the enqueue in `pgxtx.Run` from `github.com/ubgo/workflow/sqlstore/pgxtx`: `err := pgxtx.Run(ctx, tx, func(stx *sql.Tx) error { _, err := client.EnqueueTx(ctx, stx, job); return err })`. The `*sql.Tx` it hands you is a view over your `pgx.Tx`, so its statements, savepoints and error classification run on the pgx connection; its `Commit` and `Rollback` do nothing, and committing stays your call on the pgx transaction (then `client.Wake()`). It is Postgres only and valid only inside the function. `outbox.Write` takes the same view.

## Pitfalls

- The transaction must be open on the `*sql.DB` the client was built with: the run is written through it, into this client's tables.
- The client must be started and the workflow registered on it, as for `Enqueue`: before `Start`, or after `Stop`, the call returns `workflow.ErrNotStarted`; an unregistered workflow is `workflow.ErrNotFound` and a nil `tx` is `workflow.ErrInvalid`.
- Nothing can wake a worker before the commit. Without `Wake`, a committed run waits for the next poll; `Wake` only nudges this client, and other clients of the cluster find the run at their next poll. It never blocks and is a no-op on a client that is not running.
- Enqueue middleware does not run: it wraps `Client.Enqueue`, and what it does (a lock in another store, a debounce timer, a trace export) would not roll back with the transaction. Unique jobs, debouncing or tracing on a transactional enqueue need the [outbox](outbox.md), whose relay enqueues through any client after the commit.
- A key written by another transaction that has not committed yet makes this insert wait for it.
- On Postgres use `READ COMMITTED` (the default): a `REPEATABLE READ` transaction cannot see a key holder that committed after its snapshot, and fails instead. On MySQL the holder is read with `FOR SHARE`, which sees rows committed after the transaction began.
- An error leaves the transaction open; rolling it back or carrying on is the caller's decision.

## Related

- [Outbox](outbox.md): the same guarantee for any client, with middleware.
- [The SQL driver](../drivers/sqlstore.md): polling, workers and tuning.
- [Idempotency keys](../jobs/idempotency.md): the workflow policies `EnqueueTx` follows.
- Example: [examples/transactional-enqueue](../../examples/transactional-enqueue/).
