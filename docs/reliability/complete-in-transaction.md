# Complete inside your transaction

A "charge" step stores a payment row and returns. If the process dies after the payment commits but before the worker records the step's success, the lease expires, another worker runs the step again, and the customer is charged twice. Idempotent handlers are the usual cure; when the step's side effect is a write to the same SQL database the queue lives in, there is a stronger one: [`sqlstore.Client.CompleteTx`](ds:block?id=sql-client-completetx-q8d5sbtc) records the step's success inside the app's own transaction, so the payment and the completion commit together, or neither does.

## Before you start

- The SQL driver (`github.com/ubgo/workflow/sqlstore`) on SQLite, Postgres or MySQL 8, with the app's tables in the same database and the app's transaction opened on the same `*sql.DB` the client was built with.
- [`CompleteTx`](ds:block?id=sql-client-completetx-q8d5sbtc) is called from a step handler that this client is running, with the handler's `ctx`. No capability is involved; other drivers have no such method.
- The runnable program for this page is [examples/complete-in-transaction](../../examples/complete-in-transaction/).

## Complete the step in your transaction

Inside the handler, begin a transaction, do the app's writes, call `CompleteTx(ctx, tx, output)` and commit. `output` is the step's output, exactly as the handler would have returned it:

<!-- snippet: examples/complete-in-transaction/main.go#complete -->
```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer func() {
	if rbErr := tx.Rollback(); !errors.Is(rbErr, sql.ErrTxDone) {
		err = errors.Join(err, rbErr)
	}
}()
order := string(in)
if _, err := tx.ExecContext(ctx, "INSERT INTO payments (order_id, amount) VALUES (?, 4200)", order); err != nil {
	return err
}
if order == "o2" {
	return workflow.Permanent(errDeclined) // the deferred Rollback drops the payment row
}
// The step succeeds with this output if and only if tx commits.
if err := c.CompleteTx(ctx, tx, []byte(`{"order":"`+order+`","paid":4200}`)); err != nil {
	return err
}
if err := tx.Commit(); err != nil {
	return err
}
```

[`CompleteTx`](ds:block?id=sql-client-completetx-q8d5sbtc) writes, in `tx`, what a returned success writes: the step succeeds with `output`, the steps waiting on it become ready with it as their input, and when it was the last step the run succeeds (its result, chain hand-off and idempotency key release included). Chain edits the handler made before the call (`workflow.PrependToChain`, `AppendToChain`) are written with it.

Once the transaction commits, the step is done. What the handler returns afterwards is ignored: in the example the handler returns an error after the commit, and the step is not retried, not failed and not completed a second time; the run-finished hook fires once ([`TestCompleteTxCommit`](ds:block?id=enginetest-testcompletetxcommit-t6a53up6)). The worker carries out the rest (lifecycle `Finished` events, waking the workers for the next step) when the handler returns.

If the transaction rolls back, nothing was recorded, and the handler's return value is recorded as usual: a success with its output, or an error that the retry policy handles ([`TestCompleteTxRollback`](ds:block?id=enginetest-testcompletetxrollback-hyf5acd3)). In the example, order `o2` is declined after the payment row was written; the rollback removes the row and the step's `workflow.Permanent` error fails the run.

## Fenced by the lease

The completion is written only while the step is still running under this attempt's lease. [`CompleteTx`](ds:block?id=sql-client-completetx-q8d5sbtc) locks the step row and changes it only when its lease token is this attempt's, so it refuses with an error matching [`sqlstore.ErrLeaseLost`](ds:block?id=sql-errleaselost-qhvxyrbf) when:

- the attempt's lease expired and another client took the step over (a new token on the row);
- the run was cancelled, failed elsewhere or deleted while the handler ran;
- the attempt's own [heartbeat](ds:block?id=sql-client-heartbeat-uu2x8xhn) already gave the lease up (the handler's context cause is `ErrLeaseLost`).

Until the transaction ends, no other client can claim, renew or end the step: they wait on the row lock (Postgres, MySQL) or the writer lock (SQLite). Roll the transaction back on any error from `CompleteTx`, as the deferred `Rollback` above does.

It refuses with `workflow.ErrInvalid` a nil transaction, a second completion of the same attempt, and a context that is not a step handler's context of this client (another client's step, `RunInline`, a test calling the handler directly; [`TestCompleteTxRefusals`](ds:block?id=enginetest-testcompletetxrefusals-86xvqrf7) covers each refusal and the `ErrLeaseLost` cases):

<!-- snippet: examples/complete-in-transaction/main.go#refusal -->
```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
err = c.CompleteTx(ctx, tx, nil) // ctx is not a step's context
fmt.Println("refused with ErrInvalid:", errors.Is(err, workflow.ErrInvalid))
```

## Try it

```sh
cd examples
go run ./complete-in-transaction
```

```text
-- commit
email got {"order":"o1","paid":4200}
run: succeeded
charge attempts: 1
-- roll back
run: failed
payments stored: 1
-- outside a step
refused with ErrInvalid: true
```

## Pitfalls

- The transaction must be open on the `*sql.DB` the client was built with: the completion is written to this client's tables through it ([`CompleteTx`](ds:block?id=sql-client-completetx-q8d5sbtc)). A transaction on another database cannot be fenced and is not detected.
- `output` is stored as given ([`CompleteTx`](ds:block?id=sql-client-completetx-q8d5sbtc)). Step middleware that rewrites a step's output on its way out of the handler (a codec, `middleware.Encrypt`, the claim check) does not see it: pass the output in its stored form, or return it instead of completing in the transaction.
- Keep the transaction short. While it is open the step row is locked, so the attempt's lease renewal waits on it; a transaction held longer than the lease ([`WithLease`](ds:block?id=sql-withlease-gtcuhgcm), [30 seconds](ds:block?id=sql-defaultlease-qygq5sbe) by default) makes the attempt give up its lease ([`heartbeat`](ds:block?id=sql-client-heartbeat-uu2x8xhn)): a completion already written still commits with the transaction, a `CompleteTx` called after that is refused.
- After the commit, return promptly: the next lease renewal finds the step no longer running and cancels the handler's context ([`heartbeat`](ds:block?id=sql-client-heartbeat-uu2x8xhn)). Chain edits made after the call are not written.
- On SQLite, make a write the transaction's first statement (or call `CompleteTx` first): a deferred transaction that has only read cannot start writing once another writer committed (`SQLITE_BUSY_SNAPSHOT`). With `driversqlite.Memory`, which holds one connection, the handler's transaction has that connection to itself until it ends.
- The handler's side effects outside the database (an email, an HTTP call) are not part of the transaction; they still need to be idempotent.

## Related

- [Transactional enqueue](transactional-enqueue.md): the other half, a run written inside the app's transaction.
- [The SQL driver](../drivers/sqlstore.md#leases-and-crashed-workers): leases, takeover and `ErrLeaseLost`.
- [Outbox](outbox.md): transactional enqueue for any client.
