# complete-in-transaction

Complete a running step inside the app's own transaction with `sqlstore.Client.CompleteTx` (SQLite in memory): the payment row and the step's success commit together, the next step gets the output written in the transaction, and the error the handler returns after the commit is ignored (one attempt, no retry). A declined card rolls the transaction back, so neither the payment nor the completion exists and the step's own error fails the run. Outside a step, `CompleteTx` is refused with `workflow.ErrInvalid`.

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

On Postgres or MySQL open the app's pool with its driver and build the client with `sqlb.PostgresDialect{}` or `sqlb.MySQLDialect{}`; the handler is the same.

The guide [Complete inside your transaction](../../docs/reliability/complete-in-transaction.md) explains each step. `main_test.go` runs the same program and checks every line above.
