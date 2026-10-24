# transactional-enqueue

Write a run inside the app's own transaction with `sqlstore.Client.EnqueueTx` on SQLite: a committed order ships (and `Wake` admits it at once, though the poll interval is an hour), a rolled-back one leaves neither the order nor the run, and the same idempotency key twice in one transaction returns the first run without aborting the transaction.

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

Against the shared local stack, open the app's Postgres pool with `driverpgx.Open(dsn)` and build the client with `sqlb.PostgresDialect{}`; keep the default `READ COMMITTED` isolation.

The guide [Transactional enqueue](../../docs/reliability/transactional-enqueue.md) explains each step. `main_test.go` runs the same program and checks every line above.
