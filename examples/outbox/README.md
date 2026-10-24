# outbox

Write jobs into an outbox table inside the app's transaction (SQLite) and relay them to another client, the memory driver: a committed entry is delivered after `Relay.Notify`, a rolled-back one never exists; an entry the target keeps refusing goes dead after `WithMaxAttempts(2)`, is reported by `WithOnDead`, listed by `ListDead` and delivered after `Requeue`; and a target without idempotency keys is refused by `NewRelay` unless `AllowDuplicates`.

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

Against the shared local stack, build the outbox on Postgres (`driverpgx.Open(dsn)`, `sqlb.PostgresDialect{}`) and relay to a Hatchet client instead of the memory driver.

The guide [Outbox](../../docs/reliability/outbox.md) explains each step. `main_test.go` runs the same program and checks every line above.
