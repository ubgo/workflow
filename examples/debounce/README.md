# debounce

Fold a burst of jobs with the same key into one run. First on the memory driver with the default in-memory store and a fake clock (three changes two seconds apart: the first two runs are cancelled, the third runs after ten quiet seconds), then across two processes: two `sqlstore` clients on one SQLite file, each with its own `middleware.Debounce`, sharing `sqlstore.DebounceStore`. The burst alternates between the processes and still runs once.

```sh
cd examples
go run ./debounce
```

```text
-- Debounce a burst
job 1: cancelled
job 2: cancelled
ran with {"change":3}
-- A store shared by every process
ran with {"change":3}
job 1 (from process A): cancelled
job 2 (from process B): cancelled
job 3 (from process A): succeeded
```

The cross-process part uses a real one-second window, so the program takes about a second. Against the shared local stack, point both clients at one Postgres database (`driverpgx.Open(dsn)`, `sqlb.PostgresDialect{}`).

The guide [Debounce](../../docs/reliability/debounce.md) explains each step. `main_test.go` runs the same program and checks every line above.
