# sqlite-worker

One workflow, two drivers that need no server. The program runs a two-step "send-receipt" workflow first on the in-memory driver (what an app's tests use), then on the durable SQL driver over a SQLite file, built with the options a production worker sets: workers, poll interval, lease, reclaim limit, a logger, its own time-sortable run IDs and an error classifier that wraps the default. Nothing in the workflow names a driver.

```sh
cd examples
go run ./sqlite-worker
```

```text
memory durable: false
memory cron: true
memory run: succeeded
sqlite durable: true
run id kind: run
sqlite run: succeeded
status after the end: succeeded
```

The SQLite file lives in a temporary directory removed at exit; in an app, pass a path that survives restarts.

## The code

- `runMemory`: `memory.New` with `WithWorkers`, `WithLogger`, `WithJitterSource`; the capability check (`CapDurable` is the one it lacks).
- `runSQLite`: `driversqlite.Open`, `sqlstore.New` with its options, `Migrate`, `Register`, `Start`, `Enqueue`, and `Status` after the run ended.
- `orderedIDs`: an `IDGenerator` that keeps the driver's rules (1 to 64 bytes, unique per kind) and sorts by creation time.

`main_test.go` runs the same program and checks every line it prints. The guides [docs/drivers/memory.md](../../docs/drivers/memory.md) and [docs/drivers/sqlstore.md](../../docs/drivers/sqlstore.md) copy their code from the regions marked `snippet:`.
