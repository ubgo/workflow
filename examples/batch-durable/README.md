# batch-durable

Batch state in SQLite through the SQL driver's `BatchStore`, and a dispatch that resumes after its process stalls. Process A dispatches a five-job batch with `WithResumableDispatch` and stalls while enqueuing job 2. Process B, a second client on the same database, moves a shared fake clock past A's dispatch lease and calls `ResumeDispatches`: job 2 is cancelled (A may have started it), jobs 3 and 4 are started, and `Finally` runs once. When A's stalled call returns, its lease is gone and it starts nothing more.

```sh
cd examples
go run ./batch-durable
```

```text
process A started jobs 0 and 1, then stalled on job 2
finally: 5 jobs, 4 succeeded, 1 cancelled
job 0: succeeded
job 1: succeeded
job 2: cancelled
job 3: succeeded
job 4: succeeded
process A's dispatch ended with an error: true
```

Both processes run in one program on one SQLite file in a temp dir, so the example needs no service and takes milliseconds. The same store works on Postgres and MySQL 8 by opening the database with sqlb's driver for that engine and passing its dialect to `sqlstore.New`.

The guide that copies this program is [batch durability](../../docs/batches/durability.md). `go test ./batch-durable` runs it on a SQLite file in the test's temp dir and checks every line.
