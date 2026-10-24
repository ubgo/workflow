# Batch durability

A batch's state (which jobs ended how, which callbacks fired, which are pending) lives in a `batch.Store`, and the guarantee a batch gives across processes is exactly that store's. With the SQL driver's store, a batch outlives the process that dispatched it: any process sharing the database records its jobs' ends, fires its callbacks once, and finishes a dispatch whose process died part-way.

## Before you start

- The `batch` package and, for a durable store, the `sqlstore` module with a database it supports (SQLite, Postgres, MySQL 8). The store works beside any driver; the jobs can run on Hatchet while the batch state is in Postgres.
- The runnable program for this page is [examples/batch-durable](../../examples/batch-durable/). It uses SQLite in a temp dir, so it needs no service.

## Where batch state lives

`batch.Store` has four methods: `Create` (an existing ID is an error), `Get` (an unknown ID wraps `workflow.ErrNotFound`), `Update(ctx, id, fn)` and `Unfinished` (every open batch, for the poller and the callback retries). `Update` must be atomic per batch: the read, `fn` and the write are one step with no other `Update` for the same ID in between, because the once-only latches in the state rely on it. A SQL store uses a transaction with a row lock; a store of your own (Redis, DynamoDB) uses a version check or a lock.

The store persists a `batch.State` as one record and never interprets it. It holds the batch's name, callback names, labels and `AllowFailures`; one `batch.JobState` per job (its `Ref` once `Enqueue` returned, and its terminal `Status`, empty until then); the latches `Cancelled`, `CatchFired` and `Finished`; the pending callbacks; and the dispatch progress described below. `State.Open()` reports whether the batch still needs the Manager (a job not finished, or a callback pending).

`batch.NewMemoryStore()` is the in-process store, for tests and the memory driver. Every Manager reporting into a batch must share it, so it holds only inside one process.

## A durable store

`sqlstore.Client.BatchStore()` returns a durable `batch.Store` in the client's database and table prefix: one row per batch, its state as a JSON document, and `Update` locks the row before reading it, so the latches hold across processes on every engine. It needs `Migrate` and nothing else: it works before `Start` and with no workflow registered, so a Hatchet app with a database can keep its batches there.

<!-- snippet: examples/batch-durable/main.go#store -->
```go
store := b.BatchStore() // durable: every process with this database sees the same batches
mb := batch.New(b, store, batch.WithClock(clk), batch.WithDispatchLease(lease))
if err := mb.Attach(ctx); err != nil {
	return err
}
if err := b.Start(ctx); err != nil {
	return err
}
```

Every process builds its `Manager` on the same store and calls `Attach`; whichever process hears a job's end records it, and the latch in the row decides which one fires each callback.

## Resume a dispatch that died

`Dispatch` holds a lease on its batch and renews it with every write. If the dispatching process dies (or stalls) part-way, the jobs it had not started would never run and the batch would never finish. `Manager.ResumeDispatches(ctx)` takes such a batch over once its lease has ended. The status poller, the callback retry loop and the Hatchet `BatchSource` call it every tick, so an app that runs any of them gets resumption for free; an app with none calls it itself.

- `batch.WithDispatchLease(d)` sets the lease (`batch.DefaultDispatchLease`, five minutes). It must exceed the longest single `Enqueue` or group call, since the lease is renewed between calls, and on an engine the lag of its run list.
- `batch.WithResumableDispatch()` keeps the job specs in the state until the dispatch finishes, so a resumer can start the jobs the dead process never started. Without it, on the one-`Enqueue`-per-job path, those jobs are recorded as cancelled instead. The cost is that each saved ref rewrites the state with every job's payload, so a dispatch of n jobs writes O(n²) payload bytes to the store. The native group path keeps the specs anyway.

<!-- snippet: examples/batch-durable/main.go#dispatcher -->
```go
ma := batch.New(stalled, a.BatchStore(), batch.WithClock(clk),
	batch.WithDispatchLease(lease),
	batch.WithResumableDispatch(), // keep the job specs so a resumer can start the unstarted ones
)
```

When it takes a batch over, the resumer looks at every job that has not finished and has no ref:

- a job the client lists as started gets its ref saved. The client lists them when it implements `batch.RunFinder` (`FindBatchRuns(ctx, id, since)`, the runs found by their batch label); the Hatchet driver does;
- a job known never to have started is started again, when its spec was kept and the batch is not cancelled;
- every other job is recorded as cancelled, with the reason logged.

Without a `RunFinder`, the one-`Enqueue`-per-job path saves each ref before the next `Enqueue`, so only the first job without a ref may have started with its ref lost; it is cancelled rather than risk running it twice, and the jobs after it are known never to have started. In the example, process A starts jobs 0 and 1 and stalls on job 2; process B advances past the lease and resumes:

<!-- snippet: examples/batch-durable/main.go#resume -->
```go
clk.Advance(lease + time.Second) // A has not written for longer than its lease
if err := mb.ResumeDispatches(ctx); err != nil {
	return err
}
```

Job 2 is cancelled (it may have started), jobs 3 and 4 are started by B, and `Finally` reports four succeeded and one cancelled. When A's stalled call finally returns, A finds its lease gone and starts nothing more. A cancelled batch starts nothing on resume: its unstarted jobs are recorded as cancelled and the runs found are cancelled.

## Try it

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

The example runs both "processes" in one program on one SQLite file and moves a shared fake clock past the lease, so it takes milliseconds. `go test ./batch-durable` runs it on a SQLite file in a temp dir and checks every line.

## Pitfalls

- **The first unrecorded job is cancelled, not retried.** Without a `RunFinder`, the job the dead process may have been enqueuing is recorded as cancelled even if it never started, so the batch's `Then` does not run; if it did run, its end is ignored, since a job's first recorded status wins. Make `Finally` the callback that matters when dispatches can die.
- **No specs, no restart.** Without `WithResumableDispatch`, on the one-`Enqueue`-per-job path, every job the dead process never started is cancelled.
- **A short lease takes over live dispatches.** A lease shorter than one slow `Enqueue` lets a resumer take over from a dispatcher that is merely slow; the original then stops at its next write. Keep the default unless you know your slowest call.
- **Resumption needs a ticker.** The poller, the callback retry loop and the Hatchet `BatchSource` call `ResumeDispatches`; `WithManualCallbackRetry` on a hook-only client turns the loop off, and then the app must call it.
- **Every process needs the same store.** A Manager on a different store (or a `MemoryStore`) never sees the batch.

## Related

- [Batches](batches.md): dispatch, progress, cancel.
- [Batch completion](completion.md): the sources that also resume dispatches.
- [Native group runs](native-groups.md): the group and coordinator paths, which keep specs anyway.
- [SQL driver](../drivers/sqlstore.md): migrations and the tables behind `BatchStore`.
- Example: [examples/batch-durable](../../examples/batch-durable/).
