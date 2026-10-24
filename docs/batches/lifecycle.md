# Change a batch after dispatch

A batch is not frozen once `Dispatch` returns. A job of the batch can add more jobs to it, a finished batch with failed jobs can run them again, two extra callbacks report when the batch starts and as each job ends, and old batch state can be pruned. Each works on every driver.

## Before you start

- A `batch.Manager` with `Attach` called before `Start`, as on [Batches](batches.md).
- For `RetryFailed`, a Manager built with `batch.WithRetryableJobs()`, which keeps every job's spec in the batch state.
- For `Prune`, a `batch.Store` that implements `batch.Pruner`. `batch.MemoryStore` does.
- The runnable program for this page is [examples/batch-lifecycle](../../examples/batch-lifecycle/).

## Step 1: report the start and every job end

`Batch.Before` names a workflow enqueued once when the batch is created, before its jobs start. `Batch.Progress` names a workflow enqueued once per job end that a run reports, whatever the status. Both receive the batch's `batch.Progress` as their JSON payload, carry `Batch.Labels`, and are retried like `Then`, `Catch` and `Finally` when their enqueue fails.

<!-- snippet: examples/batch-lifecycle/main.go#before -->
```go
_, err := m.Dispatch(ctx, batch.Batch{
	Name:     "thumbnails",
	Jobs:     []workflow.Job{{Workflow: "resize"}, {Workflow: "resize"}, {Workflow: "resize"}},
	Before:   "before",   // once, when the batch is created
	Progress: "progress", // once per job end
	Finally:  "finally",
})
```

A progress callback that fired only on successes would miss failed jobs. Here it fires on every job end; the payload's `Succeeded`, `Failed` and `CancelledJobs` counts tell them apart. A job recorded as cancelled without a run (a dispatch that failed part-way) fires no `Progress`.

## Step 2: add jobs from inside the batch

`Manager.Add(ctx, id, jobs...)` starts more jobs in a batch that is still running. A job finds its own batch through the `batch.LabelBatch` label on its `workflow.StepInfo`:

<!-- snippet: examples/batch-lifecycle/main.go#add -->
```go
{Name: "split", Steps: []workflow.Step{{Name: "run", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	id := batch.ID(info.Labels[batch.LabelBatch]) // the batch this job belongs to
	return nil, (*m).Add(ctx, id, workflow.Job{Workflow: "resize"}, workflow.Job{Workflow: "resize"})
}}}},
```

The new jobs take the next indexes, count in `Progress` from then on, and keep `Then`, `Catch` and `Finally` waiting until they end too. `Add` refuses a batch that has finished or was cancelled with `batch.ErrClosed`, and a batch whose own dispatch is still running with `batch.ErrDispatching`. The second is transient: return it from the step and the step's retry policy calls `Add` again.

## Step 3: retry the failed jobs

`Manager.RetryFailed(ctx, id)` starts the failed jobs of a finished batch again, under the same batch, and returns how many it started. It needs `WithRetryableJobs`:

<!-- snippet: examples/batch-lifecycle/main.go#retryable -->
```go
m = batch.New(c, batch.NewMemoryStore(),
	batch.WithClock(clk),
	batch.WithRetryableJobs(), // keeps each job's spec, which RetryFailed needs
	batch.WithManualCallbackRetry(),
	batch.WithIDFunc(func() (batch.ID, error) { seq++; return batch.ID("b" + strconv.Itoa(seq)), nil }),
)
```

<!-- snippet: examples/batch-lifecycle/main.go#retry -->
```go
id, err := m.Dispatch(ctx, batch.Batch{
	Name:          "sync",
	Jobs:          []workflow.Job{{Workflow: "flaky"}, {Workflow: "resize"}},
	AllowFailures: true,
	Then:          "then",
	Catch:         "catch",
	Finally:       "finally",
})
if err != nil {
	return err
}
print(2) // catch and finally: the flaky job failed once
n, err := m.RetryFailed(ctx, id)
if err != nil {
	return err
}
fmt.Fprintln(out, "retried", n)
print(2) // then and finally: this time every job succeeded
```

What it does:

- It restarts every job whose status is failed. When a failure cancelled the batch (no `AllowFailures`), it also restarts the jobs that cancel stopped, since `Then` cannot run without them.
- It clears the batch's cancellation and its `Catch`, `Then` and `Finally` latches, so `Catch` fires again on a new failure and `Then` or `Finally` once the retried jobs end. Those callbacks carry a new idempotency key, `batch:<id>:<kind>:r<round>`, so an engine does not drop them as duplicates of the first round's.
- Each retried run carries the label `batch.LabelAttempt`. A late end of the replaced run, which has no such label or an older value, is ignored, so it cannot fail the new attempt.

It refuses a batch that has not finished and one cancelled by `Manager.Cancel` (`workflow.ErrInvalid`), and a Manager without `WithRetryableJobs` (an `UnsupportedError`). A batch with nothing to retry returns 0.

## Step 4: prune old batches

`Manager.Prune(ctx, batch.PruneFilter{...})` deletes batch state the filter matches and returns how many batches it deleted. Each non-zero time is one rule, and a batch matching any rule is deleted:

| Field | Deletes |
|---|---|
| `FinishedBefore` | closed batches (every job ended, no callback pending) that finished before it |
| `UnfinishedBefore` | open batches (a job running or a callback pending) created before it |
| `CancelledBefore` | cancelled batches cancelled before it, open or not |

<!-- snippet: examples/batch-lifecycle/main.go#prune -->
```go
pruned, err := m.Prune(ctx, batch.PruneFilter{
	FinishedBefore:   clk.Now().Add(-24 * time.Hour), // closed batches, a day after they finished
	UnfinishedBefore: clk.Now().Add(-72 * time.Hour), // stuck ones, three days after they were created
})
if err != nil {
	return err
}
fmt.Fprintln(out, "pruned", pruned)
```

The Manager runs no pruning of its own; call `Prune` from a schedule the app owns. `PruneFilter.Matches(state)` is the whole rule, so a custom `Store` implements `Pruner` by deleting exactly the states it accepts.

## Try it

```sh
cd examples
go run ./batch-lifecycle
```

```text
-- before and progress
before b1 total=3
finally b1 total=3 succeeded=3 failed=0
progress b1 total=3
progress b1 total=3
progress b1 total=3
-- a job adds jobs to its batch
finally b2 total=3 succeeded=3 failed=0
then b2 total=3 succeeded=3 failed=0
-- retry the failed jobs
catch b3 total=2 failed=1
finally b3 total=2 succeeded=1 failed=1
retried 1
finally b3 total=2 succeeded=2 failed=0
then b3 total=2 succeeded=2 failed=0
-- a workflow waits for a batch
import done: 4 of 4 succeeded
-- many jobs without a batch
started 3 runs
-- prune
pruned 4
```

The second batch dispatched one `split` job, which added two `resize` jobs, so it finished with three. The third batch's `flaky` job failed once; `RetryFailed` started it again and the second round ran `Then`. `go test ./batch-lifecycle` runs the same program and checks every line.

## Pitfalls

- **`WithRetryableJobs` stores payloads.** Every write of the batch state carries every job's spec, so keep payloads small in a batch that uses it (a [claim check](../reliability/claim-check.md) reference rather than the data).
- **`Add` and `RetryFailed` on a coordinated batch.** A batch started as one coordinator run on Hatchet (`GroupSpawner`) refuses both with an `UnsupportedError`, because only the coordinator starts its jobs.
- **A failed enqueue in `Add` cancels the batch.** As for `Dispatch`, the jobs not yet started are recorded as cancelled and the batch is cancelled, so `Then` will not run; `RetryFailed` can bring a batch back only when a job failure cancelled it.
- **Pruning an open batch stops its tracking.** The ends of its jobs arrive for a batch that no longer exists and are ignored, and its pending callbacks are never enqueued.

## Related

- [Batches](batches.md): dispatch, callbacks, progress and cancel.
- [Wait for a batch in a workflow](await.md): a step that fans out to a batch and resumes after it.
- [How a batch learns a job ended](completion.md): sources, polling and callback retries.
- [Durability](durability.md): a shared store and dispatches that resume after a crash.
- Example: [examples/batch-lifecycle](../../examples/batch-lifecycle/).
