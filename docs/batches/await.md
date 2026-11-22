# Wait for a batch in a workflow

A workflow sometimes has to fan out: import a file by resizing every image in it, then send one report when all are done. [`Manager.AwaitStep`](ds:block?id=batch-manager-awaitstep-m6y9gj8c) is a step that dispatches a batch and completes when the batch finishes, so the steps after it run once every job has ended. It is a batch inside a chain of steps: the steps before it run first, and the steps after it wait for the whole batch. For many jobs with no batch to track, [`batch.EnqueueMany`](ds:block?id=batch-enqueuemany-6wx8q3ub) starts them in one call.

## Before you start

- A `batch.Manager` attached and running in every worker process that may run the step, since the step reads the batch from the Manager's store ([`Manager.await`](ds:block?id=batch-manager-await-3qf56kjz)). With a shared store (sqlstore's `BatchStore`), any process may run it.
- A driver that sets `workflow.StepInfo` on a step's ctx; without it [`AwaitStep`](ds:block?id=batch-manager-await-3qf56kjz) fails permanently. Every driver in this module does.
- The runnable program for this page is [examples/batch-lifecycle](../../examples/batch-lifecycle/).

## Step 1: build the await step

[`m.AwaitStep(name, build, opts...)`](ds:block?id=batch-manager-awaitstep-m6y9gj8c) returns a `workflow.Step`. `build` receives the step's input and returns the batch to dispatch; its successors receive the batch's `batch.Progress` as JSON:

<!-- snippet: examples/batch-lifecycle/main.go#await -->
```go
fan := m.AwaitStep("fan", func(_ context.Context, in []byte) (batch.Batch, error) {
	rows, err := strconv.Atoi(string(in))
	if err != nil {
		return batch.Batch{}, workflow.Permanent(err)
	}
	b := batch.Batch{Name: "import"}
	for range rows {
		b.Jobs = append(b.Jobs, workflow.Job{Workflow: "resize"})
	}
	return b, nil
}, batch.AwaitPoll(awaitPoll))
report := workflow.Step{Name: "report", After: []string{"fan"}, Run: workflow.Define(func(_ context.Context, p batch.Progress) (string, error) {
	lines <- fmt.Sprintf("import done: %d of %d succeeded", p.Succeeded, p.Total)
	return "", nil
})}
return c.Register(workflow.Workflow{Name: "import", Steps: []workflow.Step{fan, report}})
```

How it works, attempt by attempt:

- The first attempt calls `build` and dispatches the batch under an ID derived from the run and the step ([`batch.AwaitID(runID, step)`](ds:block?id=batch-awaitid-sw2bkyj7)), so a retried attempt finds that batch instead of dispatching a second one.
- Each later attempt looks at the batch. While it runs, the step returns [`batch.ErrBatchPending`](ds:block?id=batch-errbatchpending-qe5qk678), and the retry policy [`AwaitStep`](ds:block?id=batch-manager-awaitstep-m6y9gj8c) sets (`AwaitPolls` retries, `AwaitPoll` apart) looks again.
- Once the batch has finished, the step returns its `Progress`. A batch with a failed or cancelled job (a discarded job counts as [`Batch.DiscardedAs`](ds:block?id=batch-batch-sbryq3mb) says, failed by default) fails the step permanently with [`batch.ErrBatchFailed`](ds:block?id=batch-errbatchfailed-8v9cn2x5) ([`await`](ds:block?id=batch-manager-await-3qf56kjz)), unless [`batch.AwaitAllowFailures()`](ds:block?id=batch-awaitallowfailures-pzk3m8bk) was given, in which case the next step reads the counts and decides.

The defaults are [`DefaultAwaitPoll`](ds:block?id=batch-defaultawaitpoll-gynjh8ck) (5 seconds) and [`DefaultAwaitPolls`](ds:block?id=batch-defaultawaitpolls-muyng9sm) (720, one hour). Raise [`AwaitPolls`](ds:block?id=batch-awaitpolls-3gecwxn7) for a batch that runs longer; a value below 1 keeps the default.

## Step 2: start many jobs without a batch

[`batch.EnqueueMany(ctx, client, jobs)`](ds:block?id=batch-enqueuemany-6wx8q3ub) starts jobs with no batch labels, no store and no callbacks, and returns one `RunRef` per job:

<!-- snippet: examples/batch-lifecycle/main.go#bulk -->
```go
refs, err := batch.EnqueueMany(ctx, c, []workflow.Job{
	{Workflow: "resize"}, {Workflow: "resize"}, {Workflow: "resize"},
})
if err != nil {
	return err
}
fmt.Println("started", len(refs), "runs")
```

It picks the cheapest path the client offers: the client's own `EnqueueMany` when it implements [`batch.ManyEnqueuer`](ds:block?id=batch-manyenqueuer-f3g9xun6) (one transaction or one bulk request), else one `RunGroup` call on a `batch.GroupRunner` (the Hatchet driver) when no job has `RunAt` or an `IdempotencyKey`, else one `Enqueue` per job in order. On an error the refs it returns have a zero `RunRef` for each job that did not start, so the caller can retry exactly those.

## Try it

```sh
cd examples
go run ./batch-lifecycle
```

The lines this page covers:

```text
-- a workflow waits for a batch
import done: 4 of 4 succeeded
-- many jobs without a batch
started 3 runs
```

The `import` workflow's payload is `4`, so its await step dispatched four `resize` jobs, and its `report` step ran once all four had succeeded. `go test ./batch-lifecycle` runs the same program and checks every line.

## Pitfalls

- **The wait is polling on retries.** Every look at an unfinished batch is a failed attempt ([`ErrBatchPending`](ds:block?id=batch-errbatchpending-qe5qk678)) in the engine's run history and in step metrics, and costs one store read. Pick [`AwaitPoll`](ds:block?id=batch-awaitpoll-9m6g9j83) to match how soon the workflow must resume.
- **An engine may cap retries.** A step's retry count is the polling budget ([`DefaultAwaitPolls`](ds:block?id=batch-defaultawaitpolls-muyng9sm)); an engine with a lower cap ends the wait early with `ErrBatchPending`. Check the engine's limit for a long batch.
- **The memory driver forgets the wait with the process.** Its retries live in memory, as all of its runs do (its [capability list](ds:block?id=memory-caps-yxkuq583) has no `CapDurable`); on a durable driver the wait survives a restart.
- **`build` runs once per run.** It runs on the first attempt only, while no batch exists under the [`AwaitID`](ds:block?id=batch-awaitid-sw2bkyj7) ([`await`](ds:block?id=batch-manager-await-3qf56kjz)); an error from it is retried on the same policy. Keep it quick and deterministic.
- **`EnqueueMany` through a wrapper is a loop.** `workflow.Use`, `middleware.WithCodec` and the test doubles implement neither bulk interface ([`EnqueueMany`](ds:block?id=batch-enqueuemany-6wx8q3ub)), so every job passes the wrapper's middleware one at a time.

## Related

- [Batches](batches.md): dispatch, callbacks, progress and cancel.
- [Change a batch after dispatch](lifecycle.md): `Add`, `RetryFailed`, `Before`, `Progress` and `Prune`.
- [Native group runs](native-groups.md): the `GroupRunner` that `EnqueueMany` uses on Hatchet.
- [Steps](../workflows/steps.md): step graphs and retry policies.
- Example: [examples/batch-lifecycle](../../examples/batch-lifecycle/).
