# Batches

A batch runs a group of jobs as one unit and enqueues a callback workflow when it ends: `Then` when every job succeeded, `Catch` on the first failure, `Finally` when every job has finished, whatever the outcome. Thumbnails for an upload, an export split into parts, a fan-out of emails: the app dispatches the jobs and learns about the whole, with progress to show while it runs. It is built on the public `Client` only, so it runs on every driver.

## Before you start

- Any `workflow.Client`, the `batch` package (core module) and a `batch.Store` for the batch state. `batch.NewMemoryStore()` keeps it in the process; a store shared by several processes is on [durability](durability.md).
- The callbacks are ordinary registered workflows. Each receives the batch's `batch.Progress` as its JSON payload.
- The runnable program for this page is [examples/batches](../../examples/batches/).

## Dispatch a batch

`batch.New(client, store, opts...)` returns a `Manager`. Call `Manager.Attach(ctx)` once, before `client.Start`, with the context you pass to `Start`: it connects the sources that tell the batch its jobs ended (on the memory driver, the run-finished hook, which cannot be registered after `Start`; see [completion](completion.md)).

<!-- snippet: examples/batches/main.go#manager -->
```go
seq := 0
clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
store := batch.NewMemoryStore()
m := batch.New(client, store,
	batch.WithClock(clk),
	batch.WithIDFunc(func() (batch.ID, error) { // the default is 128 random bits as hex
		seq++
		return batch.ID("thumbs-" + strconv.Itoa(seq)), nil
	}),
	batch.WithManualCallbackRetry(), // this program calls RetryCallbacks itself
)
if err := m.Attach(ctx); err != nil { // before Start: it registers the run-finished hook
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
```

`Manager.Dispatch(ctx, batch.Batch{...})` saves the batch, enqueues its jobs in order and returns its ID. `Then`, `Catch` and `Finally` name registered workflows; empty means none.

<!-- snippet: examples/batches/main.go#dispatch -->
```go
id, err := m.Dispatch(ctx, batch.Batch{
	Name: "thumbnails",
	Jobs: []workflow.Job{
		{Workflow: "resize", Payload: []byte(`"a.png"`)},
		{Workflow: "resize", Payload: []byte(`"b.png"`)},
		{Workflow: "resize", Payload: []byte(`"c.png"`)},
	},
	Then:    "then",
	Catch:   "catch",
	Finally: "finally",
	Labels:  map[string]string{"tenant": "acme"}, // copied onto Then, Catch and Finally
})
if err != nil {
	return err
}
```

The semantics:

- `Then` runs once when every job succeeded and the batch was not cancelled.
- `Catch` runs once, on the first failed job. Without `AllowFailures`, that failure also cancels the batch and the jobs still running. With `Batch.AllowFailures`, the other jobs keep running; `Then` still does not run, because not every job succeeded.
- `Finally` runs once when every job has finished, including after a cancel.
- A job cancelled on its own is not a failure: it fires no `Catch`, but it stops `Then`.
- A job a step dropped with `workflow.Discard` (status `discarded`) counts as failed by default, so it fires `Catch`. `Batch.DiscardedAs` makes it count as cancelled or succeeded instead; see [Run states](../jobs/run-states.md#batches-and-discarded-jobs).

Each decision is made inside one `Store.Update`, so a run end reported twice, or by two sources at once, still fires each callback at most once. A callback is an `Enqueue` of the named workflow whose payload is the batch's `Progress` at the moment the callback was decided:

<!-- snippet: examples/batches/main.go#callback -->
```go
cb := workflow.Workflow{
	Name: kind,
	Steps: []workflow.Step{{Name: "run", Run: workflow.Define(func(ctx context.Context, p batch.Progress) (string, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		callbacks <- fmt.Sprintf("%s %s tenant=%s succeeded=%d failed=%d cancelledJobs=%d cancelled=%v",
			kind, p.ID, info.Labels["tenant"], p.Succeeded, p.Failed, p.CancelledJobs, p.Cancelled)
		return "", nil
	})}},
}
```

In the example's second batch, one job fails: `Catch` receives `failed=1 cancelledJobs=0` (decided at the failure), and `Finally` receives `failed=1 cancelledJobs=1` once the cancelled job has reported back.

If an `Enqueue` fails during `Dispatch`, the jobs not yet enqueued are recorded as cancelled, the batch is cancelled (which cancels the jobs already enqueued and, once they finish, runs `Finally`) and the error is returned together with the ID, so the caller can still inspect the batch. A batch with no jobs is refused with `workflow.ErrInvalid`.

## Track progress

`Manager.Progress(ctx, id)` returns a `batch.Progress` snapshot: `Total`, `Pending`, `Succeeded`, `Failed`, `CancelledJobs`, `Discarded` (jobs that ended discarded, also counted in the bucket `DiscardedAs` names), whether the batch itself was `Cancelled`, and whether it is `Finished` (every job terminal). `Processed()` is the number of jobs that ended in any status, and `Percent()` that number as a whole percentage of `Total`. An unknown ID returns an error wrapping `workflow.ErrNotFound`.

## Cancel a batch

`Manager.Cancel(ctx, id)` cancels the batch and every job still running. `Then` will not run; `Finally` runs once the cancelled jobs report back. Cancelling a finished or already cancelled batch is a no-op, and an unknown ID returns an error wrapping `workflow.ErrNotFound`.

<!-- snippet: examples/batches/main.go#cancel -->
```go
p, err := m.Progress(ctx, id)
if err != nil {
	return err
}
fmt.Fprintf(out, "before cancel: %d of %d processed (%d%%)\n", p.Processed(), p.Total, p.Percent())
if err := m.Cancel(ctx, id); err != nil {
	return err
}
```

## Labels on a batch

`Batch.Labels` are copied onto every `Then`, `Catch` and `Finally` job, so a callback carries what the batch was for (a tenant, a user) and passes enqueue middleware that requires a label or routes on one. They are saved in the batch's state, so a callback enqueued by another process or after a restart carries them too. They are not put on the batch's own jobs; set those on each `Job`.

Each job of the batch gets two labels of the package's own: `batch.LabelBatch` (`workflow.batch.id`, the batch ID) and `batch.LabelIndex` (`workflow.batch.index`, its position). A run end that carries both is how the batch knows which of its jobs finished; callbacks never carry them. The prefix `workflow.batch.` is reserved, and `Dispatch` refuses a job or a `Batch.Labels` with a label under it, rather than overwriting it:

<!-- snippet: examples/batches/main.go#reserved -->
```go
_, err = m.Dispatch(ctx, batch.Batch{
	Jobs: []workflow.Job{{Workflow: "resize", Labels: map[string]string{batch.LabelBatch: "mine"}}},
})
fmt.Fprintln(out, "refused:", errors.Is(err, workflow.ErrInvalid))
```

## Batch IDs

A batch ID is 128 random bits as hex by default. `batch.WithIDFunc(fn)` makes IDs with `fn` instead, for example a UUIDv7 or a ULID so IDs sort by time or match the app's own; the example numbers them `thumbs-1`, `thumbs-2`. An ID must be unique across every process sharing the store (`Store.Create` refuses a repeat) and non-empty: `Dispatch` fails with `fn`'s error, or `workflow.ErrInvalid` for an empty ID, and saves nothing. The ID appears in the `workflow.batch.id` label and in the callbacks' idempotency keys (`batch:<id>:<kind>`), so keep it to characters the engine accepts in a label value. A nil `fn` keeps the default.

## Try it

```sh
cd examples
go run ./batches
```

```text
-- every job succeeds
dispatched thumbs-1
finally thumbs-1 tenant=acme succeeded=3 failed=0 cancelledJobs=0 cancelled=false
then thumbs-1 tenant=acme succeeded=3 failed=0 cancelledJobs=0 cancelled=false
-- a job fails
catch thumbs-2 tenant=acme succeeded=0 failed=1 cancelledJobs=0 cancelled=true
finally thumbs-2 tenant=acme succeeded=0 failed=1 cancelledJobs=1 cancelled=true
-- the app cancels
before cancel: 0 of 2 processed (0%)
finally thumbs-3 tenant=acme succeeded=0 failed=0 cancelledJobs=2 cancelled=true
-- a Finally that failed to enqueue
pending finally: attempts=1 last error: batch thumbs-4: enqueue finally callback "finally": engine unavailable
finally thumbs-4 tenant=globex succeeded=1 failed=0 cancelledJobs=0 cancelled=false
-- a reserved label
refused: true
```

Callbacks are separate runs that may finish in any order, so the example sorts each group of lines. The fourth batch is on [completion › callbacks that fail to enqueue](completion.md#callbacks-that-fail-to-enqueue). `go test ./batches` runs the same program and checks every line.

## Pitfalls

- **`Attach` before `Start`.** On a driver whose completion source is the run-finished hook, a `Manager` attached after `Start` fails, because the hook cannot be registered then; without `Attach`, no job end ever reaches the batch.
- **Callbacks are at least once without idempotency keys.** On a driver with `CapIdempotencyKey`, every callback carries the key `batch:<id>:<kind>`, so a retry after an enqueue that landed but was not recorded cannot double it. On a driver without it, a crash at that moment can enqueue a callback twice; make callbacks idempotent.
- **`Catch` sees the batch at the failure.** Its `Progress` is a snapshot taken when the failure was recorded, before the other jobs were cancelled; read `Finally` for the final counts.
- **A job's labels are copied, not mutated.** `Dispatch` clones each job's labels before adding the batch labels, so reusing a `Job` value across batches is safe.
- **`MemoryStore` is per process.** Every `Manager` reporting into a batch must share its store; in several processes, use a durable store ([durability](durability.md)).

## Related

- [How a batch learns a job ended](completion.md): sources, polling and callback retries.
- [Change a batch after dispatch](lifecycle.md): `Add`, `RetryFailed`, `Before`, `Progress` and `Prune`.
- [Wait for a batch in a workflow](await.md): `AwaitStep` and `EnqueueMany`.
- [Test batches](../testing/batches-and-chains.md): `AssertBatched` on the Fake.
- [Durability](durability.md): a SQL store and dispatches that resume after a crash.
- [Native group runs](native-groups.md): one engine call for a whole batch on Hatchet.
- [React when a run finishes](../observability/run-finished.md): the hook a batch listens to.
- Example: [examples/batches](../../examples/batches/).
