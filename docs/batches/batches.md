# Batches

A batch runs a group of jobs as one unit and enqueues a callback workflow when it ends: `Then` when every job succeeded, `Catch` on the first failure, `Finally` when every job has finished, whatever the outcome ([`recordFinish`](ds:block?id=batch-recordfinish-9b55fcjg)). Thumbnails for an upload, an export split into parts, a fan-out of emails: the app dispatches the jobs and learns about the whole, with progress to show while it runs. It is built on the public `Client` only (each job is one [`Enqueue`](ds:block?id=batch-manager-start-srx3z8d9) with two labels), so it runs on every driver.

## Before you start

- Any `workflow.Client`, the `batch` package (core module) and a [`batch.Store`](ds:block?id=batch-store-vp7qhnme) for the batch state. [`batch.NewMemoryStore()`](ds:block?id=batch-newmemorystore-xy8h2erx) keeps it in the process; a store shared by several processes is on [durability](durability.md).
- The callbacks are ordinary registered workflows. Each receives the batch's [`batch.Progress`](ds:block?id=batch-progress-539fkcc8) as its JSON payload ([`callback`](ds:block?id=batch-manager-callback-9sp5vupc)).
- The runnable program for this page is [examples/batches](../../examples/batches/).

## Dispatch a batch

[`batch.New(client, store, opts...)`](ds:block?id=batch-new-geazwpek) returns a `Manager`. Call [`Manager.Attach(ctx)`](ds:block?id=batch-manager-attach-xnchrce9) once, before `client.Start`, with the context you pass to `Start`: it connects the sources that tell the batch its jobs ended (on the memory driver, the run-finished hook ([`defaultSources`](ds:block?id=batch-manager-defaultsources-cgw2pdqt)), which cannot be registered after `Start`; see [completion](completion.md)).

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

[`Manager.Dispatch(ctx, batch.Batch{...})`](ds:block?id=batch-manager-dispatch-pdbtg7ks) saves the batch, enqueues its jobs in order and returns its ID. `Then`, `Catch` and `Finally` name registered workflows; empty means none ([`Batch`](ds:block?id=batch-batch-sbryq3mb)).

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

- `Then` runs once when every job succeeded and the batch was not cancelled ([`finish`](ds:block?id=batch-finish-2yzs4ctf)).
- `Catch` runs once, on the first failed job ([`recordFinish`](ds:block?id=batch-recordfinish-9b55fcjg)). Without `AllowFailures`, that failure also cancels the batch and the jobs still running. With `Batch.AllowFailures`, the other jobs keep running; `Then` still does not run, because not every job succeeded.
- `Finally` runs once when every job has finished, including after a cancel ([`finish`](ds:block?id=batch-finish-2yzs4ctf)).
- A job cancelled on its own is not a failure: it fires no `Catch`, but it stops `Then` ([`recordFinish`](ds:block?id=batch-recordfinish-9b55fcjg)).
- A job a step dropped with `workflow.Discard` (status `discarded`) counts as failed by default, so it fires `Catch` ([`counted`](ds:block?id=batch-state-counted-ezrpnwzb)). [`Batch.DiscardedAs`](ds:block?id=batch-batch-sbryq3mb) makes it count as cancelled or succeeded instead; see [Run states](../jobs/run-states.md#batches-and-discarded-jobs).

Each decision is made inside one [`Store.Update`](ds:block?id=batch-store-vp7qhnme), so a run end reported twice, or by two sources at once, still fires each callback at most once. A callback is an [`Enqueue`](ds:block?id=batch-manager-callback-9sp5vupc) of the named workflow whose payload is the batch's `Progress` at the moment the callback was decided:

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

If an `Enqueue` fails during [`Dispatch`](ds:block?id=batch-manager-dispatch-pdbtg7ks), the jobs not yet enqueued are recorded as cancelled ([`abortJobs`](ds:block?id=batch-manager-abortjobs-h5p9xpfp)), the batch is cancelled (which cancels the jobs already enqueued and, once they finish, runs `Finally`) and the error is returned together with the ID, so the caller can still inspect the batch. An `Enqueue` can fail and still have started its run (a timeout after the engine accepted the job); on a client with `workflow.CapCancelAll` `Dispatch` also cancels every unfinished run carrying the batch's [`batch.LabelBatch`](ds:block?id=batch-labelbatch-qsurvmc8) ([`cancelUnplaced`](ds:block?id=batch-manager-cancelunplaced-4t7aybp3)), so such a run is cancelled too. Without that capability it runs on unseen by the batch. A batch with no jobs is refused with `workflow.ErrInvalid` ([`checkBatch`](ds:block?id=batch-checkbatch-3aj6zx3v)).

## Track progress

[`Manager.Progress(ctx, id)`](ds:block?id=batch-manager-progress-etkyu46k) returns a [`batch.Progress`](ds:block?id=batch-progress-539fkcc8) snapshot: `Total`, `Pending`, `Succeeded`, `Failed`, `CancelledJobs`, `Discarded` (jobs that ended discarded, also counted in the bucket `DiscardedAs` names), whether the batch itself was `Cancelled`, and whether it is `Finished` (every job terminal). [`Processed()`](ds:block?id=batch-progress-processed-5vb2zg4h) is the number of jobs that ended in any status (`Total - Pending`), and [`Percent()`](ds:block?id=batch-progress-percent-2e5q7usv) that number as a whole percentage of `Total`, rounded down. An unknown ID returns an error wrapping `workflow.ErrNotFound`.

<!-- snippet: examples/batches/main.go#progress -->
```go
done, err := m.Progress(ctx, id) // a snapshot of the batch's counts
if err != nil {
	return err
}
fmt.Printf("progress %s: total=%d pending=%d succeeded=%d failed=%d processed=%d (%d%%) finished=%v\n",
	done.ID, done.Total, done.Pending, done.Succeeded, done.Failed, done.Processed(), done.Percent(), done.Finished)
```

After the first batch's callbacks ran, it prints `progress thumbs-1: total=3 pending=0 succeeded=3 failed=0 processed=3 (100%) finished=true`. `Progress` reads the store, so any process sharing the store sees the same counts; it is a snapshot, so poll it for a progress bar. The same `Progress` value is the JSON payload of every callback.

## Cancel a batch

[`Manager.Cancel(ctx, id)`](ds:block?id=batch-manager-cancel-7v23p4bf) cancels the batch and every job still running ([`markCancelled`](ds:block?id=batch-markcancelled-qxy4j7tu)). `Then` will not run; `Finally` runs once the cancelled jobs report back. Cancelling a finished or already cancelled batch is a no-op, and an unknown ID returns an error wrapping `workflow.ErrNotFound`.

<!-- snippet: examples/batches/main.go#cancel -->
```go
p, err := m.Progress(ctx, id)
if err != nil {
	return err
}
fmt.Printf("before cancel: %d of %d processed (%d%%)\n", p.Processed(), p.Total, p.Percent())
if err := m.Cancel(ctx, id); err != nil {
	return err
}
```

## Labels on a batch

[`Batch.Labels`](ds:block?id=batch-batch-sbryq3mb) are copied onto every `Then`, `Catch` and `Finally` job ([`callbackLabels`](ds:block?id=batch-callbacklabels-mz7rx2nn)), so a callback carries what the batch was for (a tenant, a user) and passes enqueue middleware that requires a label or routes on one. They are saved in the batch's state, so a callback enqueued by another process or after a restart carries them too. They are not put on the batch's own jobs; set those on each `Job`.

Each job of the batch gets two labels of the package's own ([`labelJobs`](ds:block?id=batch-labeljobs-k5gb85qs)): [`batch.LabelBatch`](ds:block?id=batch-labelbatch-qsurvmc8) (`workflow.batch.id`, the batch ID) and [`batch.LabelIndex`](ds:block?id=batch-labelindex-jtskuw4z) (`workflow.batch.index`, its position). A run end that carries both is how the batch knows which of its jobs finished; callbacks never carry them. The prefix `workflow.batch.` is reserved, and `Dispatch` refuses a job or a `Batch.Labels` with a label under it ([`refuseBatchLabels`](ds:block?id=batch-refusebatchlabels-68cnbjj7)), rather than overwriting it:

<!-- snippet: examples/batches/main.go#reserved -->
```go
_, err = m.Dispatch(ctx, batch.Batch{
	Jobs: []workflow.Job{{Workflow: "resize", Labels: map[string]string{batch.LabelBatch: "mine"}}},
})
fmt.Println("refused:", errors.Is(err, workflow.ErrInvalid))
```

## Batch IDs

A batch ID is 128 random bits as hex by default ([`idBytes`](ds:block?id=batch-idbytes-xvspfaku), [`newID`](ds:block?id=batch-newid-jj44pfd7)). [`batch.WithIDFunc(fn)`](ds:block?id=batch-withidfunc-a9ccxauz) makes IDs with `fn` instead, for example a UUIDv7 or a ULID so IDs sort by time or match the app's own. The example numbers them `thumbs-1`, `thumbs-2` so its output is the same on every run:

<!-- snippet: examples/batches/main.go#idfunc -->
```go
batch.WithIDFunc(func() (batch.ID, error) { // the default is 128 random bits as hex
	seq++
	return batch.ID("thumbs-" + strconv.Itoa(seq)), nil
}),
```

An ID must be unique across every process sharing the store ([`Store.Create`](ds:block?id=batch-memorystore-create-uxe6zyqx) refuses a repeat) and non-empty: `Dispatch` fails with `fn`'s error, or `workflow.ErrInvalid` for an empty ID, and saves nothing:

<!-- snippet: examples/batches/main.go#empty-id -->
```go
blank := batch.New(client, store, batch.WithIDFunc(func() (batch.ID, error) { return "", nil }))
_, err = blank.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "resize"}}})
fmt.Println("refused:", errors.Is(err, workflow.ErrInvalid)) // nothing was saved or enqueued
```

The ID appears in the `workflow.batch.id` label and in the callbacks' idempotency keys ([`batch:<id>:<kind>`](ds:block?id=batch-callbackkey-agsg3nsq)), so keep it to characters the engine accepts in a label value. A nil `fn` keeps the default. A counter like the example's is only safe in one process; across processes use an ID the store cannot see twice, such as a UUID.

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
progress thumbs-1: total=3 pending=0 succeeded=3 failed=0 processed=3 (100%) finished=true
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
-- an empty batch ID
refused: true
-- a client that starts a group in one call
group-1: grouped=true, RunGroup calls=1
group-1: finally succeeded=3 of 3
group-2: grouped=false, RunGroup calls=1
group-2: finally succeeded=3 of 3
-- Hatchet bulk-trigger chunks
chunks of at most 500 runs and 1048576 bytes
over the ceiling refused: true
```

Callbacks are separate runs that may finish in any order, so the example sorts each group of lines. The fourth batch is on [completion › callbacks that fail to enqueue](completion.md#callbacks-that-fail-to-enqueue), and the group lines on [native group runs](native-groups.md). `go test ./batches` runs the same program and checks every line.

## Pitfalls

- **`Attach` before `Start`.** On a driver whose completion source is the run-finished hook ([`HookSource`](ds:block?id=batch-hooksource-attach-3ympu39g)), a `Manager` attached after `Start` fails, because the hook cannot be registered then; without `Attach`, no job end ever reaches the batch.
- **Callbacks are at least once without idempotency keys.** On a driver with `CapIdempotencyKey`, every callback carries the key [`batch:<id>:<kind>`](ds:block?id=batch-callbackkey-agsg3nsq) ([`callback`](ds:block?id=batch-manager-callback-9sp5vupc)), so a retry after an enqueue that landed but was not recorded cannot double it. On a driver without it, a crash at that moment can enqueue a callback twice; make callbacks idempotent.
- **`Catch` sees the batch at the failure.** Its `Progress` is a snapshot taken when the failure was recorded, before the other jobs were cancelled ([`recordFinish`](ds:block?id=batch-recordfinish-9b55fcjg)); read `Finally` for the final counts.
- **A job's labels are copied, not mutated.** `Dispatch` clones each job's labels before adding the batch labels ([`labelJobs`](ds:block?id=batch-labeljobs-k5gb85qs)), so reusing a `Job` value across batches is safe.
- **`Progress` is a snapshot, not a stream.** It reads the store once ([`Manager.Progress`](ds:block?id=batch-manager-progress-etkyu46k)); a job that ends a moment later is not in it. For a push on every job end, set `Batch.Progress` to a workflow ([lifecycle](lifecycle.md#step-1-report-the-start-and-every-job-end)).
- **[`Percent`](ds:block?id=batch-progress-percent-2e5q7usv) rounds down.** 199 of 200 jobs ended reads 99, and it reads 100 only once every job ended. Test `Finished` to decide that a batch is done; `Pending` is the number of jobs not yet ended.
- **[`MemoryStore`](ds:block?id=batch-memorystore-hjud4syz) is per process.** Every `Manager` reporting into a batch must share its store; in several processes, use a durable store ([durability](durability.md)).

## Related

- [How a batch learns a job ended](completion.md): sources, polling and callback retries.
- [Change a batch after dispatch](lifecycle.md): `Add`, `RetryFailed`, `Before`, `Progress` and `Prune`.
- [Wait for a batch in a workflow](await.md): `AwaitStep` and `EnqueueMany`.
- [Test batches](../testing/batches-and-chains.md): `AssertBatched` on the Fake.
- [Durability](durability.md): a SQL store and dispatches that resume after a crash.
- [Native group runs](native-groups.md): one engine call for a whole batch on Hatchet.
- [React when a run finishes](../observability/run-finished.md): the hook a batch listens to.
- Example: [examples/batches](../../examples/batches/).
