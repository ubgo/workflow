# Native group runs

By default `Dispatch` enqueues a batch's jobs one at a time: a 10,000-job batch is 10,000 `Enqueue` calls from the dispatching process. A driver that can start many runs in one engine call implements an optional interface, and `Dispatch` uses it with no change to the app's code. The Hatchet driver offers two: bulk triggers from the dispatching process, and a coordinator run that starts every job on the engine's side.

## Before you start

- A batch `Manager` ([batches](batches.md)) on a client that implements `batch.GroupRunner` or `batch.GroupSpawner`; the Hatchet driver implements both.
- Only eligible batches take these paths: no job with `RunAt` or an `IdempotencyKey`, since those need `Enqueue`'s own handling. Any other batch is enqueued one job at a time.
- The runnable program for this page is [examples/batches](../../examples/batches/); its Hatchet part runs when `HATCHET_CLIENT_TOKEN` is set.

## Native group runs

`batch.GroupRunner` has one method, `RunGroup(ctx, group, jobs)`, which starts every job and returns one `RunRef` per job in order. `Dispatch` calls it when the client implements it and every job is eligible. The batch learns of each child's end exactly as for an enqueued job, through its sources, so an implementation keeps two promises: each job's labels (which carry the batch labels) reach that child's `RunEvent.Labels`, and each returned ref answers `Status` and `Cancel` like a ref from `Enqueue`.

An error wrapping `workflow.ErrUnsupported`, returned before anything started, makes `Dispatch` fall back to one `Enqueue` per job. Any other error means the group did not fully start; the refs of the children that did start are kept and `Dispatch` cancels the batch, which cancels them.

`hatchet.Client.RunGroup` implements it as Hatchet bulk triggers: one `RunMany` call per chunk of jobs of the same workflow, so a batch of N jobs costs about N/1000 calls instead of N. Each run carries exactly what `Enqueue` stamps (input envelope, labels, idempotency metadata, priority). A chunk is at most `hatchet.Config.GroupChunkRuns` runs (default and maximum `hatchet.DefaultGroupChunkRuns`, 1000) and at most `GroupChunkBytes` of JSON input and metadata (default and maximum `hatchet.DefaultGroupChunkBytes`, 2 MiB), so the SDK never splits a chunk again and a failed chunk maps back to its jobs; [Hatchet tunings](../operations/tuning.md#hatchet-tunings) covers when to lower them. Every job is checked before anything starts.

Nothing is needed to turn it on: a batch dispatched on the Hatchet client with eligible jobs uses it.

## The Hatchet batch coordinator

Bulk triggers still run in the dispatching process: if it dies between chunks, the remaining jobs wait for a resumer. `batch.GroupSpawner` moves the work to the engine. `SpawnGroup(ctx, group, jobs)` starts one coordinator run whose task starts every job as its own child run, so `Dispatch` is one trigger whatever the batch's size and the engine sees the jobs through to started; `CancelGroup` cancels the coordinator and every unfinished child, finding them by label; `SpawnsGroups()` reports whether the client wants the path for new batches.

On the Hatchet driver, `hatchet.Config.BatchCoordinator` turns it on, and `Start` then registers the coordinator workflow on the worker:

<!-- snippet: examples/batches/hatchet.go#coordinator -->
```go
c := hatchet.New(hatchet.Config{
	WorkerName:       "batches-example",
	BatchCoordinator: true, // Start registers the coordinator workflow on this worker
})
store := batch.NewMemoryStore() // a sqlstore BatchStore when several processes share batches
m := batch.New(c, store, batch.WithCompletionSource(
	batch.HookSource(),                     // fast, best effort on Hatchet
	c.BatchSource(store, time.Second, nil), // the LabelSource a coordinated batch needs
))
if err := m.Attach(ctx); err != nil {
	return err
}
```

The children get no `RunRef`: the engine names a child only when the coordinator starts it, after `Dispatch` has returned. The batch keeps the coordinator as `State.Parent` instead and learns each child's end by its `workflow.batch.id` label, which is why a coordinated batch needs a `batch.LabelSource` such as the Hatchet `BatchSource` among the Manager's sources. `Dispatch` refuses one without it, before saving anything:

<!-- snippet: examples/batches/hatchet.go#needs-label-source -->
```go
plain := batch.New(c, batch.NewMemoryStore(), batch.WithCompletionSource(batch.PollSource(time.Second)))
if err := plain.Attach(ctx); err != nil {
	return err
}
_, err = plain.Dispatch(ctx, batch.Batch{Jobs: jobs[:1]})
fmt.Fprintln(out, "hatchet: coordinated batch without a LabelSource refused:", errors.Is(err, workflow.ErrInvalid))
```

The coordinator trigger is idempotent per batch, so a resumed dispatch that calls `SpawnGroup` again gets the first coordinator back rather than a second one. A batch whose jobs' input and metadata exceed `hatchet.Config.BatchCoordinatorMaxInput` (default about 2 MiB, which fits the engine's default 4 MiB message) is declined with `ErrUnsupported` and started as bulk triggers instead. `BatchCoordinatorTimeout` bounds one attempt of the coordinator's task (and is how long a coordinator whose worker died waits before the engine retries it elsewhere); `BatchCoordinatorRetry` is its retry policy.

## Try it

```sh
cd examples
HATCHET_CLIENT_TOKEN=... HATCHET_CLIENT_HOST_PORT=localhost:7004 HATCHET_CLIENT_TLS_STRATEGY=none go run ./batches
```

Against the shared local stack's engine (gRPC on port 7004, no TLS), after the memory driver's lines, the Hatchet part dispatches 50 jobs through the coordinator:

```text
hatchet: coordinated=true, one parent run=true
hatchet: finally succeeded=50 of 50
hatchet: coordinated batch without a LabelSource refused: true
```

Without the token the Hatchet part does not run, and `go test ./batches` skips its test with the reason.

## Pitfalls

- **Wrappers hide the group paths.** `middleware.WrapClient`, `middleware.WithCodec` and `workflow.Use` embed `workflow.Client`, so a batch dispatched through them enqueues each job through the wrapper. That is deliberate (a codec is never bypassed), but it also means no bulk trigger and no coordinator; build the Manager on the driver's own client when the jobs need neither.
- **Set `BatchCoordinator` alike everywhere.** Every process sharing a batch store should set it the same way, so a resumed dispatch finds the coordinator; a process with it off cannot run the coordinator's task.
- **A coordinated batch's jobs have no refs.** `Progress` counts them as usual, but `State.Jobs[i].Ref` stays zero; look children up on the engine by the `workflow.batch.id` label.
- **Keys and delays opt a batch out.** One job with `RunAt` or an `IdempotencyKey` sends the whole batch down the one-`Enqueue`-per-job path.
- **Hatchet chunk limits have a ceiling.** `Start` refuses a `GroupChunkRuns` or `GroupChunkBytes` above the default, because the SDK would split the call itself and could no longer say which runs failed.

## Related

- [Batches](batches.md): dispatch, progress, cancel.
- [Batch completion](completion.md#on-hatchet): `BatchSource`, the label source a coordinated batch needs.
- [Durability](durability.md): how a dispatch that died part-way is resumed on each path.
- [Hatchet driver](../drivers/hatchet.md): the rest of `hatchet.Config`.
- Example: [examples/batches](../../examples/batches/).
