# Native group runs

By default `Dispatch` enqueues a batch's jobs one at a time: a 10,000-job batch is 10,000 `Enqueue` calls from the dispatching process. A driver that can start many runs in one engine call implements an optional interface, and `Dispatch` uses it with no change to the app's code. The Hatchet driver offers two: bulk triggers from the dispatching process, and a coordinator run that starts every job on the engine's side.

## Before you start

- A batch `Manager` ([batches](batches.md)) on a client that implements `batch.GroupRunner` or `batch.GroupSpawner`; the Hatchet driver implements both.
- Only eligible batches take these paths: no job with `RunAt` or an `IdempotencyKey`, since those need `Enqueue`'s own handling. Any other batch is enqueued one job at a time.
- The runnable program for this page is [examples/batches](../../examples/batches/): `groups.go` runs a `GroupRunner` on the memory driver and checks the Hatchet chunk limits with no engine, and `hatchet.go` runs the coordinator when `HATCHET_CLIENT_TOKEN` is set.

## Native group runs

`batch.GroupRunner` has one method, `RunGroup(ctx, group, jobs)`, which starts every job and returns one `RunRef` per job in order. `Dispatch` calls it when the client implements it and every job is eligible. The batch learns of each child's end exactly as for an enqueued job, through its sources, so an implementation keeps two promises: each job's labels (which carry the batch labels) reach that child's `RunEvent.Labels`, and each returned ref answers `Status` and `Cancel` like a ref from `Enqueue`.

An error wrapping `workflow.ErrUnsupported`, returned before anything started, makes `Dispatch` fall back to one `Enqueue` per job. Any other error means the group did not fully start; the refs of the children that did start are kept and `Dispatch` cancels the batch, which cancels them.

### Step 1: implement `GroupRunner` on a client

The example's client embeds the memory driver and adds `RunGroup`. It loops over `Enqueue` where a real driver would make one engine call per chunk, and counts its calls so the output shows which path a batch took. Embedding keeps the driver's run-finished hook, so the batch hears of each child's end as for an enqueued job:

<!-- snippet: examples/batches/groups.go#group-runner -->
```go
// groupClient is a driver that can start a whole batch in one call: it
// implements batch.GroupRunner, so Manager.Dispatch hands it every
// eligible batch at once. A real driver makes one engine call per chunk
// (Hatchet's RunGroup is one bulk trigger per 1000 runs); this one loops
// over Enqueue and counts the calls, so the output can show which path a
// batch took. Embedding *memory.Client keeps the run-finished hook, so
// the batch hears of every child's end as for an enqueued job.
type groupClient struct {
	*memory.Client
	groups atomic.Int32 // RunGroup calls
}

// RunGroup starts jobs and returns one RunRef per job, in order. Each job
// keeps its labels, which carry the batch labels the batch needs.
func (g *groupClient) RunGroup(ctx context.Context, group string, jobs []workflow.Job) ([]workflow.RunRef, error) {
	g.groups.Add(1)
	refs := make([]workflow.RunRef, len(jobs))
	for i, job := range jobs {
		ref, err := g.Client.Enqueue(ctx, job)
		if err != nil {
			return refs, fmt.Errorf("group %s job %d: %w", group, i, err) // the refs so far let Dispatch cancel what started
		}
		refs[i] = ref
	}
	return refs, nil
}
```

### Step 2: dispatch as usual

Nothing in the app's code changes. `Dispatch` sees that the client implements `GroupRunner` and that every job is eligible, sets `State.Grouped` and starts the whole batch in one call:

<!-- snippet: examples/batches/groups.go#group-dispatch -->
```go
id, err := m.Dispatch(ctx, batch.Batch{Jobs: jobs, Finally: "group-finally"}) // eligible: one RunGroup call
if err != nil {
	return err
}
st, err := store.Get(ctx, id)
if err != nil {
	return err
}
fmt.Printf("%s: grouped=%v, RunGroup calls=%d\n", id, st.Grouped, g.groups.Load())
```

It prints `group-1: grouped=true, RunGroup calls=1`. One job with an `IdempotencyKey` (or a `RunAt`) makes the batch ineligible, and `Dispatch` enqueues every job of it one at a time; the count stays at one:

<!-- snippet: examples/batches/groups.go#group-ineligible -->
```go
keyed := append([]workflow.Job{{Workflow: "resize", IdempotencyKey: "resize:cover"}}, jobs[1:]...)
id, err = m.Dispatch(ctx, batch.Batch{Jobs: keyed, Finally: "group-finally"}) // one key: one Enqueue per job
if err != nil {
	return err
}
st, err = store.Get(ctx, id)
if err != nil {
	return err
}
fmt.Printf("%s: grouped=%v, RunGroup calls=%d\n", id, st.Grouped, g.groups.Load())
```

It prints `group-2: grouped=false, RunGroup calls=1`. Both batches reach `Finally` with every job succeeded.

`hatchet.Client.RunGroup` implements it as Hatchet bulk triggers: one `RunMany` call per chunk of jobs of the same workflow, so a batch of N jobs costs about N/1000 calls instead of N. Each run carries exactly what `Enqueue` stamps (input envelope, labels, idempotency metadata, priority). A chunk is at most `hatchet.Config.GroupChunkRuns` runs (default and maximum `hatchet.DefaultGroupChunkRuns`, 1000) and at most `GroupChunkBytes` of JSON input and metadata (default and maximum `hatchet.DefaultGroupChunkBytes`, 2 MiB), so the SDK never splits a chunk again and a failed chunk maps back to its jobs; [Hatchet tunings](../operations/tuning.md#hatchet-tunings) covers when to lower them. Every job is checked before anything starts.

Nothing is needed to turn it on: a batch dispatched on the Hatchet client with eligible jobs uses it.

### Step 3: size the Hatchet chunks

The defaults fit an engine with the default 4 MiB gRPC message. For an engine with a lower limit, lower the chunk sizes in `hatchet.Config`; the client is still a `GroupRunner`:

<!-- snippet: examples/batches/groups.go#chunks -->
```go
bulk := hatchet.New(hatchet.Config{
	WorkerName:      "thumbnails",
	GroupChunkRuns:  chunkRuns,  // runs per bulk trigger; at most hatchet.DefaultGroupChunkRuns (1000)
	GroupChunkBytes: chunkBytes, // JSON input and metadata per bulk trigger; at most hatchet.DefaultGroupChunkBytes (2 MiB)
})
var _ batch.GroupRunner = bulk // Dispatch on bulk starts an eligible batch with RunGroup
```

The defaults are also the ceilings. `Start` refuses a larger value with `workflow.ErrInvalid` before it connects, so a misconfiguration fails at boot rather than at the first big batch:

<!-- snippet: examples/batches/groups.go#chunk-ceiling -->
```go
// Twice the ceiling: refused before it connects to any engine.
tooBig := hatchet.New(hatchet.Config{GroupChunkRuns: 2 * hatchet.DefaultGroupChunkRuns})
err = tooBig.Start(ctx)
fmt.Println("over the ceiling refused:", errors.Is(err, workflow.ErrInvalid))
```

It prints `over the ceiling refused: true` with no engine running. A single job larger than `GroupChunkBytes` is not refused; it goes in a chunk of its own.

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
fmt.Println("hatchet: coordinated batch without a LabelSource refused:", errors.Is(err, workflow.ErrInvalid))
```

The coordinator trigger is idempotent per batch, so a resumed dispatch that calls `SpawnGroup` again gets the first coordinator back rather than a second one. A batch whose jobs' input and metadata exceed `hatchet.Config.BatchCoordinatorMaxInput` (default about 2 MiB, which fits the engine's default 4 MiB message) is declined with `ErrUnsupported` and started as bulk triggers instead. `BatchCoordinatorTimeout` bounds one attempt of the coordinator's task (and is how long a coordinator whose worker died waits before the engine retries it elsewhere); `BatchCoordinatorRetry` is its retry policy.

## Try it

The group runner and the chunk limits need no engine:

```sh
cd examples
go run ./batches
```

After the lines of [Batches](batches.md#try-it), it prints:

```text
-- a client that starts a group in one call
group-1: grouped=true, RunGroup calls=1
group-1: finally succeeded=3 of 3
group-2: grouped=false, RunGroup calls=1
group-2: finally succeeded=3 of 3
-- Hatchet bulk-trigger chunks
chunks of at most 500 runs and 1048576 bytes
over the ceiling refused: true
```

`go test ./batches` checks these lines in `TestGroups`. The coordinator needs a live engine:

```sh
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

- **One ref per job, in order.** `Dispatch` treats a `RunGroup` that returns no error but a different number of refs as a failed group and cancels the batch. When a group fails part-way, return one entry per job with a zero ref for each job that did not start: `Dispatch` keeps the refs it can place and cancels those children. Nil refs, or any other length, make it record every job as cancelled without a ref; the children that did start are then found by their `batch.LabelBatch` label and cancelled with one `CancelAll`, which needs `workflow.CapCancelAll` on the client. Without it `Dispatch`'s error carries a `*workflow.UnsupportedError` naming `CapCancelAll`, and those children run on unseen by the batch, so prefer positional refs.
- **Decline only before starting.** `ErrUnsupported` means "nothing started, enqueue them instead". Returned after some children started, it makes `Dispatch` enqueue every job again, so those jobs run twice.
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
