# Batch completion

A batch decides its callbacks from its jobs' ends, so it needs to hear about every one. This page covers where those ends come from (the run-finished hook, a status poller, or Hatchet's run list), how to choose, and what happens when a decided callback fails to enqueue.

## Before you start

- A batch `Manager` ([batches](batches.md)).
- The runnable program for this page is [examples/batches](../../examples/batches/); its Hatchet part runs when `HATCHET_CLIENT_TOKEN` is set.

## How a batch learns a job ended

A `batch.Source` tells a `Manager` that batch jobs ended; `Manager.Attach(ctx)` connects the Manager's sources, once, before `client.Start`. A source reports through `Manager.RunFinished(ctx, labels, status)`, which ignores repeats, so several sources may report the same job: the first terminal status recorded wins. Unless `batch.WithCompletionSource(srcs...)` names them, `Attach` picks the sources by what the client can do:

| Client | Default sources | Why |
|---|---|---|
| a `workflow.RunNotifier` declaring `CapRunFinished` (memory, SQL, Redis) | `batch.HookSource()` | every run end is guaranteed to arrive |
| a `RunNotifier` without it (Hatchet) | `HookSource()` plus `batch.PollSource(interval)` | the hook is fast; the poller is the safety net for an event the hook loses |
| any other client | `PollSource(interval)` | `Client.Status` exists on every driver |

- `HookSource()` registers the batch on the client's run-finished hook. Its `Attach` fails with an `UnsupportedError` when the client is not a `RunNotifier`.
- `PollSource(interval)` calls `Client.Status` for every unfinished job of every open batch each interval, on the Manager's clock, and also retries pending callbacks and resumes dead dispatches. A `Status` error for one job is logged and retried next tick, never guessed into a status. Its `Attach` fails with `workflow.ErrInvalid` for an interval that is not positive.
- `batch.WithStatusPoll(interval)` changes the default poller's interval (`batch.DefaultPollInterval`, five seconds). Each tick costs one `Status` call per unfinished job, so it trades engine load against callback latency. It does not add a poller where the defaults would not; name `PollSource` in `WithCompletionSource` for that.

`WithCompletionSource` replaces the defaults entirely, for a native engine feed (below), or `HookSource()` plus `PollSource` on a client whose hook is guaranteed but which the app wants double-checked. `Attach` returns the first source's error, and a `Manager` whose `Attach` failed must not be used.

An app with its own feed of run ends (a message bus, an engine webhook) can skip sources altogether and call `Manager.RunFinished` with a finished run's `Job.Labels` and terminal status. A run that is not a batch job, a non-terminal status, an unknown batch or a repeat is ignored and returns nil, so it can be wired to every run.

## On Hatchet

The default poller costs one `Status` call per unfinished job per tick. `hatchet.Client.BatchSource(store, interval, logger)` is a source of the Hatchet driver's own that makes one run-list call per unfinished batch each interval instead, filtered by the batch's `workflow.batch.id` label and the terminal statuses. `store` must be the `Store` the Manager uses, because the source reads the open batches from it. A job whose ref holds only a `ScheduledID` (a delayed start that has not fired) has no engine run to list, so the source asks `Status` for that one job. A list error is logged and retried next tick. Each tick also retries pending callbacks, as the poller does; a nil logger means `hatchet.Config.Logger`.

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

`BatchSource` is a `batch.LabelSource`: it finds jobs by label, without their refs. That is what a batch started by the Hatchet batch coordinator needs, whose jobs never get refs, and `Dispatch` refuses a coordinated batch on a Manager with no `LabelSource` attached ([the Hatchet batch coordinator](native-groups.md#the-hatchet-batch-coordinator)).

## Callbacks that fail to enqueue

A decided callback is saved as pending (`State.Pending`, one `batch.PendingCallback` per callback) in the same `Store.Update` as the decision, then enqueued, then removed. One whose `Enqueue` fails stays pending, with its `Attempts`, `LastError` and a `RetryAt` when the current attempt's claim ends, and is retried after that claim on whichever comes first:

- the status poller's next tick (and the Hatchet `BatchSource`'s);
- the next job end recorded for that batch;
- the retry loop `Attach` starts when no source is a poller, which calls `Manager.RetryCallbacks` every claim length;
- a call of `Manager.RetryCallbacks(ctx)` by the app, which retries every due pending callback across every batch in the store.

`batch.WithCallbackRetry(d)` sets the claim length (`batch.DefaultCallbackRetry`, thirty seconds), which must outlast one `Enqueue` so two attempts never overlap. `batch.WithManualCallbackRetry()` stops `Attach` from starting the retry loop, for an app that calls `RetryCallbacks` on its own schedule. The example does that, with a client whose first `Finally` enqueue fails:

<!-- snippet: examples/batches/main.go#retry -->
```go
st, err := store.Get(ctx, id)
if err != nil {
	return err
}
for _, pc := range st.Pending {
	fmt.Fprintf(out, "pending %s: attempts=%d last error: %s\n", pc.Kind, pc.Attempts, pc.LastError)
}
clk.Advance(batch.DefaultCallbackRetry) // the failed attempt's claim ends
if err := m.RetryCallbacks(ctx); err != nil {
	return err
}
```

A batch stays open (`State.Open`, listed by `Store.Unfinished`) while a callback is pending, even after every job has finished. When the client declares `workflow.CapIdempotencyKey`, every callback carries the key `batch:<id>:<kind>`, so a retry after an enqueue that landed but was not recorded cannot double it.

## Try it

```sh
cd examples
go run ./batches
```

The completion-related lines of the output:

```text
-- a Finally that failed to enqueue
pending finally: attempts=1 last error: batch thumbs-4: enqueue finally callback "finally": engine unavailable
finally thumbs-4 tenant=globex succeeded=1 failed=0 cancelledJobs=0 cancelled=false
```

The Hatchet part (`hatchet.go`) dispatches 50 jobs through the batch coordinator with `BatchSource` as its completion source. Against the shared local stack's engine (gRPC on port 7004, no TLS):

```sh
HATCHET_CLIENT_TOKEN=... HATCHET_CLIENT_HOST_PORT=localhost:7004 HATCHET_CLIENT_TLS_STRATEGY=none go run ./batches
```

```text
hatchet: coordinated=true, one parent run=true
hatchet: finally succeeded=50 of 50
hatchet: coordinated batch without a LabelSource refused: true
```

`go test ./batches` skips the Hatchet test, with the reason, when the token is not set.

## Pitfalls

- **Manual retry means you must retry.** With `WithManualCallbackRetry` and no poller, a callback that fails after the batch's last job end is never retried unless the app calls `RetryCallbacks`.
- **A retry waits for the claim.** `RetryCallbacks` skips a pending callback until its `RetryAt`; calling it right after a failure does nothing.
- **The poller's cost grows with unfinished jobs.** A 10,000-job batch at the default interval is up to 2,000 `Status` calls a second; on Hatchet use `BatchSource`, elsewhere raise the interval.
- **Two sources are fine, a missing one is not.** Repeats are ignored, so adding a poller beside the hook is always safe; a `WithCompletionSource` that names no source able to see the jobs leaves batches open forever.
- **`Status` errors are not guesses.** An engine that pruned a run returns `workflow.ErrNotFound` for it; the poller logs and retries it every tick, so retention on the engine must outlast your batches.

## Related

- [Batches](batches.md): dispatch, progress, cancel.
- [Native group runs](native-groups.md): the coordinator that needs a `LabelSource`.
- [React when a run finishes](../observability/run-finished.md): the hook behind `HookSource`.
- [Durability](durability.md): `ResumeDispatches`, which the poller also calls.
- Example: [examples/batches](../../examples/batches/).
