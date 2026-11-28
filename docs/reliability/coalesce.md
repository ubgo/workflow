# Coalescing runs

An idempotency key held by a run that is executing drops a new enqueue with the same key: the enqueue returns the running run. For coalescing work (a cache rebuild, a search reindex, a sync) that loses work, because changes that arrive while the job runs need one more run afterwards. `Workflow.Coalesce` keeps one run pending beside the running one for each key.

## Before you start

- A driver with `workflow.CapCoalesce` (and `CapIdempotencyKey`): memory and sqlstore (migration `014_checkpoints_coalesce_schedule_idempotency`, table `<prefix>run_next`). Redis and Hatchet refuse the workflow at `Start` (`CheckCaps`).
- The example is [`examples/coalesce`](../../examples/coalesce). Run it with `go run ./coalesce` from `examples/`.

## Turn it on

<!-- snippet: examples/coalesce/main.go#workflow -->
```go
if err := c.Register(workflow.Workflow{
	Name:     "reindex",
	Steps:    []workflow.Step{{Name: "rebuild", Run: reindex}},
	Coalesce: &workflow.Coalesce{Payload: workflow.CoalesceKeepLast},
}); err != nil {
	return err
}
```

## What an enqueue does

| The key | The enqueue |
|---|---|
| Free | Starts a run that holds the key, as without `Coalesce` |
| Held by a run still in the queue (scheduled, or not yet admitted) | Folds into that run and returns its `RunRef` (outcome: duplicate) |
| Held by a run that left the queue (admitted: running, waiting, retrying), nothing pending | Stores a new run pending behind it and returns its `RunRef` (outcome: inserted). The pending run reports `StatusScheduled` and starts only once the holder ends, whatever its end: succeeded, failed, cancelled or discarded. Then it holds the key |
| Held, with a run already pending | Folds into the pending run and returns its `RunRef` |

So a key has at most one run executing and one waiting:

<!-- snippet: examples/coalesce/main.go#enqueue -->
```go
job := func(version string) workflow.Job {
	return workflow.Job{Workflow: "reindex", IdempotencyKey: "reindex:catalog", Payload: []byte(version)}
}
first, err := c.Enqueue(ctx, job("v1"))
if err != nil {
	return err
}
<-started // v1 is running
var pending workflow.RunRef
for _, v := range []string{"v2", "v3", "v4"} { // changes arriving meanwhile
	if pending, err = c.Enqueue(ctx, job(v)); err != nil {
		return err
	}
}
st, err := c.Status(ctx, pending)
if err != nil {
	return err
}
fmt.Println("while v1 runs, the pending run is:", st)
```

## Which payload the pending run keeps

| `Coalesce.Payload` | Effect |
|---|---|
| `CoalesceKeepFirst` (the default, also empty) | The payload of the enqueue that stored the run; later payloads are dropped |
| `CoalesceKeepLast` | Each later enqueue replaces the payload and priority while the run is still in the queue, so the run that starts carries the newest request |

## Pitfalls

- A job's `RunAt` on the pending run is kept: it starts at the later of `RunAt` and the holder's end. Editing the pending run's start (`RunEditor`, `Reschedule`) can start it before the holder ends.
- `Coalesce` with `IdempotencyOff` is refused by `Validate`: it works on keys.
- On sqlstore, `EnqueueTx` coalesces inside the caller's transaction; `EnqueueMany` refuses a keyed job of a coalescing workflow with an `UnsupportedError` naming `Workflow.Coalesce`. A chained job (`Job.Chain`) is enqueued with plain key semantics.
- On sqlstore the pending run's `RunInfo.RunAt` reads as the far future until the holder ends.
- This is not a concurrency limit: runs of other keys run as usual. Use [concurrency keys](../jobs/concurrency.md) to cap runs across keys.
- It combines with them. Once its holder ends, the pending run is admitted like any other run: it waits for room on its concurrency key, on every `ConcurrencyLimits` key and under its rate limits, and runs that were already waiting for those keys go first. Under `CoalesceKeepLast` its keys are evaluated from the payload it will run with, not from the payload of the enqueue that stored it.
- A holder parked under [`HoldWhileRunning`](../jobs/concurrency.md#choose-how-long-a-run-holds-its-slot) has left the queue, so an enqueue behind it is kept pending, not folded into the holder.

## Related

- [Idempotency keys](../jobs/idempotency.md)
- [Unique jobs](unique-jobs.md): uniqueness above the driver, with holder states and replace.
- [Debounce](debounce.md): wait for a burst to end before one run.
