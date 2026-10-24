# Lifecycle events

The run-finished hook says when a run ended. A live view wants more: a dashboard row that turns from queued to running, a progress bar that moves, an audit trail that shows the first attempt failed and the second one ran. `workflow.LifecycleNotifier` streams those moments as they happen: a run inserted, an attempt started, an attempt that will be retried, a snooze, a progress report, and the end. It is a live feed, best effort and local to the process, not a log; the [run-finished hook](run-finished.md) stays the channel that guarantees every end arrives.

## Before you start

- A client that implements `workflow.LifecycleNotifier`. The memory and Redis drivers do, and so do the wrappers that forward it (`workflow.Forward`, so `middleware.WithCodec` and plugin clients; `failover`, which registers on both backends and prefixes the refs; `hybrid`, which passes it to the jobs client). The Hatchet driver does not. The SQL driver does too, for the runs and attempts its own client handles. There is no capability bit, and a wrapper has the method whatever it wraps, so before offering a live view ask `workflow.Supports(c, workflow.OpLifecycle)`: a plain client answers by its interface, a wrapper (`workflow.Forward`, `failover`) for the client underneath.
- Observers are registered before `Start`.
- The example is [`examples/lifecycle-events`](../../examples/lifecycle-events). It runs on the memory driver with a fake clock, so the times it prints never vary. Run it with `go run ./lifecycle-events` from `examples/`.

## Watch the lifecycle feed

Discover the interface with a type assertion and register an observer. It runs on a driver goroutine, so it hands each event on (to a channel, a websocket hub, a buffered writer) and returns:

<!-- snippet: examples/lifecycle-events/main.go#observe -->
```go
events := make(chan workflow.LifecycleEvent, 32)
ln, ok := workflow.Client(c).(workflow.LifecycleNotifier)
if !ok {
	return errors.New("this driver has no lifecycle feed")
}
// Register before Start. The observer runs on a driver goroutine:
// return quickly, hand anything slow to a channel.
if err := ln.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) {
	events <- ev
}); err != nil {
	return err
}
```

`OnLifecycle` after `Start` returns `workflow.ErrAlreadyStarted`, and a nil function is refused with `workflow.ErrInvalid` on the memory and Redis drivers. Each observer sees every event, in registration order.

## The event kinds

`LifecycleEvent.Kind` is one of six values. Code that switches on it keeps a `default` branch, since a later version may add kinds.

| Kind | When | Fields set |
|---|---|---|
| `LifecycleInserted` (`inserted`) | `Enqueue` accepted a new run; a duplicate returned for an idempotency key is not reported | `Status` (queued, or scheduled for a delayed run) |
| `LifecycleStarted` (`started`) | A step attempt started | `Step`, `Attempt` |
| `LifecycleRetrying` (`retrying`) | An attempt failed and the step will run again after its backoff | `Step`, `Attempt` (the failed one), `Err` |
| `LifecycleSnoozed` (`snoozed`) | A step called `workflow.Snooze` and runs again later, without spending an attempt | `Step`, `Attempt`, `Err` |
| `LifecycleProgress` (`progress`) | A step called `workflow.ReportProgress` | `Step`, `Attempt`, `Progress` |
| `LifecycleFinished` (`finished`) | The run reached a terminal status, the same moment the run-finished hook reports | `Status`, `Err` for a failure |

Every event also carries `Ref`, `Workflow`, `Queue`, `Tenant`, the run's `Labels` (read-only), `Node` (the node it happened on, on a driver with `CapNodeIdentity`, see [Nodes and health](../operations/nodes.md)) and `At`, on the driver's clock. `Status` is the run's status after the event.

## Try it

```sh
cd examples
go run ./lifecycle-events
```

```text
-- lifecycle
  09:00:00 inserted  export status=queued node=worker-1
  09:00:00 started   export step=rows attempt=1 status=running node=worker-1
  09:00:00 retrying  export step=rows attempt=1 status=retrying node=worker-1 err=database busy
  09:00:00 started   export step=rows attempt=2 status=running node=worker-1
  09:00:00 progress  export step=rows attempt=2 status=running node=worker-1 progress=50% "500 of 1000 rows"
  09:00:00 finished  export status=succeeded node=worker-1
  09:00:00 inserted  charge status=queued node=worker-1
  09:00:00 started   charge step=card attempt=1 status=running node=worker-1
  09:00:00 finished  charge status=failed node=worker-1 err=memory: step "card" failed after 1 attempt(s): card declined
```

The export's first attempt fails and is retried at once (its policy has no backoff), the second reports progress and succeeds. The charge fails for good; its `Err` is the run's `*workflow.RunError`, described in [Errors › Run errors](../concepts/errors.md#run-errors). `go test ./lifecycle-events` checks every line.

## Lifecycle events or the run-finished hook

| | `LifecycleNotifier` | `RunNotifier` (`OnRunFinished`) |
|---|---|---|
| What it reports | Insert, attempt start, retry, snooze, progress, end | The end only |
| Delivery | Best effort, at most once | At least once on a driver with `CapRunFinished`, including ends that happened while no client ran |
| Where | The process where it happened: each node of a cluster streams its own share | One client per end on a durable driver |
| Use it for | A live screen, a progress bar, a per-node trace | Releasing a lock, counting a batch, an audit row, an alert |

Anything that must not miss an end (a unique lock, a batch's progress, a billing row) belongs on the run-finished hook. The lifecycle feed is for showing what happens now.

## Pitfalls

- **It is local.** An observer sees the enqueues this process accepted and the attempts its workers ran. On a cluster, a dashboard that wants every node's events collects them from every node, or reads run state from the driver instead.
- **It can drop events.** The Redis driver queues at most 4,096 events for its observers; past that it drops new ones and logs a warning, so a slow observer loses events rather than slowing workers. Keep observers short.
- **It is not ordered across runs.** For one run, the events of one process arrive in the order they happened; events of different runs interleave.
- **A panic is recovered and logged.** It never changes a run, and the observer is called again for the next event.
- **Labels are shared.** `LifecycleEvent.Labels` must not be modified; copy it first.
- **`Step` on a finished event.** The finished event names no attempt (`Attempt` is zero); the memory driver still fills `Step` with the step that ended the run, so read it as a hint, not as an attempt.

## Related

- [React when a run finishes](run-finished.md): the guaranteed channel for run ends.
- [Stuck jobs](stuck-jobs.md): report an attempt that runs far too long.
- [Nodes and health](../operations/nodes.md): what `Node` names.
- [Errors › Run errors](../concepts/errors.md#run-errors): the error a failed run carries.
- Example: [examples/lifecycle-events](../../examples/lifecycle-events/).
