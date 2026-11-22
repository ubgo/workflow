# Lifecycle events

The run-finished hook says when a run ended. A live view wants more: a dashboard row that turns from queued to running, a progress bar that moves, an audit trail that shows the first attempt failed and the second one ran. [`workflow.LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9) streams those moments as they happen: a run inserted, an attempt started, an attempt that will be retried, a snooze, a progress report, and the end. It is a live feed, best effort and local to the process, not a log; the [run-finished hook](run-finished.md) stays the channel that guarantees every end arrives.

## Before you start

- A client that implements `workflow.LifecycleNotifier`. The [memory](ds:block?id=memory-client-onlifecycle-wspq9c6m) and [Redis](ds:block?id=redis-client-onlifecycle-pkz638m3) drivers do, and so do the wrappers that forward it ([`workflow.Forward`](ds:block?id=forward-onlifecycle-h3z2xmg2), so `middleware.WithCodec` and plugin clients; [`failover`](ds:block?id=failover-client-onlifecycle-wubrqrvt), which registers on both backends and prefixes the refs; [`hybrid`](ds:block?id=hybrid-client-spwwj95u), which passes it to the jobs client). The Hatchet driver does not. The [SQL driver](ds:block?id=sql-client-onlifecycle-yrrp27ek) does too, for the runs and attempts its own client handles. There is no capability bit, and a wrapper has the method whatever it wraps, so before offering a live view ask `workflow.Supports(c, workflow.OpLifecycle)` ([`Supports`](ds:block?id=supports-4xyjjwvz), [`OpLifecycle`](ds:block?id=oplifecycle-mcqawhzd)): a plain client answers by its interface, a wrapper (`workflow.Forward`, `failover`) for the client underneath. Hatchet does not: the engine has no insert or start callback; see [Hatchet limitations](../drivers/hatchet.md#limitations).
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

`OnLifecycle` after `Start` returns `workflow.ErrAlreadyStarted`, and a nil function is refused with `workflow.ErrInvalid` on the [memory](ds:block?id=memory-client-onlifecycle-wspq9c6m), [SQL](ds:block?id=sql-client-onlifecycle-yrrp27ek) and [Redis](ds:block?id=redis-client-onlifecycle-pkz638m3) drivers. Each observer sees every event, in registration order ([`LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9)).

## The event kinds

`LifecycleEvent.Kind` is one of six values. Code that switches on it keeps a `default` branch, since a later version may add kinds ([`LifecycleKind`](ds:block?id=lifecyclekind-fnvwsyy7)).

| Kind | When | Fields set |
|---|---|---|
| [`LifecycleInserted`](ds:block?id=lifecycleinserted-74ak7wrv) (`inserted`) | `Enqueue` accepted a new run; a duplicate returned for an idempotency key is not reported | `Status` (queued, or scheduled for a delayed run) |
| [`LifecycleStarted`](ds:block?id=lifecyclestarted-yz56258m) (`started`) | A step attempt started | `Step`, `Attempt` |
| [`LifecycleRetrying`](ds:block?id=lifecycleretrying-y3jsmjmh) (`retrying`) | An attempt failed and the step will run again after its backoff | `Step`, `Attempt` (the failed one), `Err` |
| [`LifecycleSnoozed`](ds:block?id=lifecyclesnoozed-k96yqmmm) (`snoozed`) | A step called `workflow.Snooze` and runs again later, without spending an attempt | `Step`, `Attempt`, `Err` |
| [`LifecycleProgress`](ds:block?id=lifecycleprogress-yvqxcvp5) (`progress`) | A step called `workflow.ReportProgress` | `Step`, `Attempt`, `Progress` |
| [`LifecycleFinished`](ds:block?id=lifecyclefinished-3p3kttag) (`finished`) | The run reached a terminal status, the same moment the run-finished hook reports | `Status`, `Err` for a failure |

Every event also carries ([`LifecycleEvent`](ds:block?id=lifecycleevent-369x8exj)) `Ref`, `Workflow`, `Queue`, `Tenant`, the run's `Labels` (read-only), `Node` (the node it happened on, on a driver with `CapNodeIdentity`, see [Nodes and health](../operations/nodes.md)) and `At`, on the driver's clock. `Status` is the run's status after the event.

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
| Delivery | Best effort, at most once ([`LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9)) | At least once on a driver with [`CapRunFinished`](ds:block?id=caprunfinished-7nrsun98), including ends that happened while no client ran ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)) |
| Where | The process where it happened: each node of a cluster streams its own share | One client per end on a durable driver |
| Use it for | A live screen, a progress bar, a per-node trace | Releasing a lock, counting a batch, an audit row, an alert |

Anything that must not miss an end (a unique lock, a batch's progress, a billing row) belongs on the run-finished hook. The lifecycle feed is for showing what happens now.

## Pitfalls

- **It is local.** An observer sees the enqueues this process accepted and the attempts its workers ran ([`LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9)). On a cluster, a dashboard that wants every node's events collects them from every node, or reads run state from the driver instead.
- **It can drop events.** The Redis driver queues at most 4,096 events for its observers ([`lifecycleBuffer`](ds:block?id=redis-lifecyclebuffer-r6nt77tq)); past that it [drops new ones and logs a warning](ds:block?id=redis-client-emit-dh9vkbpx), so a slow observer loses events rather than slowing workers. Keep observers short.
- **It is not ordered across runs.** For one run, the events of one process arrive in the order they happened ([`LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9)); events of different runs interleave.
- **A panic is recovered and logged** ([`LifecycleNotifier`](ds:block?id=lifecyclenotifier-3vtxxku9)). It never changes a run, and the observer is called again for the next event.
- **Labels are shared.** [`LifecycleEvent.Labels`](ds:block?id=lifecycleevent-369x8exj) must not be modified; copy it first.
- **`Step` on a finished event.** The finished event names no attempt (`Attempt` is zero); the [memory driver](ds:block?id=memory-client-queueendedlocked-w8yb438q) still fills `Step` with the step that ended the run, while the [SQL driver](ds:block?id=sql-client-emitfinished-xaqvpkwt) and the [Redis driver](ds:block?id=redis-client-finish-3awqe53u) leave it empty, so read it as a hint, not as an attempt; [`RunEvent.Step`](ds:block?id=runevent-upvhdx59) from the run-finished hook is the field to rely on.

## Related

- [React when a run finishes](run-finished.md): the guaranteed channel for run ends.
- [Stuck jobs](stuck-jobs.md): report an attempt that runs far too long.
- [Nodes and health](../operations/nodes.md): what `Node` names.
- [Errors › Run errors](../concepts/errors.md#run-errors): the error a failed run carries.
- Example: [examples/lifecycle-events](../../examples/lifecycle-events/).
