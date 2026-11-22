# React when a run finishes

The run-finished hook tells app-side code that a run reached a terminal status (succeeded, failed or cancelled, or discarded on a driver with `CapRunStates`, as [`RunEvent.Status`](ds:block?id=runevent-upvhdx59) says) without polling `Status`: release a uniqueness lock, count a batch's finished jobs, write an audit row, notify a user. It is an optional interface a driver's client implements, and a capability says whether every end is guaranteed to arrive.

## Before you start

- A driver: the [memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j) and [Redis](ds:block?id=redis-caps-bfgdst9t) drivers implement the hook and declare [`workflow.CapRunFinished`](ds:block?id=caprunfinished-7nrsun98); the [Hatchet driver](ds:block?id=hatchet-client-onrunfinished-ymfhyxkr) implements it best effort, without the capability ([its list](ds:block?id=hatchet-caps-d6x9fv8z)).
- Register observers before `Start`.
- The runnable program for this page is [examples/run-finished](../../examples/run-finished/).

## React when a run finishes

[`workflow.RunNotifier`](ds:block?id=runnotifier-tbkv69uz) has one method, `OnRunFinished(fn workflow.RunFinishedFunc) error`. Discover it with a type assertion on the client, so a driver that cannot report run ends is never forced to pretend it can, and code falls back (to a TTL, to polling `Status`) when the assertion fails. `Capabilities().Has(workflow.CapRunFinished)` then says whether delivery is guaranteed ([`CapRunFinished`](ds:block?id=caprunfinished-7nrsun98)):

<!-- snippet: examples/run-finished/main.go#observe -->
```go
n, ok := c.(workflow.RunNotifier)
if !ok {
	return errors.New("this driver cannot report run ends; poll Status instead")
}
guaranteed := c.Capabilities().Has(workflow.CapRunFinished)
fmt.Println("every run end is delivered:", guaranteed)

ends := make(chan workflow.RunEvent, 8)
err := n.OnRunFinished(func(ctx context.Context, ev workflow.RunEvent) {
	// Runs on a driver goroutine, at least once per run: keep it
	// short and idempotent, and enqueue anything long.
	ends <- ev
})
if err != nil {
	return err
}
```

A [`workflow.RunEvent`](ds:block?id=runevent-upvhdx59) carries the run's `Ref` (with the `ScheduledID` too when the run was enqueued with `RunAt`, so a caller that stored the ref `Enqueue` returned can match it), the `Workflow` name, the terminal `Status`, `Err` (nil on success; a cancelled run's `Err` matches `workflow.ErrCancelled` on every driver), the run's `Labels` (read-only) and `FinishedAt` from the driver's clock.

The contract every implementation keeps ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)):

- Register before `Start`. [`OnRunFinished`](ds:block?id=runnotifier-tbkv69uz) after `Start` returns `workflow.ErrAlreadyStarted`. Several observers may be registered; each sees every event, in registration order.
- An observer runs on a driver goroutine ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)), never on the goroutine that enqueued or cancelled the run, and may run concurrently for different runs. It should return quickly; long work belongs in a new `Enqueue`.
- It is called after `Status` reports the terminal status ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)), so an observer that calls `Status` sees the same status.
- Its context is cancelled when the client shuts down (the `Start` context), and a panic in an observer ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)) is recovered by the driver and never changes the run's status.
- Delivery is at least once ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)): an observer may see one run's event more than once and must tolerate it.
- With [`CapRunFinished`](ds:block?id=caprunfinished-7nrsun98), every run that reaches a terminal status is delivered, including cancelled runs and runs that never started, and a durable driver (the SQL driver) also delivers, after a restart, the ends that happened while no client was running. Without it, an event may be lost, so a consumer keeps a fallback.

<!-- snippet: examples/run-finished/main.go#late -->
```go
err = n.OnRunFinished(func(context.Context, workflow.RunEvent) {})
fmt.Println("observer after Start refused:", errors.Is(err, workflow.ErrAlreadyStarted))
```

The client wrappers in package `middleware` (`WrapClient`, `WithCodec`) forward the hook to the driver, and return an `UnsupportedError` naming `CapRunFinished` when the wrapped client has none, rather than a silent no-op ([`forwardRunFinished`](ds:block?id=mw-forwardrunfinished-e6rtfsuk)).

## On Hatchet

The Hatchet driver implements [`RunNotifier`](ds:block?id=hatchet-client-onrunfinished-ymfhyxkr) by polling the engine's run list and does not declare `CapRunFinished` ([its list](ds:block?id=hatchet-caps-d6x9fv8z)): an end can be lost, for example when the process crashes between the engine recording it and the poll that would deliver it. Three [`hatchet.Config`](ds:block?id=hatchet-config-wdky3m9f) fields tune it:

- `RunFinishedPoll`: how often the engine is asked for ended runs (default [`hatchet.DefaultRunFinishedPoll`](ds:block?id=hatchet-defaultrunfinishedpoll-47nap6m3), one second).
- `RunFinishedWindow`: the oldest an unfinished run may be and still be watched for its end (default [`hatchet.DefaultRunFinishedWindow`](ds:block?id=hatchet-defaultrunfinishedwindow-eees2d36), 24 hours).
- [`RunFinishedCatchUp`](ds:block?id=hatchet-config-wdky3m9f): makes `Start` replay the ends of runs created this long before it, so a restarted process delivers ends it missed while down (again, if an earlier process already delivered them). Zero replays nothing older than `Start`.

<!-- snippet: examples/run-finished/hatchet.go#hatchet -->
```go
c := hatchet.New(hatchet.Config{
	WorkerName:         "run-finished-example",
	RunFinishedPoll:    2 * time.Second,  // how often the engine is asked for ended runs
	RunFinishedWindow:  6 * time.Hour,    // runs older than this are no longer watched
	RunFinishedCatchUp: 10 * time.Minute, // replay the ends of runs created this long before Start
})
fmt.Println("hatchet: every run end is delivered:", c.Capabilities().Has(workflow.CapRunFinished))
ends := make(chan workflow.RunEvent, 1)
if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
	return err
}
```

Code built on the hook keeps a fallback there: [`UniqueUntilFinished`](ds:block?id=mw-uniqueuntilfinished-kgv65kyf) keeps its TTL, and a batch adds a status poller or the Hatchet `BatchSource` ([how a batch learns a job ended](../batches/completion.md#how-a-batch-learns-a-job-ended)). On an engine, [`Err`](ds:block?id=runevent-upvhdx59) is rebuilt from the message the engine stored, so `errors.Is` against an app's own sentinel is reliable only on in-process drivers.

## Try it

```sh
cd examples
go run ./run-finished
```

```text
every run end is delivered: true
observer after Start refused: true
charge succeeded order=o-1 err=<nil> cancelled=false
charge failed order=o-2 err=memory: step "run" failed after 1 attempt(s): card declined cancelled=false
charge cancelled order=o-3 err=workflow: run cancelled cancelled=true
```

The third run was enqueued with `RunAt` an hour ahead and cancelled before it fired; it never started, and the memory driver still reports its end.

The Hatchet part runs when `HATCHET_CLIENT_TOKEN` is set; against the shared local stack's engine (gRPC on port 7004, no TLS) it adds:

```sh
HATCHET_CLIENT_TOKEN=... HATCHET_CLIENT_HOST_PORT=localhost:7004 HATCHET_CLIENT_TLS_STRATEGY=none go run ./run-finished
```

```text
hatchet: every run end is delivered: false
hatchet: run succeeded
```

`go test ./run-finished` checks the memory output and skips the Hatchet test, with the reason, when the token is not set.

## Pitfalls

- **Register before `Start`.** A hook added later is refused with `ErrAlreadyStarted` ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)), including the ones [`UniqueUntilFinished`](ds:block?id=mw-uniqueuntilfinished-kgv65kyf) and [`batch.Manager.Attach`](ds:block?id=batch-manager-attach-xnchrce9) register for you.
- **Observers must be idempotent.** At-least-once delivery ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)) means a second event for the same run is normal, not a bug; key side effects by `ev.Ref.RunID`.
- **Do not block the driver.** An observer runs on a driver goroutine ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)); a slow one delays the delivery of other ends. Enqueue a job for anything slow.
- **Without `CapRunFinished`, plan for loss.** On Hatchet, an end that happened while no process was polling is delivered only if [`RunFinishedCatchUp`](ds:block?id=hatchet-config-wdky3m9f) reaches back far enough, and a crash can still lose one. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **Labels are shared, not copied.** [`ev.Labels`](ds:block?id=runevent-upvhdx59) is the run's own map; observers must not modify it.

## Related

- [Unique until the run finishes](../reliability/unique-jobs.md#unique-until-the-run-finishes): a lock released by the hook.
- [How a batch learns a job ended](../batches/completion.md#how-a-batch-learns-a-job-ended): the hook as a batch's completion source.
- [Capabilities](../concepts/capabilities.md): `CapRunFinished` and the others.
- [Hatchet driver](../drivers/hatchet.md): the rest of `hatchet.Config`.
- Example: [examples/run-finished](../../examples/run-finished/).
