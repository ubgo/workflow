# Cancel and status

Once a job is enqueued you hold a `workflow.RunRef`. With it you can ask where the run is (an admin page, a "your export is ready" poll, a test) and stop it (a user cancels an export, an order is refunded before it ships). Both calls take the same `RunRef` `Enqueue` returned, whether the run has started yet or not.

## Before you start

- Cancelling needs `workflow.CapCancel`; the memory, SQL, Redis and Hatchet drivers all have it.
- The example is [`examples/delayed-jobs`](../../examples/delayed-jobs), on the memory driver with a fake clock. Run it with `go run ./delayed-jobs` from `examples/`.

## Cancel a run

`Client.Cancel` stops a delayed start before it fires, a queued run before it starts, or a running run. A running step sees its `ctx` cancelled, so a long step should watch `ctx.Done()`:

<!-- snippet: examples/delayed-jobs/main.go#cancel-running -->
```go
exp, err := c.Enqueue(ctx, workflow.Job{Workflow: "export"})
if err != nil {
	return err
}
<-running // the step is executing
if err := c.Cancel(ctx, exp); err != nil {
	return err
}
if st, err = c.Status(ctx, exp); err != nil {
	return err
}
fmt.Fprintln(out, "cancelled while running:", st)
```

Cancel is idempotent: cancelling a run that has finished or was already cancelled returns nil, so you can call it from a retried request without checking first. A cancelled run is never retried. The `Err` of its run-finished event matches `workflow.ErrCancelled` under `errors.Is` on every driver, which is what the example's `run-finished: cancelled, cancelled true` line checks.

## Read a run's status

`Client.Status` returns a `workflow.Status`:

| Status | Meaning |
|---|---|
| `StatusScheduled` | A delayed start that has not begun |
| `StatusQueued` | Waiting for a worker (or a concurrency slot, a rate limit; a retry's backoff on a driver without `CapRunStates`) |
| `StatusRunning` | A step is executing |
| `StatusRetrying` | A step failed and waits on its retry backoff; `RunInfo.NextAttempt` and `NextAttemptAt` say which attempt comes and when. Only with `CapRunStates` |
| `StatusWaiting` | Suspended on a wait or sleep step, holding no worker |
| `StatusSucceeded` | Finished; every step succeeded |
| `StatusFailed` | Finished; a step failed for good |
| `StatusCancelled` | Finished; cancelled, or dropped by a concurrency strategy (and a `Discard`ed run on a driver without `CapRunStates`) |
| `StatusDiscarded` | Finished; a step dropped the run with `workflow.Discard`. Only with `CapRunStates` |

`IsTerminal` is true for the last four: the run will not change again, and its idempotency key is free. [Run states](run-states.md) covers the retrying and discarded states, which drivers report them, and the [whole state machine](run-states.md#the-state-machine).

<!-- snippet: examples/delayed-jobs/main.go#status -->
```go
st, err = c.Status(ctx, ref) // the RunRef Enqueue returned still works
if err != nil {
	return err
}
fmt.Fprintf(out, "after it ran: %s, terminal %t\n", st, st.IsTerminal())
```

An unknown reference returns an error matching `workflow.ErrNotFound`:

<!-- snippet: examples/delayed-jobs/main.go#not-found -->
```go
_, err = c.Status(ctx, workflow.RunRef{RunID: "run-404"})
fmt.Fprintln(out, "unknown run:", errors.Is(err, workflow.ErrNotFound))
```

`RunID` is the run's identity once it has started. Treat it as opaque: the memory driver counts (`run-1`), the SQL driver uses a generated ID, Hatchet its own. Store the whole `RunRef` rather than the `RunID` alone, because a delayed run has no `RunID` until it fires.

## Try it

```sh
cd examples
go run ./delayed-jobs
```

```text
enqueued: scheduled id set true, run id set false, status scheduled
cancelled before it started: cancelled
run-finished: cancelled, cancelled true
reminder sent to ada@example.com at Tue 09:00
after it ran: succeeded, terminal true
past RunAt: run id set true
reminder sent to bob@example.com at Tue 09:00
cancelled while running: cancelled
run-finished: cancelled, cancelled true
unknown run: true
```

## Pitfalls

- **Polling Status is the slow way to learn that a run ended.** To react to the end of a run (notify a user, count a batch), register an observer with `OnRunFinished` on a driver that implements `workflow.RunNotifier`, as the example does ([run-finished](../observability/run-finished.md)).
- **A step that ignores its ctx keeps running after Cancel.** The run is cancelled at once and its slot is freed (on the memory driver, even while such a handler is still returning), but the Go code in the handler stops only when it returns. Pass `ctx` to every call that takes one.
- **On the SQL driver, Cancel reaches a step running on another process at that process's next lease renewal**, not instantly; the run's status is cancelled at once.
- **Hatchet's read side trails its writes.** A run is missing from Hatchet's REST API for about a second after it is created, so the Hatchet driver retries a not-found `Status` for up to `Config.VisibilityLag` (5 seconds by default) before returning `ErrNotFound`.
- **A run dropped by a concurrency strategy is cancelled, not an error.** Under `ConcurrencyDropNewest` and `ConcurrencyCancelNewest`, `Enqueue` returns a `RunRef` and nil, and `Status` reports `StatusCancelled` ([strategies](concurrency.md#strategies)).

## Related

- [Run a job later](delays.md)
- [React when a run finishes](../observability/run-finished.md)
- [Errors](../concepts/errors.md)
- Example: [`examples/delayed-jobs`](../../examples/delayed-jobs)
