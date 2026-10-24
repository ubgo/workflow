# Timeouts

A job that never ends is worse than one that fails: it holds a worker, hides the problem and leaves the user waiting. The library has four deadlines, each catching a different way a run can hang, and one error family that tells them apart: a step attempt that runs too long, a wait whose event never comes, an attempt that stops sending heartbeats, and a whole run that takes too long. `Workflow.OnTimeout` lets a run that missed a deadline be handled differently from one that failed.

## Before you start

- `Step.Timeout`, `Workflow.Timeout` and wait deadlines work on every driver. `Workflow.RunTimeout` and `Workflow.OnTimeout` need `workflow.CapRunTimeout`, and `Step.HeartbeatTimeout` needs `workflow.CapHeartbeat`; a driver without them refuses a workflow that sets them. The memory, Redis and SQL drivers have both; Hatchet has neither.
- On a driver with `CapRunTimeout` every timeout error is built with `workflow.TimeoutError`, so it matches `workflow.ErrTimeout` and the specific sentinel below. Check `c.Capabilities()` before relying on the sentinels on another driver.
- The example is [`examples/snooze-heartbeat`](../../examples/snooze-heartbeat). It runs on the memory driver with a fake clock, so five minutes pass at once. Run it with `go run ./snooze-heartbeat` from `examples/`.

## The four deadlines

| Deadline | Set with | Catches | Error | Retried |
|---|---|---|---|---|
| Step attempt | `Step.Timeout`, default `Workflow.Timeout` | One attempt running too long | `workflow.ErrStepTimeout` | Yes, by the step's retry policy |
| Wait | `WaitSpec.Timeout` or `WaitSpec.Deadline` | An event that never arrives | `workflow.ErrWaitTimeout` | No: the run fails |
| Heartbeat | `Step.HeartbeatTimeout` | An attempt that stopped beating (hung) | `workflow.ErrHeartbeatTimeout` | Yes, by the step's retry policy |
| Run | `Workflow.RunTimeout` | The whole run taking too long | `workflow.ErrRunTimeout` | Never |

Each specific sentinel also matches `workflow.ErrTimeout`. On the memory and Redis drivers a step attempt's timeout still matches `context.DeadlineExceeded` too, as it did before the family existed. Step timeouts and waits are covered in [Workflows and steps](../workflows/steps.md#timeouts) and [Signals](../workflows/signals.md); heartbeats in [Progress and heartbeats](progress.md#catch-a-hung-step).

## Cap a whole run

`Workflow.RunTimeout` caps a run's whole life, from when it is admitted to start (for a delayed run, from when it is due) to its end, with waits, sleeps, retries and backoffs included. A step timeout cannot do that: a step that times out and retries five times, or a run with ten slow steps, can take far longer than any one attempt's limit. When the run goes past it, attempts still running see their `ctx` cancelled with the cause `workflow.ErrRunTimeout`, and the run fails with an error matching `ErrRunTimeout` and `ErrTimeout`. It is never retried: the time is spent. Zero means no limit, and a negative value fails validation.

## Handle a timeout differently from a failure

`Workflow.OnTimeout` runs once, in place of `OnFailure`, when a run fails for good because of any timeout: a step attempt past its timeout on its last attempt, a wait past its deadline, a heartbeat timeout, or `RunTimeout`. It gets the failing step's input, or the run's payload for a `RunTimeout` that struck outside any step. Nil means `OnFailure` runs for timeouts too. A timeout is often a different story for the user ("your report is late, we will email it") than a failure ("your report could not be built"):

<!-- snippet: examples/snooze-heartbeat/main.go#run-timeout -->
```go
wf := workflow.Workflow{
	Name:       "build-report",
	RunTimeout: 5 * time.Minute, // the whole run, waits and retries included
	Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		started <- struct{}{}
		<-ctx.Done() // the cause is ErrRunTimeout
		return nil, context.Cause(ctx)
	}}},
	OnTimeout: func(_ context.Context, in []byte) ([]byte, error) {
		note("OnTimeout with payload " + string(in)) // tell the user the report is late
		return nil, nil
	},
	OnFailure: func(context.Context, []byte) ([]byte, error) {
		note("OnFailure") // any other failure
		return nil, nil
	},
}
```

```text
run timeout:
  run failed: workflow: run timed out: after 5m0s
  ErrRunTimeout true, IsTimeout true, ErrStepTimeout false
  ran OnTimeout with payload "q3"
```

`OnFailure` did not run. `workflow.FailureHandler(wf, err)` is the rule every driver applies to choose between them (`OnTimeout` for a timeout when it is set, else `OnFailure`), for a custom driver that must decide the same way.

## Tell timeouts apart

`workflow.IsTimeout(err)` reports any missed deadline; `errors.Is` with a specific sentinel reports which one. They work on a `RunEvent.Err` from the run-finished hook and through any wrapping:

<!-- snippet: examples/snooze-heartbeat/main.go#timeout-errors -->
```go
runOut := errors.Is(ev.Err, workflow.ErrRunTimeout) // this deadline in particular
anyDeadline := workflow.IsTimeout(ev.Err)           // any of the four
```

A failed run's error is a `*workflow.RunError` naming the run, workflow, step and attempt; it wraps the timeout, so the checks above see through it. `RunInfo.Error` is only the message, a string, so a `Runs` listing cannot be matched with `errors.Is`: use the run-finished hook when code must react to the kind of timeout.

## Try it

```sh
cd examples
go run ./snooze-heartbeat
```

```text
snooze:
  try at +0s: attempt 1
  snoozed, status waiting
  try at +1m0s: attempt 1
  snoozed, status waiting
  try at +2m0s: attempt 1
  run succeeded, attempts 1, errors recorded 0
heartbeat:
  attempt 1 started
  attempt 2 started at +31s
  attempt 1 failed at +30s: workflow: heartbeat timed out: memory: step "load" attempt 1 went 30s without a heartbeat
  run succeeded
run timeout:
  run failed: workflow: run timed out: after 5m0s
  ErrRunTimeout true, IsTimeout true, ErrStepTimeout false
  ran OnTimeout with payload "q3"
```

The snooze lines belong to [Retries › Snooze](retries.md#snooze-a-step-that-is-not-ready) and the heartbeat lines to [Progress and heartbeats](progress.md). `go test ./snooze-heartbeat` checks every line.

## Pitfalls

- **A timeout only stops a step that watches its `ctx`.** Go cannot kill a goroutine: the run is recorded as failed and `OnTimeout` runs, but a handler that ignores `ctx` keeps running until it returns. Pass `ctx` to every call that can block.
- **`RunTimeout` counts waiting time.** A run that is sleeping, waiting for an event or backing off between retries is still on the clock; set it from the user's point of view, not from the work's.
- **`OnTimeout` replaces `OnFailure` for timeouts.** Cleanup that must happen on every failure belongs in both, or in a function both call.
- **A step attempt's timeout is retried; a run's is not.** A step timeout on attempt 1 of 3 is one failed attempt; `OnTimeout` runs only if the last attempt also times out.
- **Setting `OnTimeout` alone needs `CapRunTimeout` too.** Without the capability a driver refuses the workflow rather than running `OnFailure` for timeouts silently.

## Related

- [Workflows and steps › Timeouts](../workflows/steps.md#timeouts) and [› Run a handler when a run fails](../workflows/steps.md#run-a-handler-when-a-run-fails).
- [Progress and heartbeats](progress.md): `Heartbeat` and `HeartbeatTimeout`.
- [Retries](retries.md): what follows a step timeout.
- [Errors](../concepts/errors.md): the sentinel errors.
- Example: [examples/snooze-heartbeat](../../examples/snooze-heartbeat/).
