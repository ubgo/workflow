# Stuck jobs

Some attempts never fail and never finish: an HTTP call with no timeout, a lock nobody releases, a query waiting on a row another transaction holds. Nothing goes red, the worker is simply gone. `middleware.Stuck` reports an attempt that has run longer than you expect, once, while it keeps running, so someone looks before the queue backs up. It detects only: ending an attempt is the job of a step `Timeout` or `HeartbeatTimeout`.

## Before you start

- Any driver: `Stuck` is step middleware and runs in the process that runs the step, so it needs no capability.
- Know how long your steps normally take. The threshold should sit well above the slow end of normal and below the point where you would want to know.
- The example is [`examples/lifecycle-events`](../../examples/lifecycle-events). Its stuck part runs a step that hangs until released and moves the middleware's fake clock six minutes. Run it with `go run ./lifecycle-events` from `examples/`.

## Report attempts that run too long

`middleware.Stuck(cfg)` returns a `workflow.Middleware`; install it like any step middleware (the driver's `WithMiddleware` option, or a plugin). The config:

- `After func(workflow.StepInfo) time.Duration` gives the threshold per step, from the attempt's workflow, step, queue or labels. When it is set it decides for every step; zero or less means "never report this one".
- `Default` is the threshold for every step when `After` is nil.
- `OnStuck func(ctx, middleware.StuckEvent)` is required. It is called at most once per attempt, on its own goroutine, while the attempt keeps running. Its context does not end when the attempt's does.
- `Clock` times the attempts; nil is the real clock. Pass a `clock.Fake` in tests.

<!-- snippet: examples/lifecycle-events/main.go#stuck -->
```go
mw := middleware.Stuck(middleware.StuckConfig{
	// After decides for every step; without it, Default applies to all.
	After: func(info workflow.StepInfo) time.Duration {
		if info.Workflow == "nightly-import" {
			return 2 * time.Hour // a step known to be slow
		}
		return 5 * time.Minute // zero or less would mean: never report
	},
	OnStuck: func(_ context.Context, ev middleware.StuckEvent) {
		// Report only: the attempt keeps running. Page someone, log,
		// or count it; a Timeout is what ends an attempt.
		reports <- ev
	},
	Clock: watch,
})
c := memory.New(memory.WithMiddleware(mw), memory.WithNodeID("worker-1"))
```

A `middleware.StuckEvent` carries `Info` (the attempt's `workflow.StepInfo`: workflow, step, run, attempt, labels and, on a driver with node identity, `Node`), `Started` (when the attempt started, on the middleware's clock) and `Running` (how long it had run when reported). `Node` tells an operator which machine to look at; see [Nodes and health](../operations/nodes.md).

## Try it

```sh
cd examples
go run ./lifecycle-events
```

The last lines:

```text
-- stuck
  report step=build attempt=1 node=worker-1 started=09:00 running=6m0s
  the attempt kept running and ended succeeded
```

The step hangs; after six minutes on the fake clock `OnStuck` is called once with the attempt and how long it ran; the step then answers and the run succeeds, untouched. `go test ./lifecycle-events` checks every line.

## Stuck, Timeout or HeartbeatTimeout

| | Reports | Ends the attempt | Where it runs |
|---|---|---|---|
| `middleware.Stuck` | Once, after the threshold | No | In the worker, any driver |
| `Step.Timeout` | The attempt's error | Yes, the retry policy applies | The driver |
| `Step.HeartbeatTimeout` | The attempt's error (`workflow.ErrHeartbeatTimeout`) | Yes, when the step stops calling `workflow.Heartbeat` | The driver, with `CapHeartbeat` |

Use them together: a generous `Timeout` as the hard stop, `Stuck` at a fraction of it so a person hears about a hang before the timeout fires, and `HeartbeatTimeout` for a long step that can say it is still alive. The timeout family is covered in [Timeouts](../jobs/timeouts.md).

## Pitfalls

- **A config that watches nothing fails every step.** Without `OnStuck`, or with neither `After` nor a positive `Default`, each wrapped step fails at once with an error matching `workflow.ErrInvalid` (marked permanent, so it is not retried), rather than silently watching nothing.
- **`After` replaces `Default`.** When `After` is set, a step it answers zero for is never reported, even with a `Default`; return the default yourself.
- **The attempt holds its worker.** `Stuck` does not free it. If hangs are common, add a `Timeout`.
- **`OnStuck` delays the end of the attempt.** The attempt's handler does not return to the driver until a running `OnStuck` has returned, so keep it short: send to a channel, log, increment a counter.
- **It watches one process.** An attempt lost with a crashed worker is not reported by anybody; the driver's lease expiry and `WithMaxReclaims` cover that case.

## Related

- [Lifecycle events](lifecycle.md): the live feed of attempts and progress.
- [Timeouts](../jobs/timeouts.md): step, wait, heartbeat and run deadlines.
- [Step middleware](../middleware/step-middleware.md): installing middleware.
- [Failure alerts and error reporting](failure-alerts.md): be told when runs fail.
- Example: [examples/lifecycle-events](../../examples/lifecycle-events/).
