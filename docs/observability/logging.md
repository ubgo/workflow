# Logging

Two kinds of log lines come out of a workflow app: one per step attempt (started, finished, failed), written by the `Logging` step middleware, and the errors the drivers and components hit in the background, where there is no caller to return them to (a cron fire that could not enqueue, a callback that failed, a poll that errored). Both go to a `*slog.Logger` you pass in; nothing logs to a global, and with no logger nothing is written.

## Before you start

- A `*slog.Logger` (any handler: text, JSON, or a bridge to your logging library).
- The `middleware` package for step lines (core module).
- The runnable program for this page is [examples/middleware-stack](../../examples/middleware-stack/).

## Log every step

`middleware.Logging(logger, clock, opts...)` is step middleware that writes a line when an attempt starts, finishes or fails, with the input and output sizes and the duration measured on `clock` (nil means the real clock). When the driver put a `workflow.StepInfo` on the context, which every driver does, each line also carries `workflow`, `step`, `run_id` and `attempt`. A failure line carries `duration`, `permanent` and `error`. A nil logger makes the middleware a no-op.

By default start and finish lines are Debug and every failed attempt is Error. `middleware.WithLogLevel(fn)` picks the level of each line instead: `fn` receives a `middleware.StepLogEvent` with the `Phase` (`middleware.StepStarted`, `StepFinished` or `StepFailed`), the `Err` of a failed attempt, the driver's `StepInfo` and `HasInfo`. Middleware cannot see the driver's retry policy, so a function that logs retried attempts at Warn and only the last one at Error compares the attempt with the retries the app configured:

<!-- snippet: examples/middleware-stack/main.go#log-level -->
```go
level := func(_ context.Context, ev middleware.StepLogEvent) slog.Level {
	switch {
	case ev.Phase != middleware.StepFailed:
		return slog.LevelDebug
	case workflow.IsPermanent(ev.Err) || ev.Info.Attempt > retries[ev.Info.Workflow]:
		return slog.LevelError
	default:
		return slog.LevelWarn // a retry will follow
	}
}
logging := middleware.Logging(logger, clk, middleware.WithLogLevel(level))
```

The example's `charge` workflow has two retries and fails once, so its failure is a Warn and the retry succeeds; `validate` fails with a permanent error, so its only failure is an Error:

```text
level=WARN msg="workflow step failed" workflow=charge step=run attempt=1 duration=0s permanent=false error="payments api down"
run charge: succeeded, 2 handler call(s)
level=ERROR msg="workflow step failed" workflow=validate step=run attempt=1 duration=0s permanent=true error="field amount: bad input"
```

(The example's handler drops the time and run ID so the output never varies, and measures on a fake clock, so durations read `0s`.)

`Logging` never logs payload bytes or labels: either can hold personal data or secrets, and a codec may not have run yet. Put it first in the chain, outside `Recover`, so a panic reaches it as a failed attempt; [step middleware](../middleware/step-middleware.md#wrap-every-step) shows the whole stack.

## Driver logs

The drivers and components take a logger for what they cannot return to a caller. Each defaults to discarding:

| Where | Option | What it reports |
|---|---|---|
| memory driver | `memory.WithLogger(l)` | a cron fire that failed to enqueue, an `OnFailure` handler error |
| SQL driver | `sqlstore.WithLogger(l)` | a failed poll, an `OnFailure` error, a cron fire that could not enqueue |
| Hatchet driver | `hatchet.Config{Logger: l}` | the run-finished notifier's polls, observer panics, worker stop, and `BatchSource` errors when it is given no logger of its own |
| batches | `batch.WithLogger(l)` | a callback that failed to enqueue, a cancel that failed, poller and resume errors |
| outbox | `outbox.WithLogger(l)` | a relay's failed enqueue, a dead entry |
| panics | `middleware.Recover(l)` | the panic value and stack of a step that panicked |

The example passes the same logger to the memory driver and to `Recover`:

<!-- snippet: examples/middleware-stack/main.go#stack -->
```go
c := memory.New(memory.WithLogger(logger), memory.WithMiddleware(
	logging, // outermost: it sees a recovered panic as a failed attempt
	middleware.Recover(logger),
	middleware.Timeout(100*time.Millisecond),
	middleware.Skip(func(_ context.Context, in []byte) bool { return string(in) == `"skip"` }),
	throttle, // circuits keyed by workflow name
	middleware.FailOn(func(err error) bool { return errors.Is(err, errBadInput) }), // inside throttle: bad input never trips a circuit
))
```

## Try it

```sh
cd examples
go run ./middleware-stack
```

Every `level=` line in the output comes from `Logging` or `Recover`; [step middleware › Try it](../middleware/step-middleware.md#try-it) lists the whole output.

## Pitfalls

- **Error text is logged as is.** `Logging` writes `err.Error()`, so a step that puts a secret in an error string leaks it to the log (and to run status, which stores the same text).
- **`Recover` logs a stack.** It is multi-line and long; route it to a handler that keeps it as one attribute (JSON), or drop the `stack` attribute as the example does for readable output.
- **The default level hides successes.** Start and finish lines are Debug, so a handler at Info shows only failures. Raise them with `WithLogLevel` if you want a line per attempt.
- **A component without a logger is silent.** A callback that failed to enqueue or a poll that keeps failing is invisible unless that component got a logger; pass one to each.

## Related

- [Step middleware](../middleware/step-middleware.md): where `Logging` sits in the chain.
- [OpenTelemetry](opentelemetry.md): spans and metrics instead of, or beside, log lines.
- [React when a run finishes](run-finished.md): log or audit whole runs instead of attempts.
- Example: [examples/middleware-stack](../../examples/middleware-stack/).
