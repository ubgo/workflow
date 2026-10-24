# Step middleware

Step middleware wraps every step attempt on a driver: logging, panic recovery, an app-wide timeout, errors that must not be retried, steps to skip, a circuit breaker for a failing dependency, tracing. It is written once against `workflow.Handler` and runs the same way on the memory, SQL, Redis and Hatchet drivers, so cross-cutting behaviour never lives inside handlers and never depends on which backend runs them. The stock pieces are in package `middleware`: skip a step, throttle a failing dependency, fail on chosen errors, release a step for later.

## Before you start

- A driver and the `middleware` package (core module, no extra dependency).
- Middleware is given to the driver when it is built: `memory.WithMiddleware`, `sqlstore.WithMiddleware`, `redis.WithMiddleware` or `hatchet.Config.Middleware`. The same chain works on all four.
- The runnable program for this page is [examples/middleware-stack](../../examples/middleware-stack/).

## Wrap every step

A `workflow.Middleware` is `func(next workflow.Handler) workflow.Handler`: it gets the step's context and input bytes, may act before and after calling `next`, and returns the output and error the driver records. Before the chain runs, every driver puts a `workflow.StepInfo` on the context (workflow, step, run ID, attempt and the run's labels), so middleware can tell runs and attempts apart with `workflow.StepInfoFrom(ctx)` without knowing the driver.

`middleware.Chain(a, b)` composes middleware into one, and the first argument is the outermost: `a` runs first on the way in and last on the way out. Nil entries are skipped, so a list can hold optional pieces. The drivers' `WithMiddleware` options follow the same rule.

<!-- snippet: examples/middleware-stack/main.go#chain -->
```go
tag := func(name string) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			fmt.Fprintln(out, name, "before")
			res, err := next(ctx, in)
			fmt.Fprintln(out, name, "after")
			return res, err
		}
	}
}
h := middleware.Chain(tag("a"), tag("b"))(func(context.Context, []byte) ([]byte, error) {
	fmt.Fprintln(out, "handler")
	return nil, nil
})
_, err := h(ctx, nil)
```

This prints `a before`, `b before`, `handler`, `b after`, `a after`. The example builds a whole stack for the memory driver, in an order that matters:

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

- `Logging` is outside `Recover`, so a panic reaches `Logging` as an error and is logged as a failed attempt. With the two swapped, the panic passes through `Logging` before anything recovers it, so `Logging` writes no failure line for that attempt. The OpenTelemetry step middleware goes outermost for the same reason ([OpenTelemetry](../observability/opentelemetry.md)).
- `FailOn` is inside `ThrottleErrors`, so the breaker sees a bad input already marked permanent and does not count it against the dependency.

Writing your own middleware is covered in [Write step middleware](../extending/writing-middleware.md#write-step-middleware).

## Recover from panics

`middleware.Recover(logger)` turns a panic in a step into an error wrapping `middleware.ErrPanic`, so one bad step fails its attempt instead of killing the worker. The panic value goes into the error; the stack goes to the logger at Error (a nil logger drops it) and never into the error string, which ends up in run status where it would leak file paths. The example's `panics` workflow ends `failed` with `workflow/middleware: step panicked: nil map`.

The error is retryable on purpose: whether a panic deserves a retry is the retry policy's decision. To fail at once, set `NoRetryOn: func(err error) bool { return errors.Is(err, middleware.ErrPanic) }` on the workflow's `RetryPolicy`.

## Time out a step

`middleware.Timeout(d)` gives every step attempt a context that ends after `d`, a cap the app sets across every workflow; `Step.Timeout` and `Workflow.Timeout` set one per step, and the drivers apply those themselves. A `d` of zero or less is a no-op. The timeout is cooperative: the step must watch `ctx`, because abandoning a goroutine that keeps running would leak it and let two attempts of one step overlap. The example's `slow` step blocks on `<-ctx.Done()` and fails with `context deadline exceeded` after 100 ms.

## Fail on chosen errors

`middleware.FailOn(match)` marks an error permanent when `match` reports true, so the step fails at once instead of spending its retries. An error already permanent stays so, and a nil `match` is a no-op. The example's `validate` workflow has two retries, yet its `bad input` error ends the run after one attempt. `workflow.Permanent(err)` returned from a handler, or `RetryPolicy.NoRetryOn`, does the same per handler or per workflow; `FailOn` is for a rule that holds across every workflow.

## Skip a step

`middleware.Skip(skip)` does not call the step when `skip(ctx, in)` reports true and returns a nil output and a nil error, so the step counts as done. The example skips any step whose input is `"skip"`: that run succeeds with no handler call. A nil `skip` is a no-op.

## Release a step for later

`middleware.Release(when, delay)` does not call the step when `when(ctx, in)` reports true: the attempt ends with `workflow.RetryAfter(middleware.ErrReleased, delay)`, so the driver tries the step again after `delay`. It suits a condition the app knows before any work starts: a partner API in maintenance, a tenant over its quota for the hour, a dependency that is not ready yet. To release unless a condition holds, negate the condition; a nil `when` is a no-op. To release from inside a handler, after work has started, return `workflow.RetryAfter(err, d)` there instead.

Two rules come from the retry policy, not from the middleware, and hold on every driver. The delay is honoured only under a policy whose `DelayFor` reads it, `workflow.RetryAfterDelay`; under any other policy a released attempt waits the policy's normal `Backoff`. And a release is an attempt: it counts against `RetryPolicy.Max`, and a step released on its last attempt fails its run with an error wrapping `middleware.ErrReleased`. The program for this section is [examples/release-jobs](../../examples/release-jobs/):

<!-- snippet: examples/release-jobs/main.go#release -->
```go
var maintenance atomic.Bool // set by the app while the partner API is down
inMaintenance := func(context.Context, []byte) bool { return maintenance.Load() }

c := memory.New(memory.WithClock(clk), memory.WithMiddleware(
	attempts(out), // outermost, so it sees each release as a failed attempt
	middleware.Release(inMaintenance, 10*time.Minute),
))
err := c.Register(workflow.Workflow{
	Name: "sync",
	Retry: workflow.RetryPolicy{
		Max:      3, // a release is an attempt: allow a few
		Backoff:  workflow.Backoff{Initial: time.Hour},
		DelayFor: workflow.RetryAfterDelay, // honour the release's 10 minutes, not the hour
	},
	Steps: []workflow.Step{{Name: "push", Run: func(context.Context, []byte) ([]byte, error) {
		return nil, nil
	}}},
})
```

While the flag is set every attempt is released for ten minutes, not the policy's hour; once it clears, the next attempt runs. A run whose condition never clears spends its retries and fails:

<!-- snippet: examples/release-jobs/main.go#release-runs-out -->
```go
maintenance.Store(true)
ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "sync"})
if err != nil {
	return err
}
for range 3 { // three retries, each released again
	if err := waitTimer(clk); err != nil {
		return err
	}
	clk.Advance(10 * time.Minute)
}
st, err = waitDone(ctx, c, ref)
if err != nil {
	return err
}
fmt.Fprintf(out, "run: %s at %s\n", st, clk.Now().Format("15:04"))
```

The `attempts` middleware in the example is outside `Release`, so it sees each release as a failed attempt; it tells a release from a failure with `errors.Is(err, middleware.ErrReleased)` and reads the delay with `workflow.RetryAfterDelay`, the same call the policy makes. A retry policy can match the sentinel too, for example a `NoRetryOn` that refuses to retry anything except a release.

## Throttle a failing dependency

`middleware.ThrottleErrors(cfg)` is a circuit breaker per key. After `MaxErrors` errors within `Window` (counted from the first of them) the key's circuit opens, and every call for that key returns an error wrapping `middleware.ErrThrottled` without running the step until `Cooldown` has passed. A success resets the count. Errors marked permanent, and `ErrThrottled` itself, are not counted: a permanent error is about the input, not the downstream service.

`ThrottleConfig.Key` picks the circuit a call belongs to, such as the downstream API it calls. Left nil, it keys by the workflow name from the `StepInfo`, so one failing workflow does not throttle the others. `ThrottleErrors` returns an error wrapping `workflow.ErrInvalid` unless `MaxErrors` is at least 1 and `Window` and `Cooldown` are positive.

<!-- snippet: examples/middleware-stack/main.go#throttle -->
```go
circuits := &announcingCircuit{MemoryCircuit: middleware.NewMemoryCircuit(clk), out: out}
throttle, err := middleware.ThrottleErrors(middleware.ThrottleConfig{
	Clock:     clk,
	Store:     circuits, // nil means a MemoryCircuit of its own
	MaxErrors: 2,
	Window:    time.Minute,
	Cooldown:  5 * time.Minute,
})
if err != nil {
	return err
}
```

The circuits live in a `CircuitStore`. The default, `middleware.NewMemoryCircuit(clock)`, keeps them in the process, so each worker process trips its own circuit, which is the point of a breaker that protects a downstream from this process's calls. A store shared by the fleet (a SQL table, Redis) gives one circuit per key across every worker, for a downstream with a fleet-wide error budget. The store owns the counting, so a shared store can count every worker's errors in one atomic step: it implements `IsOpen(ctx, key)`, `RecordError(ctx, key, policy)` (the `CircuitPolicy` carries `MaxErrors`, `Window` and `Cooldown`) and `Reset(ctx, key)`. The example's store wraps a `MemoryCircuit` and announces when a circuit opens:

<!-- snippet: examples/middleware-stack/main.go#circuit-store -->
```go
type announcingCircuit struct {
	*middleware.MemoryCircuit
	out io.Writer
}

func (a *announcingCircuit) RecordError(ctx context.Context, key string, p middleware.CircuitPolicy) error {
	if err := a.MemoryCircuit.RecordError(ctx, key, p); err != nil {
		return err
	}
	open, err := a.IsOpen(ctx, key)
	if err != nil {
		return err
	}
	if open {
		fmt.Fprintf(a.out, "circuit %q open for %s\n", key, p.Cooldown)
	}
	return nil
}
```

In the example, two failed `call-api` runs open the `call-api` circuit, the third run fails with `ErrThrottled` without a handler call, and after the fake clock moves past the cooldown the fourth run reaches the step again.

## Try it

```sh
cd examples
go run ./middleware-stack
```

The step middleware part of the output (the log lines have no time or run ID, and durations read `0s` because `Logging` measures on a fake clock):

```text
a before
b before
handler
b after
a after
-- step middleware
level=WARN msg="workflow step failed" workflow=charge step=run attempt=1 duration=0s permanent=false error="payments api down"
run charge: succeeded, 2 handler call(s)
level=ERROR msg="workflow step panicked" panic="nil map"
level=ERROR msg="workflow step failed" workflow=panics step=run attempt=1 duration=0s permanent=false error="workflow/middleware: step panicked: nil map"
run panics: failed (memory: step "run" failed after 1 attempt(s): workflow/middleware: step panicked: nil map), 1 handler call(s)
level=ERROR msg="workflow step failed" workflow=validate step=run attempt=1 duration=0s permanent=true error="field amount: bad input"
run validate: failed (memory: step "run" failed after 1 attempt(s): field amount: bad input), 1 handler call(s)
level=ERROR msg="workflow step failed" workflow=slow step=run attempt=1 duration=0s permanent=false error="context deadline exceeded"
run slow: failed (memory: step "run" failed after 1 attempt(s): context deadline exceeded), 1 handler call(s)
run charge: succeeded, 0 handler call(s)
-- throttle
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="payments api down"
run call-api: failed (memory: step "run" failed after 1 attempt(s): payments api down), 1 handler call(s)
circuit "call-api" open for 5m0s
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="payments api down"
run call-api: failed (memory: step "run" failed after 1 attempt(s): payments api down), 1 handler call(s)
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="workflow/middleware: step throttled after repeated errors (key \"call-api\")"
run call-api: failed (memory: step "run" failed after 1 attempt(s): workflow/middleware: step throttled after repeated errors (key "call-api")), 0 handler call(s)
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="payments api down"
run call-api: failed (memory: step "run" failed after 1 attempt(s): payments api down), 1 handler call(s)
```

`go test ./middleware-stack` runs the same program and checks every line.

The release section's program:

```sh
cd examples
go run ./release-jobs
```

```text
-- release while the API is in maintenance
attempt 1: released for 10m0s
attempt 2: released for 10m0s
maintenance over at 09:10
attempt 3: ran
run: succeeded at 09:20
attempt 1: released for 10m0s
attempt 2: released for 10m0s
attempt 3: released for 10m0s
attempt 4: released for 10m0s
run: failed at 09:50
```

With `Max: 3`, the second run gets four attempts (the first plus three retries), all released, and fails at 09:50. `go test ./release-jobs` checks every line.

## Pitfalls

- **Order is behaviour.** `Recover` outside `Logging` hides panics from the log; `FailOn` outside `ThrottleErrors` lets a bad input count against the circuit, because the breaker sees the error before it is marked permanent. The first middleware in the list is the outermost.
- **A throttled run with no retries just fails.** `ErrThrottled` is retryable so that the retry policy's backoff brings the step back after the cooldown, the same way a released step comes back. A workflow without retries fails its run instead, as the third `call-api` run shows; give a throttled workflow a backoff longer than the cooldown.
- **`MemoryCircuit` is per process.** Ten workers trip ten separate circuits. Use a shared `CircuitStore` when the budget is the downstream's, not the worker's.
- **A circuit store error never hides a step's result.** An `IsOpen` error fails closed (the step is not run and the error wraps `ErrThrottled`); a `RecordError` error is joined onto the failed step's error; a `Reset` error after a success is dropped.
- **`Timeout` cannot stop a step that ignores its context.** The step keeps running past the deadline and its result is whatever it returns late.
- **`Skip` gives children empty input.** A step whose children read its output should not be skipped.
- **A release without `RetryAfterDelay` waits the Backoff.** `Release`'s delay reaches the driver only through a policy with `DelayFor: workflow.RetryAfterDelay`; without it, a released step comes back on the policy's normal backoff, or not at all when the workflow has no retries.
- **Releases spend retries.** Each release is an attempt against `RetryPolicy.Max`, so a step that may be released for an hour at ten-minute steps needs `Max` of at least six.
- **A recovered panic is retried by default.** Add `NoRetryOn` for `middleware.ErrPanic` when a panic means a bug that a retry cannot fix.

## Related

- [Enqueue middleware](enqueue-middleware.md): the same idea around `Client.Enqueue`.
- [Log every step](../observability/logging.md#log-every-step): `Logging` and its levels.
- [OpenTelemetry](../observability/opentelemetry.md): spans and metrics per step.
- [Payload codecs](codecs.md): `DecodeStep`, the step half of encryption.
- [Write step middleware](../extending/writing-middleware.md#write-step-middleware): your own middleware.
- [Retries](../jobs/retries.md): the policy that decides what a failed attempt does next.
- Example: [examples/release-jobs](../../examples/release-jobs/) (`Release`).
- Example: [examples/middleware-stack](../../examples/middleware-stack/).
