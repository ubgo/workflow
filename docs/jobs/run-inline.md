# Run a workflow inline

Sometimes there is nothing to queue: a command-line tool imports one file, a one-off script backfills one day, a unit test wants a whole workflow to run and return. `workflow.RunInline` runs a workflow once in the caller's goroutine: no client, no `Start`, no worker, and the same `Workflow` value an app registers on its driver. This page covers running it, passing middleware and a sleeper, wait steps, and what it ignores.

## Before you start

- `RunInline` is in the core package and needs no driver and no capability.
- The example is [`examples/run-inline`](../../examples/run-inline). Run it with `go run ./run-inline` from `examples/`.

## Step 1: run it

<!-- snippet: examples/run-inline/main.go#inline -->
```go
res, err := workflow.RunInline(ctx, importUsers(), workflow.Job{Payload: []byte(`"users.csv"`)},
	workflow.InlineMiddleware(logSteps(out)), // the same middleware the driver gets
	workflow.InlineSleep(func(_ context.Context, d time.Duration) error {
		waited = append(waited, d) // a test skips the backoff; the default really waits
		return nil
	}),
)
if err != nil {
	return err
}
fmt.Fprintf(out, "store output: %s\n", res.Outputs["store"])
```

Steps run one at a time in a fixed order: of the steps whose parents have all succeeded, the first in declaration order goes next. Each gets its `StepInput`, a `StepInfo` whose `RunID` is `workflow.InlineRunID`, its timeout, and its retry policy (`EffectiveRetriesFor`, so `Job.Retry` applies too). The result holds every succeeded step's output and every step's attempt count.

- `InlineMiddleware` wraps every step and `OnFailure`, first outermost. Pass the middleware your driver gets so steps run exactly as they do there.
- `InlineSleep` replaces how backoffs and sleep steps wait. The default is a real timer that ends early with the ctx; a test records the delays and returns at once.
- `InlineNow` replaces the clock read for `RetryPolicy.Until` and `WaitSpec.Deadline`.

When a step fails for good, `OnFailure` runs once with that step's input and the error comes back wrapped with the step name. A step that returns `workflow.Discard` ends the call with an error matching `ErrDiscarded`, without `OnFailure`.

## Step 2: wait steps

There is no `Signal` without a client, so a workflow with a wait step is refused with an `UnsupportedError` before anything runs, unless you pass the events up front with `InlineEvents`. Each wait step takes the first event not yet taken whose name and resolved key match; none matching fails the step as a timeout.

<!-- snippet: examples/run-inline/main.go#wait -->
```go
approval := workflow.Workflow{Name: "approve", Steps: []workflow.Step{
	{Name: "wait", Wait: &workflow.WaitSpec{Event: "approved", Key: "doc-7"}},
}}
_, err = workflow.RunInline(ctx, approval, workflow.Job{})
fmt.Fprintln(out, "a wait step without InlineEvents is refused:", errors.Is(err, workflow.ErrUnsupported))
res, err = workflow.RunInline(ctx, approval, workflow.Job{},
	workflow.InlineEvents(workflow.Event{Name: "approved", Key: "doc-7", Payload: []byte(`"by ada"`)}))
if err != nil {
	return err
}
fmt.Fprintf(out, "wait output: %s\n", res.Outputs["wait"])
```

## Try it

```sh
cd examples
go run ./run-inline
```

```text
step fetch attempt 1: err=CSV host timed out
step fetch attempt 2: err=<nil>
step store attempt 1: err=<nil>
store output: "stored ["ada","grace"]"
attempts: fetch 2, store 1; backoff waits [1s]
a wait step without InlineEvents is refused: true
wait output: "by ada"
```

## Pitfalls

- **Scheduling policies do not apply.** Concurrency, rate limits, idempotency keys, cron, queues, priority and `RunAt` decide how a driver schedules many runs; one inline call has nothing to schedule and ignores them. Do not use `RunInline` to get "at most one at a time".
- **Nothing is durable.** A crash mid-call loses the run. Use a driver when the work must survive.
- **Parallel branches run in sequence.** Two steps with the same parent run one after the other, so a test that relies on them overlapping needs a real driver.
- **Driver middleware is not picked up by itself.** `RunInline` sees only what `InlineMiddleware` gives it; a plugin's step middleware must be passed by hand.

## Related

- [Workflows and steps](../workflows/steps.md): the definition `RunInline` runs.
- [Retries](retries.md): the policy it applies.
- [Testing with Fake](../testing/fake.md): assert what was enqueued without running it.
