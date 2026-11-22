# Run a workflow inline

Sometimes there is nothing to queue: a command-line tool imports one file, a one-off script backfills one day, a unit test wants a whole workflow to run and return. [`workflow.RunInline`](ds:block?id=runinline-c8wjejut) runs a workflow once in the caller's goroutine: no client, no `Start`, no worker, and the same `Workflow` value an app registers on its driver. This page covers running it, passing middleware and a sleeper, wait steps, and what it ignores.

## Before you start

- [`RunInline`](ds:block?id=runinline-c8wjejut) is in the core package and needs no driver and no capability.
- The example is [`examples/run-inline`](../../examples/run-inline). Run it with `go run ./run-inline` from `examples/`.

## Step 1: run it

<!-- snippet: examples/run-inline/main.go#inline -->
```go
res, err := workflow.RunInline(ctx, importUsers(), workflow.Job{Payload: []byte(`"users.csv"`)},
	workflow.InlineMiddleware(logSteps()), // the same middleware the driver gets
	workflow.InlineSleep(func(_ context.Context, d time.Duration) error {
		waited = append(waited, d) // a test skips the backoff; the default really waits
		return nil
	}),
)
if err != nil {
	return err
}
fmt.Printf("store output: %s\n", res.Outputs["store"])
```

Steps run one at a time in a fixed order: of the steps whose parents have all succeeded, the first in declaration order goes next ([`inlineRun.nextReady`](ds:block?id=inlinerun-nextready-hz8snv9f)). Each gets its `StepInput`, a `StepInfo` whose `RunID` is [`workflow.InlineRunID`](ds:block?id=inlinerunid-2qp9k38t) (`"inline"`), its timeout (`Step.Timeout`, else `Workflow.Timeout`), and its retry policy (`EffectiveRetriesFor`, so `Job.Retry` applies too; see [`inlineRun.step`](ds:block?id=inlinerun-step-pd7pu96z)).

`RunInline` returns a [`workflow.InlineResult`](ds:block?id=inlineresult-xtz8vfn7) with its error, on success and on failure alike, so a failed call still shows how far it got. It has two maps keyed by step name: `Outputs` holds the output of every step that succeeded (a wait step's event payload, a sleep step's input passed through), and `Attempts` holds how many attempts each run step made, the failing one included. Sleep and wait steps have no entry in `Attempts`.

- [`InlineMiddleware`](ds:block?id=inlinemiddleware-rkxqrjqg) wraps every run step's handler and `OnFailure`, first outermost. Pass the middleware your driver gets so steps run exactly as they do there.
- [`InlineSleep`](ds:block?id=inlinesleep-ds9j9c4m) replaces how backoffs and sleep steps wait. The default is a real timer that ends early with the ctx ([`realSleep`](ds:block?id=realsleep-s9d4etq5)); a test records the delays and returns at once.
- [`InlineNow`](ds:block?id=inlinenow-xbdv8jez) replaces the clock read for `RetryPolicy.Until` and `WaitSpec.Deadline`.

When a step fails for good, `OnFailure` runs once with that step's input and the error comes back wrapped with the step name ([`inlineRun.run`](ds:block?id=inlinerun-run-nn9zzxbq)). A step that returns `workflow.Discard`, or whose policy has `DiscardOnExhaust` and gives up, ends the call with an error matching `ErrDiscarded` and `ErrCancelled`, without `OnFailure` ([`RunInline`](ds:block?id=runinline-c8wjejut)).

## Step 2: wait steps

There is no `Signal` without a client, so a workflow with a wait step is refused with an `UnsupportedError` before anything runs ([`RunInline`](ds:block?id=runinline-c8wjejut)), unless you pass the events up front with [`InlineEvents`](ds:block?id=inlineevents-tu7pyhj2). Each wait step takes the first event not yet taken whose name and resolved key match; none matching fails the step as a timeout, with an error matching `context.DeadlineExceeded` ([`inlineRun.wait`](ds:block?id=inlinerun-wait-6ekbasev)).

<!-- snippet: examples/run-inline/main.go#wait -->
```go
approval := workflow.Workflow{Name: "approve", Steps: []workflow.Step{
	{Name: "wait", Wait: &workflow.WaitSpec{Event: "approved", Key: "doc-7"}},
}}
_, err = workflow.RunInline(ctx, approval, workflow.Job{})
fmt.Println("a wait step without InlineEvents is refused:", errors.Is(err, workflow.ErrUnsupported))
res, err = workflow.RunInline(ctx, approval, workflow.Job{},
	workflow.InlineEvents(workflow.Event{Name: "approved", Key: "doc-7", Payload: []byte(`"by ada"`)}))
if err != nil {
	return err
}
fmt.Printf("wait output: %s\n", res.Outputs["wait"])
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

- **Scheduling policies do not apply.** Concurrency, rate limits, idempotency keys, cron, queues, priority and `RunAt` decide how a driver schedules many runs; one inline call has nothing to schedule and ignores them. Do not use [`RunInline`](ds:block?id=runinline-c8wjejut) to get "at most one at a time".
- **Nothing is durable.** A crash mid-call loses the run: [`RunInline`](ds:block?id=runinline-c8wjejut) keeps its state in memory only. Use a driver when the work must survive.
- **Parallel branches run in sequence.** Two steps with the same parent run one after the other ([`inlineRun.run`](ds:block?id=inlinerun-run-nn9zzxbq)), so a test that relies on them overlapping needs a real driver.
- **Driver middleware is not picked up by itself.** `RunInline` sees only what [`InlineMiddleware`](ds:block?id=inlinemiddleware-rkxqrjqg) gives it; a plugin's step middleware must be passed by hand.

## Related

- [Workflows and steps](../workflows/steps.md): the definition `RunInline` runs.
- [Retries](retries.md): the policy it applies.
- [Testing with Fake](../testing/fake.md): assert what was enqueued without running it.
