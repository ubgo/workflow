# Enqueue middleware

Enqueue middleware wraps every `Client.Enqueue` before the job reaches the driver: routing jobs to another workflow, adding labels, refusing duplicates, debouncing bursts, sealing payloads, tracing. Like step middleware it is written once and works on every driver, because it wraps the `Client` interface rather than any backend. Routing tables and unique jobs are built this way.

## Before you start

- Any `workflow.Client` and the `middleware` package (core module).
- Unlike step middleware, enqueue middleware is not a driver option: `middleware.WrapClient` returns a new `Client`, and the app enqueues through that one.
- The runnable program for this page is [examples/middleware-stack](../../examples/middleware-stack/), the same program as [step middleware](step-middleware.md).

## Wrap every enqueue

A `workflow.EnqueueMiddleware` is `func(next workflow.EnqueueFunc) workflow.EnqueueFunc`, where `EnqueueFunc` has the shape of `Client.Enqueue`. It may change the job, refuse it by returning an error without calling `next`, or act on the result. A middleware of the app's own that records every enqueue:

<!-- snippet: examples/middleware-stack/main.go#enqueue-own -->
```go
audit := func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
	return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
		ref, err := next(ctx, job)
		fmt.Fprintf(out, "enqueue %s tier=%q queue=%q err=%v\n", job.Workflow, job.Labels["tier"], job.Labels["queue"], err)
		return ref, err
	}
}
```

`middleware.WrapClient(c, mws...)` returns a `Client` whose `Enqueue` runs through `mws` before reaching `c`. `middleware.ChainEnqueue` composes enqueue middleware with the same rule as `Chain`: the first argument sees the job first. Every other method, `Capabilities` included, is `c`'s own, so the wrapper never claims or hides a capability, and it forwards `workflow.RunNotifier` to `c`, so code that needs the run-finished hook (such as `UniqueUntilFinished` or a batch) still finds it through any number of wrappers.

<!-- snippet: examples/middleware-stack/main.go#wrap -->
```go
byOrder := func(j workflow.Job) string { return j.Labels["order"] }
unique := middleware.Unique(byOrder, middleware.NewMemoryUnique(nil), time.Minute)
client := middleware.WrapClient(c,
	middleware.Route(middleware.RouteRule{
		Match:  func(j workflow.Job) bool { return j.Labels["tier"] == "bulk" },
		Labels: map[string]string{"queue": "bulk"},
	}),
	audit, // sees the job after Route, before Unique
	unique,
)
```

Because `Route` comes first, `audit` sees the `queue` label it added; because `Unique` comes last, a refused duplicate passes through `audit` on its way back, so the audit line records the refusal too:

<!-- snippet: examples/middleware-stack/main.go#duplicate -->
```go
_, err := client.Enqueue(ctx, workflow.Job{Workflow: "charge", Labels: map[string]string{"order": "o-7"}})
fmt.Fprintln(out, "same order again:", errors.Is(err, workflow.ErrDuplicate))
```

The stock enqueue middleware: `Route` (below), `Unique` and `UniqueUntilFinished` ([unique jobs](../reliability/unique-jobs.md)), `Debounce` ([debounce](../reliability/debounce.md)), `EncodeEnqueue` and `WithCodec` ([payload codecs](codecs.md)), `ClaimCheck.Enqueue` ([claim check](../reliability/claim-check.md)), and the OpenTelemetry `Instrumentation.Enqueue` ([OpenTelemetry](../observability/opentelemetry.md)).

### Some middleware goes in a chain once

The library's own packages write labels under reserved prefixes onto the jobs they pass down (`workflow.ReservedLabelPrefixes()`): `ubgo.workflow/` for the middleware package's unique locks and claim-check references, `workflow.batch.` for batches, `otel.` for trace context. Each package refuses an incoming job that already carries a label under its own prefix, with an error wrapping `workflow.ErrInvalid`, instead of silently overwriting it, because a forged label could free another job's lock or choose the trace a run joins. A consequence: `Unique` (and `UniqueUntilFinished`) and the OpenTelemetry `Enqueue` must each appear in a chain once, since the second copy sees the first copy's labels:

<!-- snippet: examples/middleware-stack/main.go#twice -->
```go
twice := middleware.WrapClient(middleware.WrapClient(c, unique), unique)
_, err = twice.Enqueue(ctx, workflow.Job{Workflow: "charge", Labels: map[string]string{"order": "o-8"}})
fmt.Fprintln(out, "unique twice:", errors.Is(err, workflow.ErrInvalid), err)
```

Nothing reaches the driver, and the outer `Unique` releases the lock it took, so a corrected enqueue can pass. The claim check is the one exception the middleware package allows: `Unique` accepts the `ClaimCheckLabel` a claim check stamped before it.

## Route jobs

`middleware.Route(rules...)` applies the first `RouteRule` whose `Match` accepts the job and passes every job on, matched or not. A rule can replace `Job.Workflow` (`RouteRule.Workflow`) and add labels (`RouteRule.Labels`); a nil `Match` matches every job. Routing is an app-side table that keeps queue names and tenant labels out of the code that enqueues.

A label the job already carries wins over the rule's, as an explicit choice should win over a default, and the job's `Labels` map is copied before it is changed, so the caller's map is never mutated. In the example, a job labelled `tier=bulk` leaves `Route` with `queue=bulk` added.

## Unique until a step starts

`middleware.UniqueUntilStarted(by, store, ttl)` is `Unique` whose lock is freed as soon as a step of the run starts, not when the run ends. Use it for a job where one waiting copy is enough but a new one may queue up behind a running one: a report rebuilt from current data, a cache refresh, a search reindex of one record. While the first job waits for its delay or for a free worker, a second enqueue for the same key is refused with `workflow.ErrDuplicate`; once the first job's step has started, the next enqueue passes.

It returns two halves. The enqueue half is `Unique` and goes in the enqueuing process's `WrapClient` chain; the step half reads the lock's key and owner from the run's labels (through the driver's `StepInfo`) and frees it before the step runs, so it goes in the worker's step middleware. A worker in another process installs `middleware.ReleaseUniqueOnStart(store)`, the same step half, and then the store must be one both processes share, such as `sqlstore.UniqueStore`. The program is [examples/release-jobs](../../examples/release-jobs/):

<!-- snippet: examples/release-jobs/main.go#until-started -->
```go
byReport := func(j workflow.Job) string { return "report:" + j.Labels["account"] }
store := middleware.NewMemoryUnique(clk) // a shared SQL store across processes
uniqueEnqueue, uniqueStep := middleware.UniqueUntilStarted(byReport, store, time.Hour)

mem := memory.New(memory.WithClock(clk), memory.WithMiddleware(uniqueStep)) // the worker's half
c := middleware.WrapClient(mem, uniqueEnqueue)                              // the enqueuer's half
```

<!-- snippet: examples/release-jobs/main.go#until-started-run -->
```go
job := workflow.Job{Workflow: "report", Labels: map[string]string{"account": "a-1"}, RunAt: clk.Now().Add(5 * time.Minute)}
_, err = c.Enqueue(ctx, job)
verdict("first, runs in 5 minutes", err)
_, err = c.Enqueue(ctx, job)
verdict("second, while the first waits", err)

clk.Advance(5 * time.Minute)
<-started // the first run's step is running now
job.RunAt = time.Time{}
_, err = c.Enqueue(ctx, job)
verdict("third, while the first runs", err)
```

The step half runs on every step attempt. That is safe: a release names the run's own owner token, so a later step or retry of the first run never frees the lock a newer job has taken, and a repeat retries a release that failed before. A failed release is not reported; the `ttl` frees the key anyway, and stays the safety net for a run that never starts.

## Try it

```sh
cd examples
go run ./middleware-stack
```

The enqueue middleware part of the output:

```text
-- enqueue middleware
enqueue charge tier="bulk" queue="bulk" err=<nil>
run charge: succeeded, 0 handler call(s)
enqueue charge tier="" queue="" err=workflow: duplicate idempotency key: unique key "o-7" is held
same order again: true
unique twice: true workflow: invalid spec: Unique: label "ubgo.workflow/unique-key" uses the reserved prefix "ubgo.workflow/"
```

The first `charge` job carries the payload `"skip"`, so the `Skip` step middleware completes it without a handler call.

The unique-until-started section's program (`go run ./release-jobs`) ends with:

```text
-- unique until a step starts
first, runs in 5 minutes: accepted
second, while the first waits: refused (ErrDuplicate)
third, while the first runs: accepted
```

## Pitfalls

- **A wrapper hides optional interfaces other than `RunNotifier`.** `WrapClient` embeds `workflow.Client`, so a type assertion for `batch.GroupRunner`, `batch.GroupSpawner` or a driver's own methods fails on the wrapper. That is deliberate for batches: a batch dispatched through a wrapper enqueues each job through it, so its middleware (a codec sealing payloads) is never bypassed. Keep a reference to the driver's own client for its driver-specific methods.
- **Middleware that is not the outermost sees changed jobs.** Inside `WithCodec`, payloads are already sealed with a fresh random nonce, so a `Unique` or `Route` keyed on payload content never matches; put those in a `WrapClient` outside the codec.
- **Order changes what a refusal skips.** A middleware after `Unique` never sees a refused duplicate; one before it sees the job and the refusal coming back.
- **`sqlstore.Client.EnqueueTx` does not run enqueue middleware.** It writes the run inside the app's transaction directly. The [transactional outbox](../reliability/outbox.md) relays through any client, wrapped or not.
- **`UniqueUntilStarted` without its step half is plain `Unique`.** If no worker installs the step half (or `ReleaseUniqueOnStart`), nothing frees the lock early and it lasts the whole `ttl`. With `MemoryUnique`, the enqueuer and the worker must be one process.
- **Labels under a reserved prefix are refused, not overwritten.** An app label such as `otel.tenant` or `workflow.batch.id` fails the enqueue with `workflow.ErrInvalid`; pick another key.

## Related

- [Step middleware](step-middleware.md): the same idea around every step attempt.
- [Unique jobs](../reliability/unique-jobs.md), [Debounce](../reliability/debounce.md), [Claim check](../reliability/claim-check.md): stock enqueue middleware.
- [Payload codecs](codecs.md): `EncodeEnqueue` and `WithCodec`.
- [Reserved names](../reference/names.md#labels): every reserved label prefix.
- Example: [examples/middleware-stack](../../examples/middleware-stack/).
- Example: [examples/release-jobs](../../examples/release-jobs/) (`UniqueUntilStarted`).
