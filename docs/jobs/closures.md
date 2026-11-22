# Closures

Some background work is too small to deserve a registered workflow: warm a cache after a deploy, send a log line to another service, refresh one computed value. A closure is a Go function run as a job, with the queue, delay, retries and timeout a job has, on the in-memory driver. This page covers enqueuing one, how its run looks to the rest of the library, and why durable drivers refuse it.

## Before you start

- Closures need `workflow.CapClosure`, which only the in-memory driver declares ([memory caps](ds:block?id=memory-caps-yxkuq583)). The SQL, Redis and Hatchet drivers refuse them ([SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t) and [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) caps; see [Pitfalls](#pitfalls)).
- A closure ([`workflow.ClosureJob`](ds:block?id=closurejob-ghxxzktz)) lives only in the memory of the process that enqueued it. It is lost on a restart or a crash and never runs on another machine. Anything that must survive a restart is a [function registered by name](functions.md) or a [registered workflow](../workflows/steps.md) with a payload.
- The example is [`examples/closures`](../../examples/closures). Run it with `go run ./closures` from `examples/`.

## Step 1: enqueue a closure

[`workflow.EnqueueClosure(ctx, client, closure)`](ds:block?id=enqueueclosure-ngugsmdf) runs a [`workflow.ClosureJob`](ds:block?id=closurejob-ghxxzktz):

<!-- snippet: examples/closures/main.go#closure -->
```go
var attempts atomic.Int64
ref, err := workflow.EnqueueClosure(ctx, c, workflow.ClosureJob{
	Name: "warm-price-cache",
	Fn: func(ctx context.Context) error {
		if attempts.Add(1) == 1 {
			return errors.New("cache not ready")
		}
		return nil // warm the cache here
	},
	Retry: &workflow.RetryPolicy{Max: 2}, // no backoff: retry at once
})
if err != nil {
	return err
}
```

| Field | Meaning |
|---|---|
| `Name` | Required. What the closure is, for logs and run listings |
| `Fn func(ctx) error` | Required. The work. Its context carries `StepInfo` and is cancelled when the run is cancelled or the client stops. An error retries it under `Retry` |
| `Queue` | The queue it runs on; empty means the default queue. The client must serve that queue |
| `RunAt` | Delays the start; zero means now |
| `Labels` | Copied onto the run |
| `Priority` | Orders it among waiting runs, as `Job.Priority` |
| `Retry *RetryPolicy` | Nil means no retry. Since the closure never leaves the process, `NoRetryOn` and `DelayFor` are allowed |
| `Timeout` | Bounds each attempt; zero means none |

[`workflow.ValidateClosure`](ds:block?id=validateclosure-hd4ctss3) checks these fields, and `EnqueueClosure` calls it: a closure without a `Name` or `Fn`, a negative `Timeout`, an invalid queue name or retry policy, or a label under a reserved prefix is refused with `workflow.ErrInvalid`; so is a queue the client does not serve. Before `Start` it returns `workflow.ErrNotStarted` ([memory `EnqueueClosure`](ds:block?id=memory-client-enqueueclosure-ygdye9qq)).

```text
closure warm-price-cache: succeeded after 2 attempts, workflow "ubgo.workflow.closure", label "ubgo.workflow/closure"
```

## Step 2: treat it as a run

A closure run is an ordinary run of the workflow [`workflow.ClosureWorkflow`](ds:block?id=closureworkflow-5ad4abw7) (`ubgo.workflow.closure`) with one step, [`workflow.ClosureStep`](ds:block?id=closurestep-hvxugm4k) (`closure`). The closure's `Name` is in the label [`workflow.LabelClosure`](ds:block?id=labelclosure-ruy9jt45), under the reserved internal prefix so app labels cannot forge it ([memory `EnqueueClosure`](ds:block?id=memory-client-enqueueclosure-ygdye9qq)). So the ref works with `Status`, `Cancel` and the run listing, run-finished observers see its end, and step middleware wraps `Fn`:

<!-- snippet: examples/closures/main.go#status -->
```go
st, err := c.Status(ctx, ref)
if err != nil {
	return err
}
```

A delayed closure can be cancelled before it starts, like a delayed job:

<!-- snippet: examples/closures/main.go#delayed -->
```go
later, err := workflow.EnqueueClosure(ctx, c, workflow.ClosureJob{
	Name:  "nudge",
	Fn:    func(context.Context) error { return nil },
	RunAt: start.Add(time.Hour),
})
if err != nil {
	return err
}
if err := c.Cancel(ctx, later); err != nil { // a closure run cancels like any run
	return err
}
```

To list only closure runs, set `RunQuery.Workflow` to `workflow.ClosureWorkflow`; to find one closure, filter `RunQuery.Labels` on `workflow.LabelClosure`. You cannot register a workflow named `ubgo.workflow.closure`: [`Validate`](ds:block?id=validate-mnthpzkp) refuses the name.

## Durable drivers refuse closures

A Go function cannot be written to a database row or sent to another machine. The SQL, Redis and Hatchet drivers do not declare `CapClosure` ([SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t), [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z)), and [`EnqueueClosure`](ds:block?id=enqueueclosure-ngugsmdf) refuses there with an `UnsupportedError` naming it, instead of dropping the work. The durable form is a [function registered by name](functions.md): the name and a JSON argument are stored instead of the function, and it runs on every driver.

<!-- snippet: examples/closures/main.go#refused -->
```go
job := workflow.ClosureJob{Name: "warm-price-cache", Fn: func(context.Context) error { return nil }}
for _, d := range []struct {
	name string
	c    workflow.Client
}{{"sqlstore", sqlc}, {"hatchet", hatchet.New(hatchet.Config{})}} {
	_, err := workflow.EnqueueClosure(ctx, d.c, job)
	var unsupported *workflow.UnsupportedError
	if errors.As(err, &unsupported) {
		fmt.Printf("%s refuses: missing %s, ErrUnsupported %v\n", d.name, unsupported.Missing, errors.Is(err, workflow.ErrUnsupported))
	}
}
```

```text
sqlstore refuses: missing Closure, ErrUnsupported true
hatchet refuses: missing Closure, ErrUnsupported true
```

The failover client sends closures to its primary only ([`EnqueueClosure`](ds:block?id=failover-client-enqueueclosure-garv9aj8); a closure has no idempotency key, so it cannot safely go to two backends) and declares `CapClosure` when its primary does ([`Capabilities`](ds:block?id=failover-client-capabilities-cb8n9pqm)). A client wrapped with [`middleware.WrapClient`](ds:block?id=mw-wrapclient-nqppp3fu) or [`WithCodec`](ds:block?id=mw-withcodec-4aa2ymdy) forwards closures to the client underneath ([`Forward.EnqueueClosure`](ds:block?id=forward-enqueueclosure-d39b2q5k)); enqueue middleware does not run for them, since a closure has no payload or `Job`.

## Try it

```sh
cd examples
go run ./closures
```

```text
closure warm-price-cache: succeeded after 2 attempts, workflow "ubgo.workflow.closure", label "ubgo.workflow/closure"
Status: succeeded, step "closure"
closure nudge: cancelled before it started
closure without Fn: ErrInvalid true
sqlstore refuses: missing Closure, ErrUnsupported true
hatchet refuses: missing Closure, ErrUnsupported true
```

The first line is the closure that failed once and succeeded on its retry, run under the reserved workflow name and label every closure run carries; the second reads the same run through `Status` like any other run. The delayed closure was cancelled before its start, a closure without `Fn` is refused with `ErrInvalid`, and the SQL and Hatchet drivers refuse closures with an `UnsupportedError` naming the missing capability.

## Pitfalls

- **Lost on restart.** A closure ([`ClosureJob`](ds:block?id=closurejob-ghxxzktz)) waiting in the queue, delayed, or mid-retry when the process exits is gone. Use a [`Func`](functions.md) or a registered workflow for work that must happen.
- **Only on the in-memory driver.** Code that enqueues closures cannot move to the SQL, Redis or Hatchet driver unchanged ([`EnqueueClosure`](ds:block?id=enqueueclosure-ngugsmdf) refuses without `CapClosure`); each closure becomes a workflow there. Keep closures for work whose loss is harmless. See [Hatchet limitations](../drivers/hatchet.md#limitations).
- **No idempotency key, no chain.** A closure run is never matched by an idempotency key and cannot carry `Job.Chain` ([`ClosureEnqueuer`](ds:block?id=closureenqueuer-amwdzqnj); the memory driver builds its job with neither: [`EnqueueClosure`](ds:block?id=memory-client-enqueueclosure-ygdye9qq)).
- **Captured variables are shared.** The function runs in another goroutine, later. Capture copies of loop variables and anything the caller goes on changing.

## Related

- [Functions by name](functions.md): the durable form, on every driver.
- [Enqueue a job](enqueue.md): the registered-workflow path a closure stands in for.
- [Run a workflow inline](run-inline.md): run a workflow now, in the caller's goroutine.
- [Capabilities](../concepts/capabilities.md): how refusals work.
