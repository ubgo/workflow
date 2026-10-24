# Closures

Some background work is too small to deserve a registered workflow: warm a cache after a deploy, send a log line to another service, refresh one computed value. A closure is a Go function run as a job, with the queue, delay, retries and timeout a job has, on the in-memory driver. This page covers enqueuing one, how its run looks to the rest of the library, and why durable drivers refuse it.

## Before you start

- Closures need `workflow.CapClosure`, which only the in-memory driver declares. The SQL and Hatchet drivers refuse them (see [Pitfalls](#pitfalls)).
- A closure lives only in the memory of the process that enqueued it. It is lost on a restart or a crash and never runs on another machine. Anything that must survive a restart is a [function registered by name](functions.md) or a [registered workflow](../workflows/steps.md) with a payload.
- The example is [`examples/closures`](../../examples/closures). Run it with `go run ./closures` from `examples/`.

## Step 1: enqueue a closure

`workflow.EnqueueClosure(ctx, client, closure)` runs a `workflow.ClosureJob`:

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

`workflow.ValidateClosure` checks these fields, and `EnqueueClosure` calls it: a closure without a `Name` or `Fn`, a negative `Timeout`, an unknown queue name or a label under a reserved prefix is refused with `workflow.ErrInvalid`. Before `Start` it returns `workflow.ErrNotStarted`.

```text
closure warm-price-cache: succeeded after 2 attempts, workflow "ubgo.workflow.closure", label "ubgo.workflow/closure"
```

## Step 2: treat it as a run

A closure run is an ordinary run of the workflow `workflow.ClosureWorkflow` (`ubgo.workflow.closure`) with one step, `workflow.ClosureStep` (`closure`). The closure's `Name` is in the label `workflow.LabelClosure`, under the reserved internal prefix so app labels cannot forge it. So the ref works with `Status`, `Cancel` and the run listing, run-finished observers see its end, and step middleware wraps `Fn`:

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

To list only closure runs, set `RunQuery.Workflow` to `workflow.ClosureWorkflow`; to find one closure, filter `RunQuery.Labels` on `workflow.LabelClosure`. You cannot register a workflow named `ubgo.workflow.closure`: `Validate` refuses the name.

## Durable drivers refuse closures

A Go function cannot be written to a database row or sent to another machine. The SQL and Hatchet drivers do not declare `CapClosure`, and `EnqueueClosure` refuses there with an `UnsupportedError` naming it, instead of dropping the work. The durable form is a [function registered by name](functions.md): the name and a JSON argument are stored instead of the function, and it runs on every driver.

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
		fmt.Fprintf(out, "%s refuses: missing %s, ErrUnsupported %v\n", d.name, unsupported.Missing, errors.Is(err, workflow.ErrUnsupported))
	}
}
```

```text
sqlstore refuses: missing Closure, ErrUnsupported true
hatchet refuses: missing Closure, ErrUnsupported true
```

The failover client sends closures to its primary only (a closure has no idempotency key, so it cannot safely go to two backends) and declares `CapClosure` when its primary does. A client wrapped with `middleware.WrapClient` or `WithCodec` forwards closures to the client underneath; enqueue middleware does not run for them, since a closure has no payload or `Job`.

## Pitfalls

- **Lost on restart.** A closure waiting in the queue, delayed, or mid-retry when the process exits is gone. Use a [`Func`](functions.md) or a registered workflow for work that must happen.
- **Only on the in-memory driver.** Code that enqueues closures cannot move to the SQL or Hatchet driver unchanged; each closure becomes a workflow there. Keep closures for work whose loss is harmless.
- **No idempotency key, no chain.** A closure run is never matched by an idempotency key and cannot carry `Job.Chain`.
- **Captured variables are shared.** The function runs in another goroutine, later. Capture copies of loop variables and anything the caller goes on changing.

## Related

- [Functions by name](functions.md): the durable form, on every driver.
- [Enqueue a job](enqueue.md): the registered-workflow path a closure stands in for.
- [Run a workflow inline](run-inline.md): run a workflow now, in the caller's goroutine.
- [Capabilities](../concepts/capabilities.md): how refusals work.
