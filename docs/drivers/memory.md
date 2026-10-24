# The in-memory driver

The memory driver keeps runs, waits, sleeps, schedules and queues in maps and goroutines inside the client value: no database, no server. Use it for tests, for local runs, and for apps that can afford to lose queued work when the process exits. Everything it holds is gone on exit, so it declares every capability except `workflow.CapDurable`. The same workflow definitions run unchanged on the [SQL driver](sqlstore.md) and the [Hatchet driver](hatchet.md).

## Before you start

- Install the core module: `go get github.com/ubgo/workflow@latest` (the memory driver is part of it and needs nothing else).
- Know the shape of a workflow: [First workflow](../start/first-workflow.md).

## Create the client

`memory.New` returns a ready client. Register workflows, then `Start`; nothing runs before `Start`. `Stop` shuts the client down and waits until every goroutine it started has exited; it is safe to call more than once and before `Start`.

<!-- snippet: examples/sqlite-worker/main.go#memory -->
```go
logger := slog.New(slog.NewTextHandler(io.Discard, nil)) // your app's logger
c := memory.New(
	memory.WithWorkers(4),                                  // step attempts at once (default 8)
	memory.WithLogger(logger),                              // errors no caller can see
	memory.WithJitterSource(func() float64 { return 0.5 }), // pin retry jitter in tests
)
defer c.Stop() // cancels unfinished runs and waits for every goroutine
```

Cancelling the context passed to `Start` shuts the client down too. Either way, every unfinished run becomes `StatusCancelled` (and is delivered to run-finished observers), and later calls fail with `workflow.ErrNotStarted`. Prefer `Stop` in tests, because it also waits.

## Options

| Option | What it sets | Default |
|---|---|---|
| `WithWorkers(n)` | How many step attempts execute at once. Waits and sleeps do not hold a worker. Values below 1 are ignored. | 8 |
| `WithClock(c)` | The source of every timer: delayed starts, sleep steps, wait deadlines, retry backoff, step timeouts, rate-limit windows, cron fires. Pass `clock.NewFake(t)` and "sleep three days" runs in microseconds. | `clock.Real` |
| `WithLogger(l)` | Where the driver reports what it cannot return to a caller: a cron fire that failed to enqueue, an `OnFailure` handler error, a panicking run-finished observer. | discard |
| `WithMiddleware(mw...)` | Step middleware around every step handler and the `OnFailure` handler; the first is outermost. | none |
| `WithJitterSource(rnd)` | Where `Backoff.Jitter` draws its uniform values in [0, 1), so a test can pin them. Must be safe for concurrent use. | `math/rand/v2` |
| `WithCronParser(p)` | Another cron dialect for `Workflow.Cron` and `Schedule.Cron`, giving up "the same spec on every driver". A parser that returns neither a schedule nor an error is refused. | `cron.Parse` |
| `WithQueues(qs...)` | Serve only these [named queues](../jobs/queues.md), each with its own worker cap, cluster cap (`Limit`) and priority. `Start` refuses a workflow on a queue not listed and `Enqueue` a `Job.Queue` not listed, since no other process could run it. | every queue from one pool |
| `WithRunRetention(r)` | How many finished runs it remembers for `Status`, `Runs`, `Retry` and `Delete`: `MaxAge` and `MaxRuns`; `RunRetention{}` keeps every one. Unfinished runs are never forgotten; a forgotten run's `Status` is `ErrNotFound`. | `DefaultRunRetention`: 24 hours, at most 10,000 |
| `WithStopWhenIdle(grace)` | Stop by itself, as `Drain` would, once the last unfinished run has ended and nothing was enqueued for `grace` ([stop gracefully](../concepts/client.md#stop-gracefully)). | off |
| `WithErrorHistory(n)` | How many failed attempts each run keeps in `RunInfo.Errors`, the newest kept; `n <= 0` turns it off ([Error history](../jobs/manage-runs.md#error-history)). | 20 (`workflow.DefaultErrorHistory`) |
| `WithRecycle(r)` | Drain and stop at a `workflow.Recycle` limit; unfinished runs end cancelled, since nothing outlives the client ([Worker recycling](../operations/recycling.md)). | off |
| `WithNodeID(id)` | The name this client gives itself in `StepInfo.Node`, `RunInfo.AttemptedBy` and `Nodes` ([Nodes and health](../operations/nodes.md)). | `workflow.DefaultNodeID()`, host name and process ID |
| `WithPayloadLimit(n)` | A payload cap in bytes that `Enqueue` checks, refusing a bigger job with a `*workflow.PayloadTooLargeError` ([Typed payloads](../jobs/payloads.md#cap-the-payload-size)); `0` means no cap. | no cap |

Handlers still run on real goroutines with a fake clock; only the waiting is simulated. In tests, `Client.RunAll` runs every delay, backoff, snooze and sleep at once instead ([Run everything now in tests](../testing/run-all.md)). See [Time](../concepts/time.md) and [Drive time in tests](../testing/fake-clock.md#drive-time-in-tests).

## What it can do

The memory driver declares every capability but `CapDurable`, `CapEnqueueOnly` and `CapShutdownRequeue`, including retry jitter and delay, every misfire policy, cron time zones, named queues, run management, pauses, schedule conditions, tick events, drain, the retrying and discarded run states, chains, worker scaling, closures, the overlap policy, trigger-now and last fire, error history, forcing a run to an end, snooze, heartbeats, run timeouts, tenants, due-time queries and reschedule, results, progress, lifecycle events, cancel one step, node identity and health, FIFO per key, weighted queues, seconds, active periods, `KeepRuns` and the tick time, plus `CapQueueLimitGlobal` (one process is the whole cluster, so `QueueConfig.Limit` is a plain cap), `CapDiscardExhausted` and `CapDrainReport`. It has no enqueue-only mode, because an in-process client has nobody else to run its work, and no requeue on shutdown, because `Stop` cancels every unfinished run. Start refuses nothing for lack of durability: durability is never inferred from a workflow, so the app picks a durable driver when it needs one.

<!-- snippet: examples/sqlite-worker/main.go#memory-caps -->
```go
caps := c.Capabilities()
fmt.Fprintln(out, "memory durable:", caps.Has(workflow.CapDurable))
fmt.Fprintln(out, "memory cron:", caps.Has(workflow.CapCron))
```

How it behaves, from `memory/doc.go`:

- **Admission.** A run starts when its concurrency bucket has room and its rate limits allow; until then it is `StatusQueued`. Among step attempts waiting for a worker, a higher `Job.Priority` goes first, then first come, first served.
- **Steps** run as a DAG: a step starts once every `After` parent has succeeded. A wait step outputs the signal's payload; a sleep step passes its input through.
- **Retries** follow the workflow's policy on the injected clock. `Permanent` errors, `NoRetryOn` and cancellation are never retried. When a step fails for good, the run's other steps are cancelled, `OnFailure` runs once, and the run becomes `StatusFailed`.
- **Idempotency.** A key is held from `Enqueue` until the run is terminal; a colliding `Enqueue` starts nothing and returns the holder's `RunRef` with a nil error.
- **Concurrency keys** must be Go functions (`workflow.KeyFunc`); an expression key (`workflow.KeyExpr`, in a concurrency policy, a rate limit or `WaitSpec.KeyFrom`) is refused at `Start` with an `UnsupportedError`.
- **Cron.** `Workflow.Cron` fires in its `CronLocation` (UTC when nil) and `Client.Schedule` in its `Location`, from `Start` onwards. The default misfire policy fires once after a long pause; every explicit `workflow.Misfire` policy is honoured.
- **Run ends.** It implements `workflow.RunNotifier` and declares `CapRunFinished`: every run that ends is delivered once to each observer, including runs cancelled before they started and runs cancelled by shutdown.

Run the program:

```sh
cd examples
go run ./sqlite-worker
```

```text
memory durable: false
memory cron: true
memory run: succeeded
sqlite durable: true
run id kind: run
sqlite run: succeeded
status after the end: succeeded
```

## Pitfalls

- Everything is lost on exit, including delayed starts and schedules. A production app that cannot lose work needs [sqlstore](sqlstore.md) or [Hatchet](hatchet.md).
- A workflow with a `workflow.KeyExpr` concurrency key registers fine and is refused at `Start`, not at `Register`.
- `Stop` (or cancelling the `Start` context) cancels every unfinished run; observers then see `StatusCancelled` with a cancelled ctx.
- With a fake clock, nothing that waits on time moves until the test calls `Advance` or `Set`.

## Related

- [The SQL driver](sqlstore.md) and [the Hatchet driver](hatchet.md)
- [Choose a driver](../choosing/drivers.md)
- [Capabilities](../concepts/capabilities.md)
- Example: [sqlite-worker](../../examples/sqlite-worker/) (runs one workflow on memory, then on SQLite) and [quickstart](../../examples/quickstart/)
