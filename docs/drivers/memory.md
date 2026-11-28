# The in-memory driver

The memory driver keeps runs, waits, sleeps, schedules and queues in maps and goroutines inside the client value: no database, no server. Use it for tests, for local runs, and for apps that can afford to lose queued work when the process exits. Everything it holds is gone on exit, so its [capability list](ds:block?id=memory-caps-yxkuq583) leaves out `workflow.CapDurable` (and the two that only make sense with other processes, see [What it can do](#what-it-can-do)). The same workflow definitions run unchanged on the [SQL driver](sqlstore.md) and the [Hatchet driver](hatchet.md).

## Before you start

- Install the core module: `go get github.com/ubgo/workflow@latest` (the memory driver is part of it and needs nothing else).
- Know the shape of a workflow: [First workflow](../start/first-workflow.md).

## Create the client

[`memory.New`](ds:block?id=memory-new-8g6tnkkg) returns a ready client. Register workflows, then [`Start`](ds:block?id=memory-client-start-umc68snk); nothing runs before `Start`. [`Stop`](ds:block?id=memory-client-stop-zqtrmq8h) shuts the client down and waits until every goroutine it started has exited; it is safe to call more than once and before `Start`.

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

Cancelling the context passed to `Start` shuts the client down too. Either way, every unfinished run becomes `StatusCancelled` (and is delivered to run-finished observers) through [`shutdown`](ds:block?id=memory-client-shutdown-kwcx9htd), and later calls fail with `workflow.ErrNotStarted` ([`usableLocked`](ds:block?id=memory-client-usablelocked-ky48yta5)). Prefer `Stop` in tests, because it also waits.

The sections below are the program [examples/memory-options](../../examples/memory-options/), which runs every option on a fake clock; its full output is under [Try it](#try-it).

## Set the pool, the clock, the logger and the middleware

Five options shape how the client runs steps: [`WithWorkers`](ds:block?id=memory-withworkers-5t8muwcf), [`WithClock`](ds:block?id=memory-withclock-bkbmyc67), [`WithLogger`](ds:block?id=memory-withlogger-bvpcm6pn), [`WithMiddleware`](ds:block?id=memory-withmiddleware-yea2tnqr) and [`WithJitterSource`](ds:block?id=memory-withjittersource-5dryr759). A test usually wants all of them: a fake clock so "retry in 10 seconds" takes no time, a logger it can read, a middleware it can count, and a pinned jitter draw so a backoff is the same on every run:

<!-- snippet: examples/memory-options/main.go#options -->
```go
clk := clock.NewFake(start)
var calls atomic.Int32
countCalls := func(next workflow.Handler) workflow.Handler { // step middleware
	return func(ctx context.Context, in []byte) ([]byte, error) {
		calls.Add(1)
		return next(ctx, in)
	}
}
logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{ReplaceAttr: dropTime}))
c := memory.New(
	memory.WithWorkers(2),                                         // step attempts at once (default 8)
	memory.WithClock(clk),                                         // every timer: backoff, sleeps, cron, timeouts
	memory.WithLogger(logger),                                     // errors no caller can see
	memory.WithMiddleware(countCalls),                             // around every step and OnFailure
	memory.WithJitterSource(func() float64 { return pinnedDraw }), // pin Backoff.Jitter's draw
)
defer c.Stop()
```

With `Backoff{Initial: 10 * time.Second, Jitter: 0.5}` and the draw pinned to 0.5, the first retry waits `10s - 10s*0.5*0.5` ([`Backoff.JitteredDelay`](ds:block?id=backoff-jittereddelay-me9rbcyv)), so 7.5 seconds of fake time: after `Advance(7 * time.Second)` the run still reads `retrying`, after another half second it succeeds. An `OnFailure` handler that returns an error has no caller to return it to, so the driver logs it ([`fail`](ds:block?id=memory-client-fail-6tqmatzt)):

```text
sync after 7s: retrying
sync after 7.5s: succeeded, attempts 2
level=ERROR msg="memory: OnFailure handler failed" workflow=import run=run-6 error="pager is down"
middleware calls: 4
```

The middleware ran four times: two attempts of `pull`, one of `read`, and the `OnFailure` handler, which middleware wraps too.

## Serve only some queues

[`WithQueues`](ds:block?id=memory-withqueues-3cnhh98d) gives the client a list of [named queues](../jobs/queues.md), each with its own worker cap and priority. Because one process is the whole cluster, work for a queue the client does not serve could never run, so it is refused at once:

<!-- snippet: examples/memory-options/main.go#queues -->
```go
c := memory.New(memory.WithQueues(
	workflow.QueueConfig{Name: "emails", Workers: 4},               // four email steps at once
	workflow.QueueConfig{Name: "reports", Workers: 1, Priority: 1}, // one report, served first
))
defer c.Stop()
if err := c.Register(workflow.Workflow{Name: "welcome", Queue: "emails", Steps: []workflow.Step{{Name: "send", Run: noop}}}); err != nil {
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
_, err := c.Enqueue(ctx, workflow.Job{Workflow: "welcome", Queue: "video"}) // not served here
fmt.Fprintln(w, "job on queue video: ErrInvalid =", errors.Is(err, workflow.ErrInvalid))
```

A job whose `Job.Queue` is not listed fails at [`Enqueue`](ds:block?id=memory-client-enqueuelocked-z3x2cytk), and a workflow whose `Workflow.Queue` is not listed fails at [`Start`](ds:block?id=memory-client-start-umc68snk), both with `workflow.ErrInvalid`. A job with no queue runs on its workflow's queue.

## Keep fewer or more finished runs

The client remembers finished runs so `Status`, `Runs`, `Retry` and `Delete` can find them. By default it keeps them for a day, at most 10,000 ([`memory.DefaultRunRetention`](ds:block?id=memory-defaultrunretention-72hekrav)); [`WithRunRetention`](ds:block?id=memory-withrunretention-a7b2awty) changes both limits:

<!-- snippet: examples/memory-options/main.go#retention -->
```go
c := memory.New(memory.WithRunRetention(memory.RunRetention{
	MaxAge:  time.Hour, // forget a finished run an hour after it ended
	MaxRuns: 2,         // and keep at most the newest two
}))
```

A run past either limit is forgotten ([`pruneLocked`](ds:block?id=memory-client-prunelocked-k56zmza4)) when a run ends, when one is enqueued, or when `Runs` lists them; its `Status` then returns `workflow.ErrNotFound`, as after `Delete`. Unfinished runs are never forgotten. `memory.RunRetention{}` keeps every finished run until `Delete`, for a test that inspects everything. With three runs and `MaxRuns: 2`:

```text
default retention: 24h0m0s, at most 10000 runs
run-2: forgotten
run-5: succeeded
run-8: succeeded
```

## Stop when the work is done

A batch-style process starts, enqueues its work and should exit when nothing is left. [`WithStopWhenIdle(grace)`](ds:block?id=memory-withstopwhenidle-tqng4nhe) stops the client by itself, as `Drain` would, once its last unfinished run (delayed, queued, running or waiting) has ended and nothing was enqueued for `grace`; `Done` closes then:

<!-- snippet: examples/memory-options/main.go#stop-when-idle -->
```go
c := memory.New(memory.WithClock(clk), memory.WithStopWhenIdle(idleGrace))
if err := c.Register(workflow.Workflow{Name: "resize", Steps: []workflow.Step{{Name: "image", Run: noop}}}); err != nil {
	return err
}
if err := c.Start(ctx); err != nil {
	return err
}
var refs []workflow.RunRef
for range 3 { // three images to resize
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "resize"})
	if err != nil {
		return err
	}
	refs = append(refs, ref)
}
// The last run ends, nothing is enqueued for idleGrace: Done closes.
```

```text
stopped by itself after 30s idle
enqueue after the stop: ErrNotStarted = true
```

A client that never ran anything never stops this way, so `Start` followed by `Enqueue` is safe. Schedules do not keep it alive ([`checkIdleLocked`](ds:block?id=memory-client-checkidlelocked-frej948z)).

## Run thousands of schedules from one timer

By default each schedule, declared or made with `Client.Schedule`, runs its own goroutine and its own clock timer. [`WithScheduleHeap()`](ds:block?id=memory-withscheduleheap-4r7u9kqg) runs all of them from one [scheduler goroutine](ds:block?id=memory-client-scheduler-xqnr8ude) and one timer, armed for the earliest next tick in a heap:

<!-- snippet: examples/memory-options/main.go#schedule-heap -->
```go
if heap {
	opts = append(opts, memory.WithScheduleHeap()) // one goroutine, one timer
}
c := memory.New(opts...)
```

The same ticks fire either way; only the number of timers differs, which a test with a fake clock sees in `BlockUntil` and `Pending`:

```text
schedule heap false: 3 schedules, 3 pending timer(s), 3 fired at 09:05
schedule heap true: 3 schedules, 1 pending timer(s), 3 fired at 09:05
```

Use it for thousands of per-tenant schedules in one process, where a goroutine and a timer each is the cost that matters.

## Options

| Option | What it sets | Default |
|---|---|---|
| [`WithWorkers(n)`](ds:block?id=memory-withworkers-5t8muwcf) | How many step attempts execute at once. Waits and sleeps do not hold a worker. Values below 1 are ignored. | 8 ([`defaultWorkers`](ds:block?id=memory-defaultworkers-stn5cbsa)) |
| [`WithClock(c)`](ds:block?id=memory-withclock-bkbmyc67) | The source of every timer: delayed starts, sleep steps, wait deadlines, retry backoff, step timeouts, rate-limit windows, cron fires. Pass `clock.NewFake(t)` and "sleep three days" runs in microseconds. | `clock.Real` |
| [`WithLogger(l)`](ds:block?id=memory-withlogger-bvpcm6pn) | Where the driver reports what it cannot return to a caller: a cron fire that failed to enqueue, an `OnFailure` handler error, a panicking run-finished observer. | discard |
| [`WithMiddleware(mw...)`](ds:block?id=memory-withmiddleware-yea2tnqr) | Step middleware around every step handler and the `OnFailure` handler; the first is outermost. | none |
| [`WithJitterSource(rnd)`](ds:block?id=memory-withjittersource-5dryr759) | Where `Backoff.Jitter` draws its uniform values in [0, 1), so a test can pin them. Must be safe for concurrent use. | `math/rand/v2` |
| [`WithCronParser(p)`](ds:block?id=memory-withcronparser-jnqd5hex) | Another cron dialect for `Workflow.Cron` and `Schedule.Cron`, giving up "the same spec on every driver". A parser that returns neither a schedule nor an error is refused ([`parseCronFor`](ds:block?id=memory-client-parsecronfor-metu9zbb)). | `cron.Parse` |
| [`WithQueues(qs...)`](ds:block?id=memory-withqueues-3cnhh98d) | Serve only these [named queues](../jobs/queues.md), each with its own worker cap, cluster cap (`Limit`) and priority. `Start` refuses a workflow on a queue not listed and `Enqueue` a `Job.Queue` not listed, since no other process could run it. | every queue from one pool |
| [`WithRunRetention(r)`](ds:block?id=memory-withrunretention-a7b2awty) | How many finished runs it remembers for `Status`, `Runs`, `Retry` and `Delete`: `MaxAge` and `MaxRuns`; `RunRetention{}` keeps every one. Unfinished runs are never forgotten; a forgotten run's `Status` is `ErrNotFound`. | [`DefaultRunRetention`](ds:block?id=memory-defaultrunretention-72hekrav): 24 hours, at most 10,000 |
| [`WithStopWhenIdle(grace)`](ds:block?id=memory-withstopwhenidle-tqng4nhe) | Stop by itself, as `Drain` would, once the last unfinished run has ended and nothing was enqueued for `grace` ([stop gracefully](../concepts/client.md#stop-gracefully)). | off |
| [`WithErrorHistory(n)`](ds:block?id=memory-witherrorhistory-rte3qjyb) | How many failed attempts each run keeps in `RunInfo.Errors`, the newest kept; `n <= 0` turns it off ([Error history](../jobs/manage-runs.md#error-history)). | 20 ([`workflow.DefaultErrorHistory`](ds:block?id=defaulterrorhistory-cexsqn9u)) |
| [`WithRecycle(r)`](ds:block?id=memory-withrecycle-tsfrb2gk) | Drain and stop at a `workflow.Recycle` limit; unfinished runs end cancelled, since nothing outlives the client ([Worker recycling](../operations/recycling.md)). | off |
| [`WithNodeID(id)`](ds:block?id=memory-withnodeid-j8sp495g) | The name this client gives itself in `StepInfo.Node`, `RunInfo.AttemptedBy` and `Nodes` ([Nodes and health](../operations/nodes.md)). | [`workflow.DefaultNodeID()`](ds:block?id=defaultnodeid-wzxm78ge), host name and process ID |
| [`WithPayloadLimit(n)`](ds:block?id=memory-withpayloadlimit-47kfqt3d) | A payload cap in bytes that `Enqueue` checks, refusing a bigger job with a `*workflow.PayloadTooLargeError` ([Typed payloads](../jobs/payloads.md#cap-the-payload-size)); `0` means no cap. | no cap |
| [`WithScheduleHeap()`](ds:block?id=memory-withscheduleheap-4r7u9kqg) | Run every schedule, declared and run-time, from one scheduler goroutine and one clock timer: a heap ordered by next tick, the timer armed for the earliest. Fires exactly what the default fires; for thousands of schedules in one process. On a fake clock all schedules then share one pending timer, and schedules due together are handled one after another. | off: a goroutine and a timer per schedule |

Handlers still run on real goroutines with a fake clock; only the waiting is simulated. In tests, `Client.RunAll` runs every delay, backoff, snooze and sleep at once instead ([Run everything now in tests](../testing/run-all.md)). See [Time](../concepts/time.md) and [Drive time in tests](../testing/fake-clock.md#drive-time-in-tests).

## What it can do

The memory driver [declares](ds:block?id=memory-caps-yxkuq583) every capability but `CapDurable`, `CapEnqueueOnly` and `CapShutdownRequeue`, including retry jitter and delay, every misfire policy, cron time zones, named queues, run management, pauses, schedule conditions, tick events, drain, the retrying and discarded run states, chains, worker scaling, closures, the overlap policy, trigger-now and last fire, error history, forcing a run to an end, snooze, heartbeats, run timeouts, tenants, due-time queries and reschedule, results, progress, lifecycle events, cancel one step, node identity and health, FIFO per key, several concurrency keys per workflow, limits shared by several workflows and slots given back while a run waits (`CapConcurrencyLimits`, `CapConcurrencyScope`, `CapHoldWhileRunning`), schedule idempotency keys, step checkpoints kept in memory and coalescing workflows (`CapScheduleKey`, `CapCheckpoint`, `CapCoalesce`), weighted queues, seconds, active periods, `KeepRuns` and the tick time, plus `CapQueueLimitGlobal` (one process is the whole cluster, so `QueueConfig.Limit` is a plain cap), `CapDiscardExhausted` and `CapDrainReport`. It has no enqueue-only mode, because an in-process client has nobody else to run its work, and no requeue on shutdown, because `Stop` cancels every unfinished run. Start refuses nothing for lack of durability: durability is never inferred from a workflow, so the app picks a durable driver when it needs one.

<!-- snippet: examples/sqlite-worker/main.go#memory-caps -->
```go
caps := c.Capabilities()
fmt.Println("memory durable:", caps.Has(workflow.CapDurable))
fmt.Println("memory cron:", caps.Has(workflow.CapCron))
```

How it behaves, from `memory/doc.go`:

- **Admission.** A run starts when its concurrency bucket has room (with `Workflow.ConcurrencyLimits`, its bucket of every limit too, all taken at once: [`limitRoomLocked`](ds:block?id=memory-client-limitroomlocked-xgp4r9tg); a limit with a `Scope` counts in one bucket per scope and key value for every workflow naming it: [`limitKeyOf`](ds:block?id=memory-limitkeyof-9nn9qdbp)) and its rate limits allow ([`admitLocked`](ds:block?id=memory-client-admitlocked-g322u8vq)); until then it is `StatusQueued`. A run whose keys are set to `HoldWhileRunning` gives those slots back when nothing of it executes or waits for a worker ([`parkLocked`](ds:block?id=memory-client-parklocked-v5n2gx7e)), and its next ready step waits in the same admission queue, at the run's original place, until they all have room again ([Choose how long a run holds its slot](../jobs/concurrency.md#choose-how-long-a-run-holds-its-slot)). Among step attempts waiting for a worker, a higher `Job.Priority` goes first, then first come, first served ([`before`](ds:block?id=memory-client-before-2xjrartk), which puts a higher `QueueConfig.Priority` ahead of both).
- **Steps** run as a DAG: a step starts once every `After` parent has succeeded ([`readyLocked`](ds:block?id=memory-run-readylocked-amuukcsg)). A wait step outputs the signal's payload; a sleep step passes its input through.
- **Retries** follow the workflow's policy on the injected clock ([`RetryPolicy.Next`](ds:block?id=retrypolicy-next-vj4catr9)). `Permanent` errors, `NoRetryOn` and cancellation are never retried. When a step fails for good, the run's other steps are cancelled, `OnFailure` runs once, and the run becomes `StatusFailed` ([`fail`](ds:block?id=memory-client-fail-6tqmatzt)).
- **Idempotency.** A key is held from [`Enqueue`](ds:block?id=memory-client-enqueuelocked-z3x2cytk) until the run is terminal ([`finishLocked`](ds:block?id=memory-client-finishlocked-7txxf6we) releases it); a colliding `Enqueue` starts nothing and returns the holder's `RunRef` with a nil error (on a [coalescing workflow](../reliability/coalesce.md) whose holder has been admitted, it keeps one run pending behind the holder instead).
- **Concurrency keys** must be Go functions (`workflow.KeyFunc`); an expression key (`workflow.KeyExpr`, in a concurrency policy, a rate limit or `WaitSpec.KeyFrom`) is refused at `Start` with an `UnsupportedError` ([`exprKeys`](ds:block?id=memory-exprkeys-mrxv22pq)).
- **Cron.** `Workflow.Cron` fires in its `CronLocation` (UTC when nil) and `Client.Schedule` in its `Location`, from `Start` onwards. The default misfire policy fires once after a long pause ([`effectiveMisfire`](ds:block?id=memory-effectivemisfire-cz2fnaea)); every explicit `workflow.Misfire` policy is honoured.
- **Run ends.** It implements `workflow.RunNotifier` ([`OnRunFinished`](ds:block?id=memory-client-onrunfinished-4jxy93vq)) and declares `CapRunFinished`: every run that ends is delivered once to each observer, including runs cancelled before they started and runs cancelled by shutdown.

The same workflow on memory and then on SQLite, from [examples/sqlite-worker](../../examples/sqlite-worker/) (`go run ./sqlite-worker` from `examples/`):

```text
memory durable: false
memory cron: true
memory run: succeeded
sqlite durable: true
run id kind: run
sqlite run: succeeded
status after the end: succeeded
```

## Try it

```sh
cd examples
go run ./memory-options
```

```text
sync after 7s: retrying
sync after 7.5s: succeeded, attempts 2
level=ERROR msg="memory: OnFailure handler failed" workflow=import run=run-6 error="pager is down"
middleware calls: 4
job on queue video: ErrInvalid = true
welcome on queue emails: succeeded
workflow on unserved queue at Start: ErrInvalid = true
default retention: 24h0m0s, at most 10000 runs
run-2: forgotten
run-5: succeeded
run-8: succeeded
stopped by itself after 30s idle
enqueue after the stop: ErrNotStarted = true
schedule heap false: 3 schedules, 3 pending timer(s), 3 fired at 09:05
schedule heap true: 3 schedules, 1 pending timer(s), 3 fired at 09:05
```

`main_test.go` in the example checks every line, so the output above is what the code does today.

## Record run history

A [run recorder](../observability/run-recorder.md) works on this driver as on any other: `workflow.Use(memory.New(), rec.Plugin())`, then register and start through the client `Use` returned.

- **Every end is recorded.** The driver [declares](ds:block?id=memory-caps-yxkuq583) `CapRunFinished`, so each run end reaches the recorder's observer, including runs cancelled before they started and runs cancelled by `Stop`, which end `cancelled` on the record.
- **The Reconciler can backfill.** The driver [declares](ds:block?id=memory-caps-yxkuq583) `CapRunQuery`, so a Reconciler pass finds runs enqueued or registered past the recorder and records them with source `external`.
- **The history outlives the driver when the store does.** The driver keeps finished runs for a day, at most 10,000 (`WithRunRetention`), and nothing across a restart; a record in `sqlrecorder` stays until the recorder's own [retention](../observability/run-recorder.md#retention) removes it. [`recorder.NewMemoryStore()`](ds:block?id=rec-newmemorystore-5yrbgb94) lives and dies with the process.
- **Run IDs restart with every client.** This driver numbers runs from a counter of its own (`run-2`, `run-5` and so on; step attempts share the counter, so the numbers are not consecutive) that starts again with every client, so a persistent store shared by two memory clients, or kept across a restart, would merge different runs under one ID. Give each memory client its own store: an in-memory store, or `sqlrecorder` with a table prefix of its own ([`sqlrecorder.WithTablePrefix`](ds:block?id=sqlrec-withtableprefix-avt2n8sd)).

The recorder's end-to-end suite runs its driver cases on this driver with `sqlrecorder` on SQLite, Postgres and MySQL, and [examples/run-recorder](../../examples/run-recorder/) and [examples/step-events](../../examples/step-events/) run on it.

## Pitfalls

- Everything is lost on exit, including delayed starts and schedules (no `CapDurable` in the [capability list](ds:block?id=memory-caps-yxkuq583)). A production app that cannot lose work needs [sqlstore](sqlstore.md) or [Hatchet](hatchet.md).
- A workflow with a `workflow.KeyExpr` concurrency key registers fine and is refused at [`Start`](ds:block?id=memory-client-start-umc68snk), not at `Register` ([`exprKeys`](ds:block?id=memory-exprkeys-mrxv22pq)).
- [`Stop`](ds:block?id=memory-client-stop-zqtrmq8h) (or cancelling the `Start` context) cancels every unfinished run ([`shutdown`](ds:block?id=memory-client-shutdown-kwcx9htd)); observers then see `StatusCancelled` with a cancelled ctx.
- With a fake clock, nothing that waits on time moves until the test calls `Advance` or `Set` ([`WithClock`](ds:block?id=memory-withclock-bkbmyc67) feeds every timer).
- `BlockUntil` counts timers, and the default arms one per schedule, so a test with three schedules waits for three. With [`WithScheduleHeap`](ds:block?id=memory-withscheduleheap-4r7u9kqg) the schedules share one timer: wait for one plus whatever else the test arms, or the wait never ends.
- With `WithScheduleHeap`, a slow `ScheduleWhen` delays the other schedules due at the same moment, since one [goroutine](ds:block?id=memory-client-scheduler-xqnr8ude) handles them in turn and [asks](ds:block?id=memory-client-askwhen-w456jtt5) each condition inline. Keep it fast, or leave the option off.
- [`WithStopWhenIdle`](ds:block?id=memory-withstopwhenidle-tqng4nhe) stops the client after a quiet `grace`, schedules or not: a long-running service with schedules but bursts of runs must not use it, or it stops between bursts.
- Past [`WithRunRetention`](ds:block?id=memory-withrunretention-a7b2awty)'s limits a finished run reads `ErrNotFound`, so a caller that polls `Status` long after a run ended must treat `ErrNotFound` as "gone", or raise the limits.
- [`WithJitterSource`](ds:block?id=memory-withjittersource-5dryr759) and the source behind it are called from several workers at once; a source with state needs a lock.

## Related

- [The SQL driver](sqlstore.md) and [the Hatchet driver](hatchet.md)
- [Choose a driver](../choosing/drivers.md)
- [Capabilities](../concepts/capabilities.md)
- [Record run history](../observability/run-recorder.md)
- Example: [memory-options](../../examples/memory-options/) (every option on a fake clock), [sqlite-worker](../../examples/sqlite-worker/) (runs one workflow on memory, then on SQLite) and [quickstart](../../examples/quickstart/)
