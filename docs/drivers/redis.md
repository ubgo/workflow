# The Redis driver

The `redis` module runs workflows on Redis through go-redis v9. Every run, schedule, lease and idempotency key is a Redis key, so a restart or a dead worker loses nothing, and any number of clients on any number of machines share one Redis. Pick it when your app already runs Redis and you want durable jobs, workflows and cron without a SQL database or an engine. It covers what the [SQL driver](sqlstore.md) covers, except closures and expression keys; pick sqlstore when an enqueue must commit inside your own SQL transaction.

## Before you start

- A Redis that does not evict keys (`maxmemory-policy noeviction`): an evicted run or lease is lost work, not a cache miss. The shared local stack's Redis is set up that way.
- The module: `go get github.com/ubgo/workflow/redis@latest` (Go 1.24 or later). It is separate from the core, so an app without Redis never downloads go-redis.
- The example is [`examples/redis-worker`](../../examples/redis-worker). Run it with `go run ./redis-worker` from `examples/`; set `REDIS_URL` for the live part.

## Create the client

The package is named `redis`, like go-redis itself, so import one of them under another name:

<!-- snippet: examples/redis-worker/main.go#client -->
```go
rdb := goredis.NewClient(opts) // the package is named redis too, hence goredis
defer rdb.Close()
prefix := "example-" + strconv.FormatInt(time.Now().UnixNano(), 36)
newWorker := func() (*redis.Client, error) {
	return redis.New(rdb,
		redis.WithPrefix(prefix), // clients that share a prefix share every run and schedule
		redis.WithPollInterval(50*time.Millisecond),
	)
}
```

Then `Register` your workflows and `Start`, as on every driver; nothing runs before `Start`, and cancelling its context stops the client. Clients that share a prefix (`WithPrefix`, default `wf`) share every run and schedule; give each app, and each test, its own.

## What it can do and what it refuses

It declares, each proven by the conformance suite against a real Redis: `CapDurable`, `CapDelayedStart`, `CapDurableSleep`, `CapWaitForEvent`, `CapIdempotencyKey`, `CapConcurrencyKey` (every strategy) and `CapKeyFIFO`, `CapRateLimit` (sliding windows), `CapCron`, `CapCancel`, `CapRunFinished`, `CapRetryJitter`, `CapRetryDelay`, `CapQueue` and `CapQueueOverride`, `CapJobRetry`, `CapRetryUntil`, `CapRetryMaxErrors`, `CapDiscard`, `CapDiscardExhausted` and `CapRunStates`, `CapSnooze`, `CapHeartbeat`, `CapRunTimeout`, `CapTenant`, `CapNodeIdentity`, `CapResult`, `CapDueQuery`, the three misfire policies, `CapCronZone`, `CapCronSeconds`, `CapScheduleActive`, `CapScheduleKeep`, `CapTickTime`, `CapScheduleWhen`, `CapScheduleWindow`, `CapTickEvents`, `CapSchedulePause`, `CapRunQuery`, `CapLabelPrefix`, `CapRetryRun`, `CapDeleteRun`, `CapCancelAll`, `CapPauseWorkflow`, `CapPauseQueue`, `CapChain`, `CapDrain`, `CapDrainReport`, `CapScheduleList`, `CapScheduleTrigger`, `CapScheduleHistory`, `CapOverlapSkip`, `CapEnqueueOnly`, `CapErrorHistory`, `CapShutdownRequeue`, `CapForceRun` and `CapWorkerScale`.

The client also implements the interfaces that have no capability bit: `RunRescheduler` (`Reschedule`), `StepCanceller` (`CancelStep`), `HealthChecker` (`Health`), `NodeLister` (`Nodes`), `LifecycleNotifier` (`OnLifecycle`) and `PayloadLimiter`, whose limit is 512 MiB, the largest value Redis stores, lowered with `WithPayloadLimit`.

`CapWorkerScale` means `QueueLoads` and `SetWorkers` over the `WithQueues` list ([Worker balancing](../operations/balancing.md)). It does not declare `CapClosure` or `CapQueueLimitGlobal`, and it refuses three things with an `UnsupportedError` at `Start`, `Enqueue` or `Schedule` instead of ignoring them:

- **Closures** (`CapClosure`): a Go closure cannot be stored in Redis. Register a workflow, or use [functions by name](../jobs/functions.md).
- **A queue limit across clients** (`CapQueueLimitGlobal`): a run is claimed whole, so the driver keeps no count of a queue's in-flight step attempts across clients, and a non-zero `QueueConfig.Limit` is refused at `Start`. `QueueConfig.Workers` still caps one client; see [Named queues](../jobs/queues.md#cap-a-queue-across-every-worker).
- **Expression keys** (`KeyExpr`): concurrency, rate and wait keys are Go functions (`workflow.KeyFunc`) evaluated in the client; an engine-language expression is refused at `Start`.

What the capabilities mean is in [Capabilities](../reference/capabilities.md); how each driver compares is in [Choose a driver](../choosing/drivers.md).

## How work moves

- **Enqueue** writes a run hash and puts its ID in the ready set of its queue and workflow (ordered by priority, then by ID, which starts with the creation time), or in the due set for a future `RunAt`. An idempotency key is checked and set in the same Lua script, so two clients racing on one key make one run; the key is freed when the run ends. A published message wakes idle workers at once; polling (`WithPollInterval`, one second) is the fallback.
- **Limits are decided in one script.** A run of a workflow with a concurrency or rate policy, or of a workflow the enqueuing client did not register (an enqueue-only client, a chained job, a schedule fired elsewhere), first waits in a pending list; a client that registered the workflow evaluates its queue and keys in Go and admits it in one script against the stored concurrency slots and sliding rate windows, so the limits hold across every client.
- **A run is claimed whole** by one client, under a lease (`WithLease`, 30 seconds) renewed every third of it. A client claims only the workflows it registered and, with `WithQueues`, only the queues it serves, the higher `QueueConfig.Priority` first (equal priorities share by `QueueConfig.Weight`). A pause (everything, a workflow or a queue) is stored in Redis, checked by every claim, and survives a restart. The holder runs every step whose parents are done, in parallel.
- **Writes are fenced.** Every write compares the holder's claim token with the run's owner, so a worker whose lease expired and whose run another client took writes nothing: each step outcome is recorded once per lease. A step whose result was lost with its worker runs again (at least once).
- **A dead worker's runs** are claimed again when their leases expire. A run whose lease expires more than `WithMaxReclaims` times in a row (default 5) with no step finishing fails with `redis.ErrAbandoned`, so a step that kills every worker cannot loop through the fleet.
- **Backoff, snooze and sleep** hand the run back to the due set, so a waiting run holds no worker. A **wait step** registers for its event and hands the run back until its deadline; `Signal` keeps the signal (a run takes only signals sent after its enqueue) and makes every waiting run on that name and key ready in one script.
- **Run ends are delivered at least once.** Every script that ends a run records the end; a client with `OnRunFinished` observers claims the ends of its workflows under a lease, calls the observers and removes the entry, including ends that happened while no client ran (`CapRunFinished`).
- **A clean stop** (cancel the `Start` context, or `Drain`) hands every run back at once without counting the cut-off attempts (`CapShutdownRequeue`): the next worker runs them with the same `StepInfo.Attempt`. A crashed client hands back nothing; its runs wait for their leases.
- **Cancel, `ForceComplete` and `ForceFail`** end the run in one script and tell the holder to cancel its steps ([Force a run to an end](../jobs/manage-runs.md#force-a-run-to-an-end)).

## Schedules

`Schedule.Tenant` is stored with a run-time schedule and set on every run it fires (`ScheduleInfo.Tenant` reports it); a declared `Workflow.Cron` has none.

A schedule fires on a client that registered its workflow (its `ScheduleWhen` is code only such a client has), except a worker-only node (`WithoutScheduler`); `WithSchedulerOnly` makes a node that fires schedules and runs no step ([Nodes and health](../operations/nodes.md#scheduler-only-and-worker-only-processes)). Windows, `ScheduleWhen` and schedule pauses are decided per tick on the firing client and reported to `OnTick` observers. A tick fires once across any number of clients: the script that fires it moves the schedule's last catch-up time from the value the firing client read and creates the tick's runs in the same step. The default misfire policy fires the newest missed tick once; an explicit `workflow.Misfire` is honoured. `Client.Schedule` with a `Name` upserts by (Workflow, Name), and an unchanged expression, zone and misfire policy keep the next fire time, so an app can declare its schedules at every boot. `Workflow.Cron` entries get the stable IDs `cron:<workflow>:<index>`. Zones are evaluated across DST; a fixed-offset zone (`time.FixedZone`) is stored by its offset so every client rebuilds it.

The [overlap policy, trigger-now and last fire](../schedules/overlap.md) are decided in the firing script too, so a tick and a trigger on two clients cannot both start a run:

<!-- snippet: examples/redis-worker/main.go#trigger -->
```go
id, err := workers[0].Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "report", Cron: "0 2 * * *", Location: time.UTC, Overlap: workflow.OverlapSkip})
if err != nil {
	return err
}
run, err := workers[1].TriggerSchedule(ctx, id) // any client of the prefix
if err != nil {
	return err
}
```

## Web nodes and operations

`Enqueue` refuses a job carrying a label under a reserved prefix (`workflow.ReservedLabelPrefixes`) with an error matching `workflow.ErrInvalid`, through `workflow.CheckJobLabels`, unless the package that owns the label allowed it; labels the library writes on its own paths (a fired schedule, a chained job, a retry) are not checked.

`WithEnqueueOnly()` makes a client for web nodes: it enqueues and reads status without `Start` and runs nothing ([Deploy workers › Web nodes that only enqueue](../operations/deploy.md#web-nodes-that-only-enqueue)). `Register` is optional there.

<!-- snippet: examples/redis-worker/main.go#web -->
```go
web, err := redis.New(rdb, redis.WithPrefix(prefix), redis.WithEnqueueOnly())
if err != nil {
	return err
}
ref, err := web.Enqueue(ctx, workflow.Job{Workflow: "send-email", IdempotencyKey: "welcome:u-7"})
if err != nil {
	return err
}
again, err := web.Enqueue(ctx, workflow.Job{Workflow: "send-email", IdempotencyKey: "welcome:u-7"})
```

`Runs` lists runs by filter, including tenant, label prefix and due time (it walks an index by enqueue time and filters in Go); `Retry` re-opens a failed run from its failed step; `Delete` forgets a finished run; `CancelAll` cancels by filter; `Reschedule` moves a delayed run; `CancelStep` withdraws one step; `RunInfo.Errors` keeps each failed attempt (`WithErrorHistory`, 20 by default) with the node it ran on. A finished run's keys expire after `WithRetention` (24 hours by default), measured by Redis on wall time. Every operation of the [admin module](../admin/operations.md) works on this client.

## Shared stores

The client's Redis also holds state the middleware shares across processes. Each has a method over the client's prefix and a standalone constructor:

| Store | For | Method | Constructor |
|---|---|---|---|
| `UniqueStore` | `middleware.Unique` and `UniqueWith` locks, with the holder's run for `UniqueConfig.States` and `ReturnExisting` | `Client.UniqueStore()` | `redis.NewUniqueStore(rdb, prefix)` |
| `DebounceStore` | `middleware.Debounce` bursts | `Client.DebounceStore()` | `redis.NewDebounceStore(rdb, prefix)` |
| `JournalStore` | Step memoisation (`workflow.JournalStore`) | `Client.JournalStore()` | `redis.NewJournalStore(rdb, prefix, ttl)` |
| `BucketStore` | Leaky buckets (`workflow.BucketStore`) | `Client.BucketStore()` | `redis.NewBucketStore(rdb, prefix)` |

A lock's or entry's expiry is a Redis expiry, so it is wall time.

## Options

| Option | What it sets | Default |
|---|---|---|
| `WithPrefix(p)` | The key prefix, wrapped in a hash tag (`{p}:run:<id>`); letters, digits, `_`, `.` and `-`, at most 64 | `wf` |
| `WithClock(c)` | Due times, sleeps, backoff, step timeouts, cron fires | `clock.Real` |
| `WithLeaseClock(c)` | Leases and polling, apart from `WithClock` so a fake scheduling clock does not expire live leases | `clock.Real` |
| `WithLease(d)`, `WithMaxReclaims(n)` | How long a claim lives without renewal; how many expiries in a row before `ErrAbandoned` | 30s, 5 |
| `WithPollInterval(d)`, `WithWorkers(n)` | How often an idle worker looks for work; how many runs one client executes at once | 1s, 10 |
| `WithRetention(d)` | How long a finished run stays readable | 24h |
| `WithEnqueueOnly()`, `WithErrorHistory(n)` | A client that only enqueues; failed attempts kept per run | off, 20 |
| `WithQueues(qs...)` | Serve only these queues, each with its worker cap, priority and weight | every queue, one pool |
| `WithSchedulerOnly()`, `WithoutScheduler()` | A node that only fires schedules; a node that only runs steps | both |
| `WithNodeID(id)` | This node's identity in attempts, run info and `Nodes` | `workflow.DefaultNodeID()` |
| `WithPayloadLimit(n)` | Refuse a larger payload at `Enqueue` with a `*workflow.PayloadTooLargeError` | 512 MiB |
| `WithIDFunc(f)` | Run IDs; unique across the prefix, no `:` or `{`. The default sorts by creation time | time + random hex |
| `WithLogger`, `WithMiddleware`, `WithCronParser`, `WithJitterSource` | As on the other drivers | discard, none, `cron.Parse`, `math/rand/v2` |

## Try it

```sh
cd examples
REDIS_URL=redis://localhost:7012/0 go run ./redis-worker
```

```text
durable: true cron: true overlap skip: true
same key, same run: true
send-email succeeded after 2 attempts, sent 1 time(s), errors kept 1
schedule nightly: last run succeeded, next fire 02:00 UTC
```

The driver's own tests run the full conformance suite plus a crash test (one of three workers killed mid-step: every step runs once on the survivors), a lost-lease test (a stale holder's late result is fenced off) and a cron test (three clients, five ticks, five runs): `task redis:test` against the shared stack's Redis, or `WFTEST_REDIS_URL=redis://host:port/0 task redis:test` against another.

## Upgrading from an earlier version

This version changes how the driver behaves in four ways and changes its key layout:

- **The key layout changed.** Drain the prefix before upgrading a running deployment: stop enqueueing, let the workers finish (`Drain`), then deploy the new version. Runs, schedules and keys written by the old layout are not read by the new one; or start the new version on a new prefix (`WithPrefix`).
- **A schedule fires only on a client that registered its workflow**, no longer on any client of the prefix. A process that enqueues but never registers (start it `WithEnqueueOnly`) no longer fires anything.
- **A failed run's `RunEvent.Err` is a `*workflow.RunError`** naming the run, the workflow, the step and the attempt ([Errors › Run errors](../concepts/errors.md#run-errors)); its text is the step's message, as before.
- **A step timeout matches `workflow.ErrStepTimeout` and `workflow.ErrTimeout`** as well as `context.DeadlineExceeded` ([Errors › Timeouts](../concepts/errors.md#timeouts)).
- **`workflow.Discard` ends a run `StatusDiscarded`**, no longer failed, with no retries and no `OnFailure`. A policy with `RetryPolicy.DiscardOnExhaust` ends a run that ran out of retries the same way ([Retries](../jobs/retries.md#quiet-giving-up-discardonexhaust)).

## Pitfalls

- **Use `noeviction`.** With an LRU policy Redis may evict a run or a lease under memory pressure, silently.
- **Durability is Redis's.** With AOF fsync every second, a crash of Redis itself can lose up to a second of enqueues and state changes; without AOF, everything since the last snapshot.
- **Keep clocks close.** Times are read in Go and passed to the scripts, so a client whose clock runs ahead claims due work early.
- **One prefix is one Cluster slot.** Every key of a prefix carries the hash tag, so the scripts can touch them together; one prefix does not spread over several shards. The driver is proven on a single Redis node only.
- **A worker-only fleet needs a scheduler.** With every client started `WithoutScheduler`, no schedule fires; keep at least one client without it, or run a `WithSchedulerOnly` node.
- **Reserved labels are refused at `Enqueue`.** A label copied from a request under `ubgo.workflow/`, `workflow.batch.` or `otel.` is an error now, not a silent override.
- **A run's stored error is text.** A finished run's `RunEvent.Err` is rebuilt from the stored message inside its `RunError`, so `errors.Is` reaches the library's `ErrForced` but not your own sentinels; decide in the step, not in the observer.
- **Lifecycle events can be dropped.** They are a local, best-effort feed: past 4,096 queued events the client drops new ones and logs a warning ([Lifecycle events](../observability/lifecycle.md)).
- **The transaction is not yours.** Redis cannot join the app's SQL transaction; for an enqueue that must commit with your data, use the [outbox](../reliability/outbox.md) with this client as its target.

## Related

- [Choose a driver](../choosing/drivers.md), [Capabilities](../reference/capabilities.md)
- [The SQL driver](sqlstore.md), [The Hatchet driver](hatchet.md), [The in-memory driver](memory.md)
- [Deploy workers](../operations/deploy.md), [Nodes and health](../operations/nodes.md), [Hybrid placement](hybrid.md)
