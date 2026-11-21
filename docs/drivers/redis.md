# The Redis driver

The `redis` module runs workflows on Redis through go-redis v9. Every run, schedule, lease and idempotency key is a Redis key, so a restart or a dead worker loses nothing, and any number of clients on any number of machines share one Redis. Pick it when your app already runs Redis and you want durable jobs, workflows and cron without a SQL database or an engine. It covers what the [SQL driver](sqlstore.md) covers, except closures and expression keys; pick sqlstore when an enqueue must commit inside your own SQL transaction.

## Before you start

- A Redis that does not evict keys (`maxmemory-policy noeviction`): an evicted run or lease is lost work, not a cache miss. `noeviction` is Redis's default, so a stock server, such as the one in the repository's `compose.yml` (see [Try it](#try-it)), is already set up that way.
- The module: `go get github.com/ubgo/workflow/redis@latest` (Go 1.24 or later). It is separate from the core, so an app without Redis never downloads go-redis.
- The examples are [`examples/redis-worker`](../../examples/redis-worker) (a web node, two workers, a retried job and a triggered schedule) and [`examples/redis-parity`](../../examples/redis-parity) (every option and every feature the SQL driver has too). Run them with `go run ./redis-worker` and `go run ./redis-parity` from `examples/`; set `REDIS_URL` for the live part.

## Create the client

The package is named `redis`, like go-redis itself, so import one of them under another name:

<!-- snippet: examples/redis-worker/main.go#client -->
```go
rdb := goredis.NewClient(opts) // the package is named redis too, hence goredis
defer func() { err = errors.Join(err, rdb.Close()) }()
prefix := "example-" + strconv.FormatInt(time.Now().UnixNano(), prefixBase)
// newWorker builds one worker client on the shared prefix.
newWorker := func() (*redis.Client, error) {
	return redis.New(rdb,
		redis.WithPrefix(prefix),                    // clients that share a prefix share every run and schedule
		redis.WithPollInterval(50*time.Millisecond), // short for the demo; how often a worker looks for due runs
	)
}
```

Then `Register` your workflows and `Start`, as on every driver; nothing runs before `Start`, and cancelling its context stops the client. Clients that share a prefix (`WithPrefix`, default `wf`) share every run and schedule; give each app, and each test, its own.

## Set up a worker

Every other option has a default that suits production; set them when a test needs a fake clock, a fleet needs shorter leases, or run IDs must follow your own scheme:

<!-- snippet: examples/redis-parity/main.go#options -->
```go
var ids atomic.Int64 // shared by every client of this process
logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
// newWorker builds one client of the prefix with every option set.
newWorker := func(node string, extra ...redis.Option) (*redis.Client, error) {
	opts := []redis.Option{
		redis.WithPrefix(prefix),           // clients of one prefix share runs and schedules
		redis.WithClock(clock.Real{}),      // due times, sleeps, backoff, cron fires
		redis.WithLeaseClock(clock.Real{}), // leases and polling: keep this one real
		redis.WithLease(leaseFor),          // a claim's life without renewal (default 30s)
		redis.WithMaxReclaims(maxReclaims), // expiries in a row before ErrAbandoned (default 5)
		redis.WithPollInterval(pollEvery),  // fallback when no wake-up message came (default 1s)
		redis.WithWorkers(4),               // runs this client executes at once (default 10)
		redis.WithIDFunc(func() string { // unique across the prefix, no ':' or '{'
			return fmt.Sprintf("%s-%06d", prefix, ids.Add(1))
		}),
		redis.WithLogger(logger),                              // a failed renewal, a lost hand-back
		redis.WithMiddleware(),                                // step middleware, outermost first
		redis.WithCronParser(cron.Parse),                      // the same parser on every client
		redis.WithJitterSource(func() float64 { return 0.5 }), // pin Backoff.Jitter's draw
		redis.WithNodeID(node),
	}
	return redis.New(rdb, append(opts, extra...)...)
}
```

- **Two clocks.** `WithClock` drives everything that is part of the workflow: due times, sleeps, backoff, step timeouts, cron fires. `WithLeaseClock` drives leases and polling, which are about a worker being alive and so are wall time. A test that moves a fake `WithClock` by an hour must leave `WithLeaseClock` real, or every running step looks abandoned.
- **Leases.** A run is claimed whole under a lease of `WithLease` (30 seconds by default), renewed every third of it. A worker that dies stops renewing, and its runs are claimed again once the lease expires, so `WithLease` is also the longest a dead worker's run waits. A run whose lease expires `WithMaxReclaims` times in a row (default 5) with no step finishing fails with `redis.ErrAbandoned`, so a step that crashes every worker it lands on cannot loop through the fleet.
- **IDs.** `WithIDFunc` replaces the run IDs. They must be unique across every client of the prefix and contain no `:` or `{`. The default is the creation time in milliseconds as 13 hex digits followed by 16 random ones, so IDs sort by time and equal-priority runs start oldest first; IDs in another order change only that order.
- **The rest.** `WithPollInterval` is the fallback when a wake-up message was missed (one second); `WithWorkers` caps how many runs one client executes at once (10); `WithCronParser` must be the same on every client of a prefix, since any of them may fire any schedule; `WithJitterSource` pins `Backoff.Jitter`'s draw in tests; `WithLogger` receives what no caller can see (a failed renewal, a run that could not be handed back); `WithMiddleware` wraps every step and `OnFailure` handler, the first outermost.

## What it can do and what it refuses

It declares, each proven by the conformance suite against a real Redis: `CapDurable`, `CapDelayedStart`, `CapDurableSleep`, `CapWaitForEvent`, `CapIdempotencyKey`, `CapConcurrencyKey` (every strategy) and `CapKeyFIFO`, `CapRateLimit` (sliding windows), `CapCron`, `CapCancel`, `CapRunFinished`, `CapRetryJitter`, `CapRetryDelay`, `CapQueue` and `CapQueueOverride`, `CapJobRetry`, `CapRetryUntil`, `CapRetryMaxErrors`, `CapDiscard`, `CapDiscardExhausted` and `CapRunStates`, `CapSnooze`, `CapHeartbeat`, `CapRunTimeout`, `CapTenant`, `CapNodeIdentity`, `CapResult`, `CapDueQuery`, the three misfire policies, `CapCronZone`, `CapCronSeconds`, `CapScheduleActive`, `CapScheduleKeep`, `CapTickTime`, `CapScheduleWhen`, `CapScheduleWindow`, `CapTickEvents`, `CapSchedulePause`, `CapRunQuery`, `CapLabelPrefix`, `CapRetryRun`, `CapDeleteRun`, `CapCancelAll`, `CapPauseWorkflow`, `CapPauseQueue`, `CapChain`, `CapDrain`, `CapDrainReport`, `CapScheduleList`, `CapScheduleTrigger`, `CapScheduleHistory`, `CapOverlapSkip`, `CapEnqueueOnly`, `CapErrorHistory`, `CapShutdownRequeue`, `CapForceRun`, `CapWorkerScale`, `CapRoundRobin` and `CapKeyWeights` (round-robin keys take turns in proportion to `Concurrency.Weight`; see [Share the workers fairly between keys](../jobs/concurrency.md#share-the-workers-fairly-between-keys)).

The client also implements the interfaces that have no capability bit: `RunRescheduler` (`Reschedule`), `StepCanceller` (`CancelStep`), `HealthChecker` (`Health`), `NodeLister` (`Nodes`), `LifecycleNotifier` (`OnLifecycle`) and `PayloadLimiter`, whose limit is 512 MiB, the largest value Redis stores, lowered with `WithPayloadLimit`.

`CapWorkerScale` means `QueueLoads` and `SetWorkers` over the `WithQueues` list ([Worker balancing](../operations/balancing.md)). It does not declare `CapClosure`, `CapQueueLimitGlobal` or `CapSlotCost` (a worker claims a whole run, so a workflow with `Step.Slots` above one is refused at `Start`), and it refuses three things with an `UnsupportedError` at `Start`, `Enqueue` or `Schedule` instead of ignoring them:

- **Closures** (`CapClosure`): a Go closure cannot be stored in Redis. Register a workflow, or use [functions by name](../jobs/functions.md).
- **A queue limit across clients** (`CapQueueLimitGlobal`): a run is claimed whole, so the driver keeps no count of a queue's in-flight step attempts across clients, and a non-zero `QueueConfig.Limit` is refused at `Start`. `QueueConfig.Workers` still caps one client; see [Named queues](../jobs/queues.md#cap-a-queue-across-every-worker).
- **Expression keys** (`KeyExpr`): concurrency, rate and wait keys are Go functions (`workflow.KeyFunc`) evaluated in the client; an engine-language expression is refused at `Start`.

An app can ask the client before it relies on a feature. Both refusals happen in `Start` before it touches Redis, so they need no server:

<!-- snippet: examples/redis-parity/main.go#declares -->
```go
c, err := redis.New(rdb)
if err != nil {
	return err
}
for _, cp := range []workflow.Caps{
	workflow.CapWaitForEvent, workflow.CapConcurrencyKey, workflow.CapRateLimit,
	workflow.CapQueue, workflow.CapPauseWorkflow, workflow.CapPauseQueue,
	workflow.CapRunFinished, workflow.CapChain, workflow.CapScheduleWhen,
	workflow.CapShutdownRequeue,
	workflow.CapClosure, workflow.CapQueueLimitGlobal, // two it does not declare; CapSlotCost is the third
} {
	fmt.Fprintf(w, "%v: %v\n", cp, c.Capabilities().Has(cp))
}
```

<!-- snippet: examples/redis-parity/main.go#refuses -->
```go
// A key in an engine language: the driver evaluates keys in Go.
exprKey, err := redis.New(rdb)
if err != nil {
	return err
}
if err := exprKey.Register(workflow.Workflow{
	Name:        "per-tenant",
	Concurrency: &workflow.Concurrency{Key: workflow.KeyExpr("input.tenant"), MaxRuns: 1},
	Steps:       []workflow.Step{{Name: "only", Run: noop}},
}); err != nil {
	return err
}
err = exprKey.Start(ctx)
fmt.Fprintln(w, "KeyExpr at Start: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))

// A cap across every client: a run is claimed whole, so no count of a
// queue's step attempts across clients is kept.
limited, err := redis.New(rdb, redis.WithQueues(workflow.QueueConfig{Name: "imports", Workers: 2, Limit: 5})) // Limit is the refused field
if err != nil {
	return err
}
err = limited.Start(ctx)
fmt.Fprintln(w, "QueueConfig.Limit at Start: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))
```

```text
WaitForEvent: true
ConcurrencyKey: true
RateLimit: true
Queue: true
PauseWorkflow: true
PauseQueue: true
RunFinished: true
Chain: true
ScheduleWhen: true
ShutdownRequeue: true
Closure: false
QueueLimitGlobal: false
KeyExpr at Start: ErrUnsupported = true
QueueConfig.Limit at Start: ErrUnsupported = true
```

Each refusal is a `*workflow.UnsupportedError` that matches `workflow.ErrUnsupported` and names the field.

What the capabilities mean is in [Capabilities](../reference/capabilities.md); how each driver compares is in [Choose a driver](../choosing/drivers.md).

## Use the features the SQL driver has

The workflows below run unchanged on the SQL and memory drivers. On Redis each feature's state lives in Redis and every limit is decided in a Lua script, so it holds across every client of the prefix:

<!-- snippet: examples/redis-parity/main.go#workflows -->
```go
approval := workflow.Workflow{Name: "approval", Steps: []workflow.Step{
	{Name: "await", Wait: &workflow.WaitSpec{
		Event:   "approved",
		KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["order"] }),
	}},
	{Name: "ship", After: []string{"await"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		approved <- string(in) // the wait step's output is the signal's payload
		return nil, nil
	}},
}}
perCustomer := workflow.Workflow{
	Name: "invoice",
	Concurrency: &workflow.Concurrency{ // one run per customer at a time, across clients
		Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["customer"] }),
		MaxRuns: 1,
	},
	Steps: []workflow.Step{{Name: "bill", Run: func(context.Context, []byte) ([]byte, error) {
		n := running.Add(1)
		for {
			m := most.Load()
			if n <= m || most.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(stepWork)
		running.Add(-1)
		return nil, nil
	}}},
}
sms := workflow.Workflow{
	Name: "sms",
	RateLimits: []workflow.RateLimit{{ // a sliding window kept in Redis
		Key:   workflow.KeyFunc(func(workflow.Job) string { return "provider" }),
		Limit: smsPerHour,
		Per:   time.Hour,
	}},
	Steps: []workflow.Step{{Name: "send", Run: noop}},
}
extract := workflow.Workflow{Name: "extract", Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) {
	record("extract")
	return nil, nil
}}}}
load := workflow.Workflow{Name: "load", Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) {
	record("load")
	return nil, nil
}}}}
heartbeat := workflow.Workflow{
	Name: "heartbeat",
	// Decided per tick on the firing client: skip the first tick only.
	ScheduleWhen: func(context.Context, workflow.Tick) bool { return whenCalls.Add(1) > 1 },
	Steps:        []workflow.Step{{Name: "beat", Run: noop}},
}
```

### Step 1: observe run ends and ticks

Register observers before `Start`; afterwards `OnRunFinished` and `OnTick` return `workflow.ErrAlreadyStarted`. Run ends are recorded by the script that ends the run and delivered at least once by a client with observers that registered the workflow, including ends that happened while no client ran:

<!-- snippet: examples/redis-parity/main.go#observers -->
```go
err = worker.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { // before Start
	if ev.Workflow == "approval" {
		observed <- ev
	}
})
if err != nil {
	return err
}
err = worker.OnTick(func(_ context.Context, t workflow.Tick) {
	if t.Workflow == "heartbeat" {
		select {
		case ticks <- t:
		default:
		}
	}
})
```

### Step 2: wait for a signal

A wait step registers for its event and hands the run back, so a waiting run holds no worker. `Signal` keeps the signal, and a run takes only signals sent after its enqueue, so signalling right after `Enqueue` is safe:

<!-- snippet: examples/redis-parity/main.go#signal -->
```go
ref, err := worker.Enqueue(ctx, workflow.Job{Workflow: "approval", Labels: map[string]string{"order": "o-7"}})
if err != nil {
	return err
}
if err := worker.Signal(ctx, workflow.Event{Name: "approved", Key: "o-7", Payload: []byte(`"by ops"`)}); err != nil {
	return err
}
```

The step after the wait gets the signal's payload as its input (`"by ops"` here).

### Step 3: hold a key to one run at a time, and limit a rate

The `invoice` workflow's concurrency key allows one run per customer; three runs of one customer, served by two clients, ran one at a time. The `sms` workflow's rate limit allows two runs per hour in a sliding window stored in Redis; the third waits, `queued`, until the window has room. A run of a workflow with a concurrency or rate policy first waits in a pending list, and a client that registered the workflow admits it in one script against the stored slots and windows.

### Step 4: give a queue its own worker, and pause a workflow

A client started `WithQueues` claims only runs of those queues, the higher `QueueConfig.Priority` first:

<!-- snippet: examples/redis-parity/main.go#queues -->
```go
reports, err := newWorker("reports-node", redis.WithQueues(workflow.QueueConfig{Name: "reports", Workers: 1}))
```

A pause is stored in Redis, so every client obeys it and it survives a restart. Steps already running finish; nothing new is claimed until `Resume`:

<!-- snippet: examples/redis-parity/main.go#pause -->
```go
// Any client of the prefix pauses; every client obeys it.
if err := worker.Pause(ctx, workflow.PauseWorkflow("render")); err != nil {
	return err
}
// A worker client enqueues only what it registered (or use WithEnqueueOnly).
held, err := reports.Enqueue(ctx, workflow.Job{Workflow: "render"})
if err != nil {
	return err
}
```

### Step 5: chain jobs and add a schedule condition

A chain is stored with the run and handed on in the script that records the run's success, so a crash between the two jobs loses nothing:

<!-- snippet: examples/redis-parity/main.go#chain -->
```go
first, err := worker.Enqueue(ctx, workflow.Job{
	Workflow: "extract",
	Chain:    []workflow.Job{{Workflow: "load"}}, // starts once extract succeeded, stored with the run
})
```

`Workflow.ScheduleWhen` is Go code, so it is decided on the client that fires the tick, which must have registered the workflow; the tick is reported to `OnTick` observers as `skipped` with the reason `when`:

<!-- snippet: examples/redis-parity/main.go#condition -->
```go
id, err := worker.Schedule(ctx, workflow.Schedule{Name: "beat", Workflow: "heartbeat", Cron: "* * * * * *", Seconds: true, Location: time.UTC})
if err != nil {
	return err
}
```

What the program prints for these steps:

```text
approval: the step after the wait got "by ops"
run-finished observer saw approval: succeeded
invoice: 3 runs of customer acme on two clients, at most 1 at a time
sms: 2 succeeded, the next one is queued (2 per hour)
render while paused: queued, paused targets 1
render after resume: succeeded on queue reports by [reports-node]
chain: extract then load
heartbeat: a tick skipped (reason "when"), a later one fired
```

## How work moves

- **Enqueue** writes a run hash and puts its ID in the ready set of its queue and workflow (ordered by priority, then by ID, which starts with the creation time), or in the due set for a future `RunAt`. An idempotency key is checked and set in the same Lua script, so two clients racing on one key make one run; the key is freed when the run ends. A published message wakes idle workers at once; polling (`WithPollInterval`, one second) is the fallback.
- **Limits are decided in one script.** A run of a workflow with a concurrency or rate policy, or of a workflow the enqueuing client did not register (an enqueue-only client, a chained job, a schedule fired elsewhere), first waits in a pending list; a client that registered the workflow evaluates its queue and keys in Go and admits it in one script against the stored concurrency slots and sliding rate windows, so the limits hold across every client.
- **A run is claimed whole** by one client, under a lease (`WithLease`, 30 seconds) renewed every third of it. A client claims only the workflows it registered and, with `WithQueues`, only the queues it serves, the higher `QueueConfig.Priority` first (equal priorities share by `QueueConfig.Weight`). A pause (everything, a workflow or a queue) is stored in Redis, checked by every claim, and survives a restart. The holder runs every step whose parents are done, in parallel.
- **Writes are fenced.** Every write compares the holder's claim token with the run's owner, so a worker whose lease expired and whose run another client took writes nothing: each step outcome is recorded once per lease. A step whose result was lost with its worker runs again (at least once).
- **A dead worker's runs** are claimed again when their leases expire. A run whose lease expires more than `WithMaxReclaims` times in a row (default 5) with no step finishing fails with `redis.ErrAbandoned`, so a step that kills every worker cannot loop through the fleet.
- **Backoff, snooze and sleep** hand the run back to the due set, so a waiting run holds no worker. A **wait step** registers for its event and hands the run back until its deadline; `Signal` keeps the signal (a run takes only signals sent after its enqueue) and makes every waiting run on that name and key ready in one script.
- **Run ends are delivered at least once.** Every script that ends a run records the end; a client with `OnRunFinished` observers claims the ends of its workflows under a lease, calls the observers and removes the entry, including ends that happened while no client ran (`CapRunFinished`).
- **A clean stop** (cancel the `Start` context, or `Drain`) hands every run back at once without counting the cut-off attempts (`CapShutdownRequeue`): the next worker runs them with the same `StepInfo.Attempt`. A crashed client hands back nothing; its runs wait for their leases.

A clean stop during a deploy, from the old worker to the new one:

<!-- snippet: examples/redis-parity/main.go#clean-stop -->
```go
first, err := newWorker("deploy-old")
if err != nil {
	return err
}
if err := first.Register(long); err != nil {
	return err
}
firstCtx, stopFirst := context.WithCancel(ctx)
if err := first.Start(firstCtx); err != nil {
	stopFirst()
	return err
}
ref, err := first.Enqueue(ctx, workflow.Job{Workflow: "long"})
if err != nil {
	stopFirst()
	return err
}
<-started
stopFirst() // a clean stop: the run goes back at once, the attempt not counted
first.Stop()

next, err := newWorker("deploy-new") // the new version of the worker
if err != nil {
	return err
}
if err := next.Register(long); err != nil {
	return err
}
```

The step saw `StepInfo.Attempt` 1 on both workers (`attempts [1 1]`): the cut-off attempt was not spent.
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

`Enqueue` refuses a job carrying a label under a reserved prefix ([`workflow.ReservedLabelPrefixes`](ds:block?id=reservedlabelprefixes-s6pnbs6s)) with an error matching `workflow.ErrInvalid`, through [`workflow.CheckJobLabels`](ds:block?id=checkjoblabels-p6m28b9h), unless the package that owns the label allowed it; labels the library writes on its own paths (a fired schedule, a chained job, a retry) are not checked.

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

`ResetRun` re-opens a failed run from a chosen step, keeping the outputs of the steps before it; `ResetRunWith(ctx, ref, step, workflow.ResetOptions{Succeeded: true})` does the same to a succeeded run in place, in the one script that checks its status, and stamps `workflow.LabelRedo` on it. Its chain was handed on when it succeeded and is not handed on again; a succeeded run whose chain handoff failed is refused with `workflow.ErrWrongState`. `ForkRun(ctx, ref, step, workflow.ForkOptions{})` starts a new run from a finished one at `step`, on the original's workflow version, queue, priority and tenant, with the outputs of the steps it keeps, `workflow.ForkLabels`, no chain and no idempotency key unless `ForkOptions.IdempotencyKey` gives one; the original is untouched. Both need the client to have the workflow registered, to know which steps are downstream.

## Shared stores

The client's Redis also holds state the middleware shares across processes. Each has a method over the client's prefix and a standalone constructor:

| Store | For | Method | Constructor |
|---|---|---|---|
| `UniqueStore` | `middleware.Unique` and `UniqueWith` locks, with the holder's run for `UniqueConfig.States` and `ReturnExisting` | `Client.UniqueStore()` | `redis.NewUniqueStore(rdb, prefix)` |
| `DebounceStore` | `middleware.Debounce` bursts | `Client.DebounceStore()` | `redis.NewDebounceStore(rdb, prefix)` |
| `JournalStore` | Step memoisation (`workflow.JournalStore`) | `Client.JournalStore()` | `redis.NewJournalStore(rdb, prefix, ttl)` |
| `BucketStore` | Leaky buckets (`workflow.BucketStore`) | `Client.BucketStore()` | `redis.NewBucketStore(rdb, prefix)` |
| `DataStore` | Shared data by scope (`workflow.DataStore`: `SetData`, `GetData`, `UpdateData`) | `Client.DataStore()` | `redis.NewDataStore(rdb, prefix)` |
| `PromiseStore` | Promises settled once (`workflow.PromiseStore`) | `Client.PromiseStore()` | `redis.NewPromiseStore(rdb, prefix, ttl)` |
| `Inbox` | A consumer's message deduplication (`workflow.Inbox`) | `Client.Inbox()` | `redis.NewInbox(rdb, prefix)` |
| `AggregateStore` | `middleware.Aggregate`'s pending items | `Client.AggregateStore()` | `redis.NewAggregateStore(rdb, prefix)` |

A lock's or entry's expiry is a Redis expiry, so it is wall time.

The constructors need no driver client: a process that only shares state (a web node that reads `SetData` values, a consumer with an inbox) builds the store over the same Redis and prefix as the workers. A prefix that breaks the rules of `WithPrefix` is refused with `workflow.ErrInvalid`:

<!-- snippet: examples/redis-parity/main.go#stores -->
```go
// keep is how long a settled promise or a step memo is kept.
const keep = 24 * time.Hour
data, err := redis.NewDataStore(rdb, prefix) // workflow.DataStore: SetData, GetData, UpdateData
if err != nil {
	return err
}
promises, err := redis.NewPromiseStore(rdb, prefix, keep) // workflow.PromiseStore
if err != nil {
	return err
}
journal, err := redis.NewJournalStore(rdb, prefix, keep) // workflow.JournalStore, step memos
if err != nil {
	return err
}
inbox, err := redis.NewInbox(rdb, prefix) // workflow.Inbox: a consumer's dedup of messages
if err != nil {
	return err
}
buckets, err := redis.NewBucketStore(rdb, prefix) // workflow.BucketStore: leaky buckets
if err != nil {
	return err
}
aggregates, err := redis.NewAggregateStore(rdb, prefix) // middleware.Aggregate's pending items
if err != nil {
	return err
}
```

```text
data: plan="pro" version 1
promise: first settle true, second settle false, value "paid"
journal: fetch=42 found true
```

The second `Settle` of a promise keeps the first value and reports `first` false, on every process of the prefix.

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

The repository's `compose.yml` has a throwaway Redis on `127.0.0.1:36379` (the one `task redis:test` uses); start it from the repository root with `docker compose --profile redis up -d --wait` and remove it afterwards with `docker compose --profile redis down -v`.

```sh
cd examples
REDIS_URL=redis://127.0.0.1:36379/0 go run ./redis-worker
```

```text
durable: true cron: true overlap skip: true
same key, same run: true
send-email succeeded after 2 attempts, sent 1 time(s), errors kept 1
schedule nightly: last run succeeded, next fire 02:00 UTC
```

The parity program prints what the driver declares without a server, and runs every feature above with one (about ten seconds, most of it the schedule waiting for a second tick):

```sh
cd examples
REDIS_URL=redis://127.0.0.1:36379/0 go run ./redis-parity
```

```text
WaitForEvent: true
ConcurrencyKey: true
RateLimit: true
Queue: true
PauseWorkflow: true
PauseQueue: true
RunFinished: true
Chain: true
ScheduleWhen: true
ShutdownRequeue: true
Closure: false
QueueLimitGlobal: false
KeyExpr at Start: ErrUnsupported = true
QueueConfig.Limit at Start: ErrUnsupported = true
approval: the step after the wait got "by ops"
run-finished observer saw approval: succeeded
invoice: 3 runs of customer acme on two clients, at most 1 at a time
sms: 2 succeeded, the next one is queued (2 per hour)
render while paused: queued, paused targets 1
render after resume: succeeded on queue reports by [reports-node]
chain: extract then load
heartbeat: a tick skipped (reason "when"), a later one fired
long: cut short on deploy-old, finished on deploy-new, attempts [1 1]
data: plan="pro" version 1
promise: first settle true, second settle false, value "paid"
journal: fetch=42 found true
```

Without `REDIS_URL` it stops after the refusals with `REDIS_URL is not set: the live run is skipped`.

The driver's own tests run the full conformance suite plus a crash test (one of three workers killed mid-step: every step runs once on the survivors), a lost-lease test (a stale holder's late result is fenced off) and a cron test (three clients, five ticks, five runs): `task redis:test` against the shared stack's Redis, or `WFTEST_REDIS_URL=redis://host:port/0 task redis:test` against another.

## Upgrading from an earlier version

This version changes how the driver behaves in four ways and changes its key layout:

- **The key layout changed.** Drain the prefix before upgrading a running deployment: stop enqueueing, let the workers finish (`Drain`), then deploy the new version. Runs, schedules and keys written by the old layout are not read by the new one; or start the new version on a new prefix (`WithPrefix`).
- **A schedule fires only on a client that registered its workflow**, no longer on any client of the prefix. A process that enqueues but never registers (start it `WithEnqueueOnly`) no longer fires anything.
- **A failed run's `RunEvent.Err` is a `*workflow.RunError`** naming the run, the workflow, the step and the attempt ([Errors › Run errors](../concepts/errors.md#run-errors)); its text is the step's message, as before.
- **A step timeout matches `workflow.ErrStepTimeout` and `workflow.ErrTimeout`** as well as `context.DeadlineExceeded` ([Errors › Timeouts](../concepts/errors.md#timeouts)).
- **`workflow.Discard` ends a run `StatusDiscarded`**, no longer failed, with no retries and no `OnFailure`. A policy with `RetryPolicy.DiscardOnExhaust` ends a run that ran out of retries the same way ([Retries](../jobs/retries.md#quiet-giving-up-discardonexhaust)).

## Record run history

A [run recorder](../observability/run-recorder.md) applied with `workflow.Use(client, rec.Plugin())` records every run of this driver. There is no Redis store for the recorder: keep the history in `sqlrecorder`, in [`recorder.NewMemoryStore()`](ds:block?id=rec-newmemorystore-5yrbgb94) for one process, or in a store of your own.

- **Every end is recorded.** The driver declares `CapRunFinished`: a client with observers claims each run end of its workflows under a lease and delivers it at least once, on one client, including ends that happened while no client was running. The recorder's upserts absorb the repeats.
- **Records outlive the keys.** A finished run's keys expire after `WithRetention` (24 hours by default); the record stays until the recorder's own [retention](../observability/run-recorder.md#retention) removes it. An unfinished record whose run the driver no longer knows is flagged stale by the Reconciler, never cancelled.
- **Web nodes take `EnqueuePlugin`.** A client built with `WithEnqueueOnly()` starts no worker, scheduler or subscription, so it delivers no run ends; apply [`rec.EnqueuePlugin()`](ds:block?id=rec-recorder-enqueueplugin-rn5k8npv) there and [`rec.Plugin()`](ds:block?id=rec-recorder-plugin-u7hfc2yc) on the workers over a store both reach ([Web nodes that only enqueue](../observability/run-recorder.md#web-nodes-that-only-enqueue)).
- **The Reconciler can backfill.** The driver declares `CapRunQuery`, so a pass records runs enqueued past the recorder.

The recorder's end-to-end suite runs its driver cases on this driver, recording into [`recorder.MemoryStore`](ds:block?id=rec-memorystore-smz35v44); the web-node split is proven on the SQL and Hatchet drivers.

## Pitfalls

- **Use `noeviction`.** With an LRU policy Redis may evict a run or a lease under memory pressure, silently.
- **Durability is Redis's.** With AOF fsync every second, a crash of Redis itself can lose up to a second of enqueues and state changes; without AOF, everything since the last snapshot.
- **Keep clocks close.** Times are read in Go and passed to the scripts, so a client whose clock runs ahead claims due work early.
- **One prefix is one Cluster slot.** Every key of a prefix carries the hash tag, so the scripts can touch them together; one prefix does not spread over several shards. The driver is proven on a single Redis node only.
- **A worker-only fleet needs a scheduler.** With every client started `WithoutScheduler`, no schedule fires; keep at least one client without it, or run a `WithSchedulerOnly` node.
- **Reserved labels are refused at `Enqueue`.** A label copied from a request under `ubgo.workflow/`, `workflow.batch.` or `otel.` is an error now, not a silent override.
- **A run's stored error is text.** A finished run's `RunEvent.Err` is rebuilt from the stored message inside its `RunError`, so `errors.Is` reaches the library's `ErrForced` but not your own sentinels; decide in the step, not in the observer.
- **Lifecycle events can be dropped.** They are a local, best-effort feed: past 4,096 queued events the client drops new ones and logs a warning ([Lifecycle events](../observability/lifecycle.md)).
- **A worker client enqueues only what it registered.** `Enqueue` on a client that did not `Register` the workflow returns `workflow.ErrNotFound`, unless the client was built `WithEnqueueOnly()`. Register the workflow there too, or enqueue from a web-node client.
- **`ErrAbandoned` is matched by text after the fact.** The run fails with an error wrapping `redis.ErrAbandoned`, but the stored error is rebuilt from its message, so `errors.Is(ev.Err, redis.ErrAbandoned)` in an observer, or on `RunInfo`, is false; look for `redis.ErrAbandoned.Error()` in `RunInfo.Error` instead.
- **A fake `WithClock` with a real lease clock.** Moving the fake clock fires due times and cron ticks, never lease expiries; a dead worker's run is taken again only after `WithLease` of real time, and only when its process died: a clean stop hands its runs back at once.
- **The transaction is not yours.** Redis cannot join the app's SQL transaction; for an enqueue that must commit with your data, use the [outbox](../reliability/outbox.md) with this client as its target.

## Related

- [Choose a driver](../choosing/drivers.md), [Capabilities](../reference/capabilities.md)
- [The SQL driver](sqlstore.md), [The Hatchet driver](hatchet.md), [The in-memory driver](memory.md)
- [Deploy workers](../operations/deploy.md), [Nodes and health](../operations/nodes.md), [Hybrid placement](hybrid.md)
- [Record run history](../observability/run-recorder.md)
- Examples: [redis-worker](../../examples/redis-worker/) and [redis-parity](../../examples/redis-parity/)
