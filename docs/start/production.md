# Going to production

What changes between the memory driver on a laptop and a service whose jobs must not be lost: a durable driver, migrations run by the deploy, handlers that tolerate running twice, a clean shutdown, and the knobs that keep tables and logs in shape. Each item links to the guide that explains it. The library starts nothing in the background on its own and never creates tables at `Start`, so every item here is a decision you make in code.

## Before you start

- Your workflows run on the memory driver ([Your first workflow](first-workflow.md)), and your app tests pass against `workflowtest.Fake` ([Test app code with the Fake](../testing/fake.md#test-app-code-with-the-fake)).
- You have picked a durable backend: [Choose a driver](../choosing/drivers.md).

## Checklist

| # | Item | Why | Guide |
|---|---|---|---|
| 1 | Run on a driver that declares `CapDurable`: `sqlstore` (SQLite, Postgres, MySQL 8), `redis` or `hatchet` | The memory driver loses every queued, waiting and sleeping run when the process exits; it declares every capability but `CapDurable`, `CapEnqueueOnly` and `CapShutdownRequeue` | [Choose a driver](../choosing/drivers.md#memory-sql-redis-or-hatchet) |
| 2 | Run `Client.Migrate` from the deploy, before the new code starts | `Start` never migrates. `Migrate` is versioned and safe to call from several processes at once, and it refuses a database a newer release already migrated (`ErrSchemaTooNew`) | [Run the migrations](../operations/migrations.md#run-the-migrations) |
| 3 | Make every handler idempotent | Delivery is at least once: on `sqlstore` a step whose worker died is run again by another worker once its lease expires (`WithLease`, 30s by default); the outbox relays at least once; run-finished observers may see an event twice | [Leases and crashed workers](../drivers/sqlstore.md#leases-and-crashed-workers) |
| 4 | Put an idempotency key on jobs a caller may send twice (a webhook, a retried HTTP request) | One run per key while a run holding it is unfinished; the second `Enqueue` returns the first run's `RunRef` | [Idempotency keys](../jobs/idempotency.md#idempotency-keys) |
| 5 | Give every step that calls the network a retry policy and a timeout | The zero `RetryPolicy` means no retries and the zero `Timeout` means none; on Hatchet the driver applies `StepTimeout` (24h by default) because the engine always has one | [Retry policy](../jobs/retries.md#retry-policy), [Timeouts](../workflows/steps.md#timeouts) |
| 6 | Mark errors that retrying cannot fix with `workflow.Permanent` | A bad payload retried five times is five failures and a delayed alert | [Errors that must not retry](../jobs/retries.md#errors-that-must-not-retry) |
| 7 | Stop cleanly: cancel the context passed to `Start` on SIGTERM, then call `Stop` where the driver has one | Cancelling the `Start` context stops new steps and cancels running steps' contexts; `memory.Client.Stop` and `sqlstore.Client.Stop` also wait for every goroutine they started. `sqlstore` leaves runs in the database for the next process; memory marks unfinished runs cancelled. To let running steps finish first, drain with a grace period (`workflow.DrainTimeout`, or `ShutdownHookReport` in a shutdown phase) and read the report of what was cut off | [Shut down cleanly](../operations/deploy.md#shut-down-cleanly) |
| 8 | Write enqueues that must match your own data in the same transaction | A job enqueued after a commit is lost if the process dies in between; one enqueued before the commit runs for data that was rolled back | [Choose how to enqueue](../choosing/methods.md#enqueue-enqueuetx-or-the-outbox) |
| 9 | Declare every cron in an explicit time zone | `Schedule.Location` is required and `time.Local` is refused on `Workflow.CronLocation`; Hatchet runs crons in UTC only and refuses any other zone | [Schedules in a time zone](../schedules/time-zones.md#schedules-in-a-time-zone) |
| 10 | Pick a misfire policy for crons that must not run late, or must catch up | The default differs per driver; an explicit `Misfire` is honoured or refused, never ignored | [Misfire policies](../schedules/misfire.md#misfire-policies) |
| 11 | Turn on retention of finished runs (`sqlstore.WithRetention`) | Off by default: the SQL driver keeps every finished run until you choose otherwise | [Keep finished runs for a while](../operations/retention.md#keep-finished-runs-for-a-while) |
| 12 | Log and trace steps: `middleware.Logging`, `middleware.Recover`, the `otel` module; pass `WithLogger` to drivers | Drivers discard their background errors unless given a logger; a panicking handler should fail its step, not the worker | [Log every step](../observability/logging.md#log-every-step), [Add tracing and metrics](../observability/opentelemetry.md#add-tracing-and-metrics) |
| 13 | Encrypt payloads that carry personal data, and keep big ones out of the queue | Payloads sit in the database or the engine for as long as the run is kept | [Encrypt payloads](../middleware/codecs.md#encrypt-payloads), [Claim check](../reliability/claim-check.md#move-big-payloads-out-of-the-queue) |
| 14 | Prove your workflows start on the production driver, in a test | `Start` refuses a workflow that needs a capability the driver lacks; `workflowtest.NewFake(workflowtest.FakeLike(prodClient))` refuses the same set without a server | [Behave like your production driver](../testing/fake.md#behave-like-your-production-driver) |
| 15 | Run several worker processes on one database when one is not enough | Steps are claimed with `FOR UPDATE SKIP LOCKED` (Postgres, MySQL) or a conditional update (SQLite), and a cron tick fires once across every process | [Run several workers](../operations/deploy.md#run-several-workers) |
| 16 | If you mount the admin API or web UI, give it a real `Auth` | The handler answers 403 on every path until both `Identify` and `Authorize` are set; the UI page is loaded by a browser without an `Authorization` header, so `Identify` must accept your session cookie | [Admin JSON API](../admin/http-api.md#step-1-say-who-may-do-what), [Admin web UI](../admin/ui.md#step-1-accept-the-session-cookie-in-identify) |
| 17 | Recycle long-lived workers if memory creeps up, and alert on queue depth | `Recycle{MaxJobs, MaxTime, MaxMemory}` drains the worker and `workflow.Serve` returns `ErrRecycle` so your supervisor starts a fresh process; `admin.Monitor` alerts when a queue's depth or wait crosses a threshold | [Worker recycling](../operations/recycling.md#step-1-set-the-limits), [Queue alerts](../admin/alerts.md#step-1-set-thresholds-and-an-alerter) |
| 18 | Get told when runs fail for good, once per cause | Nobody reads a failed-runs list at night; `middleware.AlertPlugin` calls your own sender with a dedup window, and `middleware.Report` (or the `sentry` module) puts errors in your tracker without the payload | [Failure alerts and error reporting](../observability/failure-alerts.md) |
| 19 | Set the overlap policy of crons that must not run twice at once | By default every tick starts a run, even while the last one is still going; `Schedule.Overlap: workflow.OverlapSkip` skips the tick instead, across every node | [Overlap, trigger now and last fire](../schedules/overlap.md) |
| 20 | Let web nodes only enqueue, and wake workers on Postgres | A web server that also runs workers competes with requests; `WithEnqueueOnly()` makes a client that runs nothing, and `pgnotify` starts work at once instead of at the next poll | [Deploy workers](../operations/deploy.md#web-nodes-that-only-enqueue), [Wake workers at once](../drivers/sqlstore.md#wake-workers-at-once) |
| 21 | Keep staging and production apart on shared infrastructure | Two clients with the same table prefix or Redis prefix share every run, schedule, pause and unique lock; give each environment its own prefix and read it from one config value | [Namespaces](../operations/namespaces.md#step-1--give-each-sql-namespace-a-table-prefix) |
| 22 | Give finished runs and step output somewhere to go before they are deleted | `WithArchive` copies each chunk to cold storage first and keeps the chunk when the copy fails | [Archive before deleting](../operations/retention.md#archive-before-deleting) |
| 23 | On an edge device or a laptop, check the clock before scheduling | A device that boots at 1970 fires a wrong set of ticks; `workflow.StartWhen` holds `Start` until the clock is synchronised, and `clock.WakeChecked` fires timers on time after a sleep | [Start when ready](../operations/start-gate.md), [Laptops that sleep](../operations/sleep-and-wake.md) |
| 24 | On a multi-region deployment, keep each job, schedule and event in its region | `regional.New` routes each job by a router you write; `WithSignalRouter` keeps an event's payload in one region | [Regional routing](../operations/regional-routing.md) |

## Items 1, 2, 7 and 15 in code

The deploy example runs three workers on one database, the way three processes of one service would. Every process runs the same start-up, with `Migrate` before `Start` (item 2) and the same table prefix:

<!-- snippet: examples/postgres-cluster/main.go#workers -->
```go
// Every process runs the same start-up: Migrate (safe to run from all of
// them at once; the engine's lock serialises it), Register, Start.
startCtx, stopAll := context.WithCancel(ctx)
defer stopAll()
clients := make([]*sqlstore.Client, workers)
for i := range clients {
	c, err := sqlstore.New(db, dialect,
		sqlstore.WithTablePrefix(prefix),
		sqlstore.WithClock(clk),
		sqlstore.WithPollInterval(pollInterval), // 20ms so the demo is quick; the default is 1s
	)
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil {
		return err
	}
	for _, wf := range workflows {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(startCtx); err != nil {
		return err
	}
	clients[i] = c
}
```

On SIGTERM each process cancels its `Start` context and calls `Stop` (item 7):

<!-- snippet: examples/postgres-cluster/main.go#shutdown -->
```go
// Shut down: cancel the Start context (what SIGTERM handling does) and
// call Stop, which waits until every goroutine has exited. Steps still
// running are handed back, so another worker takes them at once.
stopAll()
for _, c := range clients {
	c.Stop()
}
```

[Deploy workers](../operations/deploy.md) walks through the program; [Shut down cleanly](../operations/deploy.md#shut-down-cleanly) adds the grace period and the drain report.

## Try it

```sh
cd examples
go run ./postgres-cluster
```

```text
30 jobs, 30 ran exactly once
hourly-report fired at 10:00
fired once across 3 workers
stopped; Enqueue now fails with workflow: client not started
```

Thirty jobs spread over three workers each run exactly once (items 3 and 15), the hourly cron fires once across the three (item 15), and after the clean stop calls that need workers fail fast. Without `PG_DSN` it runs on a SQLite file; with `PG_DSN` pointing at a Postgres database you can create tables in, the same program runs on Postgres.

## Pitfalls

- `workflowtest.FakeLike` copies the production driver's capabilities, not every field-level refusal. The Hatchet driver also refuses things that are not capabilities: at `Register` a Go concurrency key function or a retry backoff whose `Initial` is not `Factor` seconds, at `Enqueue` a negative `Job.Priority` or an idempotency key together with `RunAt`. Register your workflows on a Hatchet client built with test configuration, which needs no server before `Start`, to catch those.
- A SQLite file is durable on one machine. Several machines need Postgres or MySQL; several processes on one machine can share the SQLite file.
- `sqlstore.Client.EnqueueTx` does not run enqueue middleware (unique, debounce, tracing), because their side effects would not roll back with your transaction; use the outbox when you need both ([Choose how to enqueue](../choosing/methods.md#enqueue-enqueuetx-or-the-outbox)).
- Run-finished observers run on a driver goroutine and must return quickly (on the memory driver they share one goroutine, so a slow one delays every later delivery); start long work with a new `Enqueue`.
- The SQL driver claims only steps of workflows registered on the client, so after a deploy that removes a workflow its unfinished runs stay in the database and never run. Keep a workflow registered until its last run has finished.

## Related

- [Choose by kind of app](../choosing/app-types.md)
- [Run several workers](../operations/deploy.md#run-several-workers)
- [Capabilities](../concepts/capabilities.md#what-a-driver-declares)
- Example: [postgres-cluster](../../examples/postgres-cluster/)
