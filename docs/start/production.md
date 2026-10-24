# Going to production

What changes between the memory driver on a laptop and a service whose jobs must not be lost: a durable driver, migrations run by the deploy, handlers that tolerate running twice, a clean shutdown, and the knobs that keep tables and logs in shape. Each item links to the guide that explains it. The library starts nothing in the background on its own and never creates tables at `Start`, so every item here is a decision you make in code.

## Before you start

- Your workflows run on the memory driver ([Your first workflow](first-workflow.md)), and your app tests pass against `workflowtest.Fake` ([Test app code with the Fake](../testing/fake.md#test-app-code-with-the-fake)).
- You have picked a durable backend: [Choose a driver](../choosing/drivers.md).

## Checklist

| # | Item | Why | Guide |
|---|---|---|---|
| 1 | Run on a driver that declares `CapDurable`: `sqlstore` (SQLite, Postgres, MySQL 8), `redis` or `hatchet` | The memory driver loses every queued, waiting and sleeping run when the process exits; it declares every capability but `CapDurable` | [Choose a driver](../choosing/drivers.md#memory-sql-redis-or-hatchet) |
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
