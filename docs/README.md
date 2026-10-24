# ubgo/workflow documentation

Guides for `github.com/ubgo/workflow`: one Go API for background jobs, cron schedules and multi-step workflows, on an in-memory driver, a SQL driver (SQLite, Postgres, MySQL 8), a Redis driver or the Hatchet engine. Every guide is step by step, ends with its pitfalls, and copies its Go code from a runnable program under [examples/](../examples/), which `go test` runs, so the code on these pages compiles and works.

![Architecture: your app calls one workflow.Client; enqueue and step middleware, batches, failover and the outbox layer over any client; the memory, SQL and Hatchet drivers implement it over memory, your SQL database or the Hatchet engine](assets/architecture.svg)

The diagram numbers the two paths: steps 1 to 5 are an enqueue, from your app through the capability check and enqueue middleware to the stored run; steps 6 to 10 are its execution, from a worker claiming the run through step middleware to your step function and the run-finished observers. Your app depends on `workflow.Client` alone. Plugins, middleware and failover wrap a client without hiding what it can do, and each driver declares its capabilities, so a feature a backend cannot honour is refused at start-up instead of silently dropped ([The client](concepts/client.md), [Capabilities](concepts/capabilities.md)).

## Contents

- [Learning path](#learning-path)
- [Start from your goal](#start-from-your-goal)
- [All pages](#all-pages): [Start](#start), [Choosing](#choosing), [Concepts](#concepts), [Jobs](#jobs), [Schedules](#schedules), [Workflows](#workflows), [Events](#events), [Drivers](#drivers), [Middleware](#middleware), [Observability](#observability), [Batches](#batches), [Reliability](#reliability), [Testing](#testing), [Operations](#operations), [Admin](#admin), [Extending](#extending), [Reference](#reference)

## Learning path

Read in this order to go from a first job to a production setup. Each step builds on the one before; skip what your app does not need.

| # | Step | What you learn | Guide | Example |
|---|---|---|---|---|
| 1 | Run one job | Register a workflow, start the workers, enqueue a job, on the memory driver | [Quickstart](start/quickstart.md) | [quickstart](../examples/quickstart/) |
| 2 | Write a real workflow | Steps that depend on each other, parallel steps, a join, retries, a permanent failure and its `OnFailure` handler | [Your first workflow](start/first-workflow.md) | [first-workflow](../examples/first-workflow/) |
| 3 | Choose a driver | Memory, SQL or Hatchet, what each can do, and how the library refuses what a driver lacks | [Choose a driver](choosing/drivers.md), [Capabilities](concepts/capabilities.md) | [capabilities](../examples/capabilities/) |
| 4 | Control how jobs run | Retries and backoff, delayed jobs, cancel and status | [Retries](jobs/retries.md), [Run a job later](jobs/delays.md), [Cancel and status](jobs/cancel-and-status.md) | [retries](../examples/retries/), [delayed-jobs](../examples/delayed-jobs/) |
| 5 | Never run the same thing twice | Idempotency keys, concurrency limits per key, rate limits | [Idempotency keys](jobs/idempotency.md), [Concurrency and rate limits](jobs/concurrency.md) | [idempotency](../examples/idempotency/), [concurrency](../examples/concurrency/) |
| 6 | Schedule recurring work | Cron on a workflow and at run time, time zones and DST, missed ticks | [Cron schedules](schedules/cron.md), [Time zones](schedules/time-zones.md), [Misfire policies](schedules/misfire.md) | [cron-schedules](../examples/cron-schedules/) |
| 7 | Wait for the outside world | Wait steps released by `Signal`, deadlines, durable sleeps | [Wait for an event](workflows/signals.md), [Sleep inside a workflow](workflows/sleep.md) | [signals-and-sleep](../examples/signals-and-sleep/) |
| 8 | Test your app | The Fake that refuses what production refuses, assertions, the Recorder, a fake clock | [Test app code with the Fake](testing/fake.md), [Drive time in tests](testing/fake-clock.md) | [app-tests](../examples/app-tests/) |
| 9 | Make it durable | The SQL driver on SQLite, migrations run by your deploy | [The SQL driver](drivers/sqlstore.md), [Migrations](operations/migrations.md) | [sqlite-worker](../examples/sqlite-worker/), [migrations-export](../examples/migrations-export/) |
| 10 | Add cross-cutting behaviour | Logging, recovery, timeouts, a circuit breaker, encrypted payloads, tracing and metrics, run-end observers | [Step middleware](middleware/step-middleware.md), [Payload codecs](middleware/codecs.md), [OpenTelemetry](observability/opentelemetry.md), [React when a run finishes](observability/run-finished.md) | [middleware-stack](../examples/middleware-stack/), [opentelemetry](../examples/opentelemetry/), [run-finished](../examples/run-finished/) |
| 11 | Keep jobs and data consistent | Enqueue inside your transaction, the outbox, unique jobs, debounce | [Transactional enqueue](reliability/transactional-enqueue.md), [Outbox](reliability/outbox.md), [Unique jobs](reliability/unique-jobs.md) | [transactional-enqueue](../examples/transactional-enqueue/), [outbox](../examples/outbox/), [unique-jobs](../examples/unique-jobs/) |
| 12 | Run groups of jobs | Batches with `Then`, `Catch` and `Finally`, progress, a durable batch store | [Batches](batches/batches.md), [Batch durability](batches/durability.md) | [batches](../examples/batches/), [batch-durable](../examples/batch-durable/) |
| 13 | Go to production | Several workers on one database, clean shutdown, retention, failover, the launch checklist | [Going to production](start/production.md), [Deploy workers](operations/deploy.md), [Retention](operations/retention.md), [Failover](reliability/failover.md) | [postgres-cluster](../examples/postgres-cluster/), [retention](../examples/retention/), [failover](../examples/failover/) |
| 14 | Extend it | A plugin, your own store, your own driver held to the conformance suite | [Write a plugin](extending/writing-a-plugin.md), [Write a driver](extending/writing-a-driver.md) | [custom-plugin](../examples/custom-plugin/), [custom-driver](../examples/custom-driver/) |

## Start from your goal

| I want to | Read | Example |
|---|---|---|
| Send an email after sign-up without blocking the request | [Enqueue a job](jobs/enqueue.md) | [quickstart](../examples/quickstart/) |
| Pick a backend for my app | [Choose a driver](choosing/drivers.md), [Choose by kind of app](choosing/app-types.md) | [capabilities](../examples/capabilities/) |
| Retry a flaky API call, honouring its `Retry-After` | [Retries](jobs/retries.md) | [retries](../examples/retries/) |
| Send a reminder tomorrow at 09:00, and cancel it if the user acts first | [Run a job later](jobs/delays.md), [Cancel and status](jobs/cancel-and-status.md) | [delayed-jobs](../examples/delayed-jobs/) |
| Run a nightly job, or a report per tenant in the tenant's zone | [Cron schedules](schedules/cron.md), [Time zones](schedules/time-zones.md) | [cron-schedules](../examples/cron-schedules/) |
| Accept a webhook twice and process it once | [Idempotency keys](jobs/idempotency.md) | [idempotency](../examples/idempotency/) |
| Keep one tenant from taking every worker | [Concurrency and rate limits](jobs/concurrency.md) | [concurrency](../examples/concurrency/) |
| Pause a run until a payment or an approval arrives | [Wait for an event](workflows/signals.md) | [signals-and-sleep](../examples/signals-and-sleep/) |
| Save an order and enqueue its job atomically | [Transactional enqueue](reliability/transactional-enqueue.md), [Outbox](reliability/outbox.md) | [transactional-enqueue](../examples/transactional-enqueue/), [outbox](../examples/outbox/) |
| Allow one export per user at a time | [Unique jobs](reliability/unique-jobs.md) | [unique-jobs](../examples/unique-jobs/) |
| Reindex once after a burst of edits | [Debounce](reliability/debounce.md) | [debounce](../examples/debounce/) |
| Process 10,000 rows and know when all are done | [Batches](batches/batches.md) | [batches](../examples/batches/) |
| Send payloads too big for the queue | [Claim check](reliability/claim-check.md) | [claim-check](../examples/claim-check/) |
| Keep personal data out of the queue tables | [Payload codecs](middleware/codecs.md) | [encrypted-payloads](../examples/encrypted-payloads/) |
| Trace a job from the request that enqueued it | [OpenTelemetry](observability/opentelemetry.md) | [opentelemetry](../examples/opentelemetry/) |
| Keep accepting jobs while the engine is down | [Failover](reliability/failover.md) | [failover](../examples/failover/) |
| [Secrets in payloads](reliability/secrets.md) | `SecretGuard` refusing credentials at enqueue, `SecretRef` resolved in the step |
| Run several worker machines on one Postgres | [Deploy workers](operations/deploy.md) | [postgres-cluster](../examples/postgres-cluster/) |
| Run my jobs on the Redis I already have | [The Redis driver](drivers/redis.md) | [redis-worker](../examples/redis-worker/) |
| Run my jobs on Hatchet | [The Hatchet driver](drivers/hatchet.md) | [hatchet-worker](../examples/hatchet-worker/) |
| [Hybrid placement](drivers/hybrid.md) | Crons fire on one client, runs live on another; what the split refuses; several nodes |
| Test the code that enqueues, without running workers | [Test app code with the Fake](testing/fake.md) | [app-tests](../examples/app-tests/) |
| Send slow imports to their own workers and pause them during maintenance | [Named queues](jobs/queues.md) | [named-queues](../examples/named-queues/) |
| See failed runs, retry one from the failed step, or clear a backlog | [Manage runs](jobs/manage-runs.md), [Admin operations](admin/operations.md) | [manage-runs](../examples/manage-runs/), [admin](../examples/admin/) |
| Run a schedule only in office hours, and ping a heartbeat when it ends | [Schedule conditions](schedules/conditions.md), [Schedule hooks and pings](schedules/hooks.md) | [schedule-hooks](../examples/schedule-hooks/) |
| Send a confirmation email only if the request that ordered it succeeded | [Deferred dispatch](jobs/deferred.md) | [deferred-dispatch](../examples/deferred-dispatch/) |
| Tell a run waiting to retry from one waiting for a worker, and a dropped run from a failed one | [Run states](jobs/run-states.md) | [run-states](../examples/run-states/) |
| Let a job decide what runs after it | [Chains of jobs](workflows/chains.md) | [chain-edits](../examples/chain-edits/) |
| [Memoise work inside a step](workflows/memoisation.md) | `Memo` and the journal, `MapEach`, `Loop`, `ProcessChunks` with progress, run data |
| [Branching](workflows/branching.md) | `Choice`: one branch picked at run time, built from ordinary steps |
| [Child runs](workflows/children.md) | `Spawn` and `AwaitChildren`: children started once per run, waiting without holding a worker |

### Events

| Page | What it covers |
|---|---|
| [Events](events/README.md) | What the events package is and is not, quick answers |
| [Publish and subscribe](events/publish-subscribe.md) | A `Bus`, subscriptions and filters, deduplication by event ID, delivering to waiting runs |
| [Inbound webhooks](events/webhooks.md) | `NewWebhook` and its status codes, HMAC signatures, Standard Webhooks, your own verifier |
| Give a backed-up queue the workers an idle one does not need | [Worker balancing](operations/balancing.md) | [worker-balancing](../examples/worker-balancing/) |
| Run a small Go function in the background without registering a workflow | [Closures](jobs/closures.md) | [closures](../examples/closures/) |
| Run a small Go function in the background so it survives a restart | [Functions by name](jobs/functions.md) | [functions](../examples/functions/) |
| [Typed payloads](jobs/payloads.md) | `JobOf`, `DecodePayload`, payload size limits, payload versions with upcasters |
| [Progress and heartbeats](jobs/progress.md) | `ReportProgress`, `RunInfo.Progress`, progress events, `Heartbeat` and `Step.HeartbeatTimeout` |
| [Results and awaiting a run](jobs/results.md) | A run's result, `Await`, `AwaitResult`, `RunFailedError` |
| [Timeouts](jobs/timeouts.md) | The four deadlines, `Workflow.RunTimeout`, `OnTimeout`, the `ErrTimeout` family |
| [Tenants](jobs/tenants.md) | `Job.Tenant`, reading it in steps and events, `RunQuery.Tenant` |
| [Upstream rate limits](jobs/upstream-limits.md) | A leaky-bucket, cost-based limit on calls to an outside API, `Observe`, your own bucket store |
| Keep what a nightly task logged in a file, and mail it when the task fails | [Task output](schedules/output.md) | [task-output](../examples/task-output/) |
| Restart a worker after so many jobs, so long, or above a memory size | [Worker recycling](operations/recycling.md) | [worker-recycling](../examples/worker-recycling/) |
| [Nodes and health](operations/nodes.md) | Which node ran each attempt, the online nodes, liveness and readiness probes, scheduler-only and worker-only processes |
| Keep a slow hourly job from starting again while it still runs, and run it now by hand | [Overlap, trigger now and last fire](schedules/overlap.md) | [schedule-overlap](../examples/schedule-overlap/) |
| [Local cron](schedules/local-cron.md) | An in-process cron that runs on every node: cache warmers, local cleanup |
| Be told once when runs keep failing, and send their errors to Sentry | [Failure alerts and error reporting](observability/failure-alerts.md) | [failure-alerts](../examples/failure-alerts/), [sentry-reporting](../examples/sentry-reporting/) |
| [Lifecycle events](observability/lifecycle.md) | A live feed of inserts, attempts, retries, progress and ends; best effort, local; how it differs from the run-finished hook |
| [Stuck jobs](observability/stuck-jobs.md) | Report an attempt that runs far too long without touching it; Stuck vs Timeout vs HeartbeatTimeout |
| See every attempt's error, or end a stuck run by hand | [Manage runs › Error history](jobs/manage-runs.md#error-history), [Force a run to an end](jobs/manage-runs.md#force-a-run-to-an-end) | [run-repair](../examples/run-repair/) |
| Let web servers enqueue without running workers | [Deploy workers › Web nodes that only enqueue](operations/deploy.md#web-nodes-that-only-enqueue) | [enqueue-only](../examples/enqueue-only/) |
| Start jobs at once on Postgres instead of at the next poll | [The SQL driver › Wake workers at once](drivers/sqlstore.md#wake-workers-at-once) | [worker-wakeup](../examples/worker-wakeup/) |
| Give operators a dashboard API with my own login | [Admin JSON API](admin/http-api.md) | [admin](../examples/admin/) |
| Give operators a web dashboard behind my own login | [Admin web UI](admin/ui.md) | [admin-ui](../examples/admin-ui/) |
| Find every run of one customer, or every order number starting with A- | [Search by label](jobs/manage-runs.md#search-by-label) | [label-search](../examples/label-search/) |
| Retry a step without charging the card twice | [Memoise work inside a step](workflows/memoisation.md) | [flow-memo](../examples/flow-memo/) |
| Accept a signed webhook and start a run once per delivery | [Inbound webhooks](events/webhooks.md) | [events-webhooks](../examples/events-webhooks/) |
| Stay inside an outside API's call limit per account | [Upstream rate limits](jobs/upstream-limits.md) | [upstream-limits](../examples/upstream-limits/) |
| Keep tokens and passwords out of stored payloads | [Secrets in payloads](reliability/secrets.md) | [secret-guard](../examples/secret-guard/) |
| Wait in a step until a third-party export is ready, without spending retries | [Retries › Snooze](jobs/retries.md#snooze-a-step-that-is-not-ready) | [snooze-heartbeat](../examples/snooze-heartbeat/) |
| Show a progress bar for a long export and hand back its result | [Progress and heartbeats](jobs/progress.md), [Results and awaiting a run](jobs/results.md) | [results-progress](../examples/results-progress/) |
| Change a job's payload shape without breaking queued jobs | [Typed payloads](jobs/payloads.md) | [typed-payloads](../examples/typed-payloads/) |
| Show a live progress bar and every retry as it happens | [Lifecycle events](observability/lifecycle.md) | [lifecycle-events](../examples/lifecycle-events/) |
| Give Kubernetes a liveness and a readiness probe | [Nodes and health](operations/nodes.md#health-and-readiness-probes) | [node-health](../examples/node-health/) |
| Fire crons in my process and keep the jobs in the database | [Hybrid placement](drivers/hybrid.md) | [hybrid-placement](../examples/hybrid-placement/) |
| Fire crons on one chosen node, or keep nodes with drifting clocks from stealing each other's steps | [The SQL driver › Clocks, skew and one fire node](drivers/sqlstore.md#clocks-skew-and-one-fire-node) | [postgres-cluster](../examples/postgres-cluster/) |
| Retry every failed run of a workflow from a terminal | [The workflow command](admin/cli.md) | [admin-cli](../examples/admin-cli/) |
| Run a schedule only in production, or only during a campaign week | [Schedule conditions](schedules/conditions.md) | [schedule-specs](../examples/schedule-specs/) |
| Warm a cache on every node every minute | [Local cron](schedules/local-cron.md) | [schedule-specs](../examples/schedule-specs/) |
| Check whether the library has a feature | [Feature index](reference/features.md) | |

## All pages

### Start

| Page | What it covers |
|---|---|
| [Install](start/install.md) | The core module, the separate modules (`sqlstore`, `redis`, `hatchet`, `otel`, `sentry`, `admin`, `cli`) and the Go version each needs |
| [Quickstart](start/quickstart.md) | One workflow on the memory driver in about thirty lines, then the same code on SQLite |
| [Your first workflow](start/first-workflow.md) | Four steps with a retry, a parallel pair, a join and a failure handler |
| [Going to production](start/production.md) | The checklist: durable driver, migrations, idempotent handlers, clean shutdown, retention, logs |

### Choosing

| Page | What it covers |
|---|---|
| [Choosing](choosing/README.md) | The decision guides, with quick answers to the first decisions |
| [Choose a driver](choosing/drivers.md) | Memory, SQL or Hatchet: durability, machines, what you operate, capabilities |
| [Choose a method](choosing/methods.md) | `Enqueue`, `EnqueueTx` or the outbox; cron or `Schedule`; batches; duplicate guards; limits; delays; retries; run-end hooks |
| [Choose by kind of app](choosing/app-types.md) | A starting configuration for nine kinds of app |

### Concepts

| Page | What it covers |
|---|---|
| [The client](concepts/client.md) | The one `Client` interface, start and stop, validation |
| [Capabilities](concepts/capabilities.md) | What a driver declares, checking workflows and schedules, refusals |
| [Errors](concepts/errors.md) | Sentinel errors and the backend-outage marker |
| [Labels](concepts/labels.md) | Job labels and the reserved prefixes |
| [Plugins](concepts/plugins.md) | Adding a feature to a client with `workflow.Use` |
| [Time](concepts/time.md) | The injected clock every scheduling decision reads |

### Jobs

| Page | What it covers |
|---|---|
| [Enqueue a job](jobs/enqueue.md) | `Job`, `RunRef`, payloads, labels and priority |
| [Run a job later](jobs/delays.md) | `Job.RunAt` and scheduled runs |
| [Idempotency keys](jobs/idempotency.md) | One run per key, and requiring or ignoring keys per workflow |
| [Retries](jobs/retries.md) | Retry policies, per-step retries, backoff and jitter, `Retry-After`, permanent errors, snooze |
| [Concurrency and rate limits](jobs/concurrency.md) | Limits per key, strategies for the run over the limit, FIFO per key, rate limits |
| [Cancel and status](jobs/cancel-and-status.md) | Stopping a run and reading where it is |
| [Named queues](jobs/queues.md) | `Workflow.Queue` and `Job.Queue`, workers, priority and weights per queue, pausing a queue |
| [Manage runs](jobs/manage-runs.md) | List, search by label, retry, delete and bulk-cancel runs; due-time filters, reschedule, cancel one step; pause a workflow, a queue or everything |
| [Run a workflow inline](jobs/run-inline.md) | `RunInline`: one run in the caller's goroutine, with no client or workers |
| [Run states](jobs/run-states.md) | The retrying and discarded statuses, next-attempt time, querying them, batches and discarded jobs, switching on statuses |
| [Deferred dispatch](jobs/deferred.md) | Jobs enqueued during a request, sent only when it succeeds: `DeferHTTP`, `Deferred` by hand, and when a transaction is better |
| [Closures](jobs/closures.md) | A Go function run as an in-process job on the memory driver, and why durable drivers refuse it |
| [Functions by name](jobs/functions.md) | A Go function registered under a name and enqueued with a JSON argument: durable closures on every driver |

### Schedules

| Page | What it covers |
|---|---|
| [Cron schedules](schedules/cron.md) | Cron on a workflow and at run time, the syntax, a seconds field, calendar specs, parse errors, other dialects, one fire across nodes, the tick a run is for |
| [Time zones](schedules/time-zones.md) | Schedules in a zone, loading zones by name, the embedded zone database, next runs in two zones, daylight saving time |
| [Misfire policies](schedules/misfire.md) | What happens to ticks missed during downtime |
| [Schedule conditions](schedules/conditions.md) | Time-of-day windows and `ScheduleWhen`, environment gating, an active period, the answer to `between()`, `unlessBetween()`, `when()` and `skip()` |
| [Schedule hooks and pings](schedules/hooks.md) | Tick events, per-schedule hooks and heartbeat pings |
| [Manage schedules](schedules/manage.md) | List schedules with their next fire, pause and resume, group schedules, keep the newest runs, store a schedule as JSON |
| [Task output](schedules/output.md) | Capture what a task logs and returns and send it to a file, a writer, a mail function or the database |
| [Overlap, trigger now and last fire](schedules/overlap.md) | Skip a tick while the previous run is unfinished, run a schedule now, read its last fire and run |

### Workflows

| Page | What it covers |
|---|---|
| [Workflows and steps](workflows/steps.md) | Defining a workflow, typed handlers, dependencies, step info, timeouts, failure handlers |
| [Wait for an event](workflows/signals.md) | Wait steps, matching events to runs, deadlines |
| [Sleep inside a workflow](workflows/sleep.md) | Durable sleep steps |
| [Chains of jobs](workflows/chains.md) | `Job.Chain`, editing the rest of a chain from a running step, failure, and child runs on Hatchet |

### Drivers

| Page | What it covers |
|---|---|
| [The in-memory driver](drivers/memory.md) | For tests, local runs and work that may be lost |
| [The SQL driver (sqlstore)](drivers/sqlstore.md) | SQLite, Postgres and MySQL 8: workers, polling, wake-ups with LISTEN/NOTIFY, leases, database errors, run IDs |
| [The Redis driver](drivers/redis.md) | Durable runs and cron on Redis: whole-run leases, fenced writes, what it still refuses, shared stores, deployment, upgrading |
| [The Hatchet driver](drivers/hatchet.md) | Configuration, what it refuses, engine quirks, the native SDK |

### Middleware

| Page | What it covers |
|---|---|
| [Step middleware](middleware/step-middleware.md) | Recover, timeout, fail on chosen errors, skip, circuit breaker |
| [Enqueue middleware](middleware/enqueue-middleware.md) | Wrapping every enqueue, routing jobs |
| [Payload codecs](middleware/codecs.md) | Transforming payloads, encryption with key rotation |

### Observability

| Page | What it covers |
|---|---|
| [Logging](observability/logging.md) | A log line per step attempt, and driver logs |
| [OpenTelemetry](observability/opentelemetry.md) | Spans, trace context from enqueue to step, parenting, span names, labels, metrics |
| [React when a run finishes](observability/run-finished.md) | The run-finished hook, and its best-effort form on Hatchet |
| [Failure alerts and error reporting](observability/failure-alerts.md) | Alerts on final failures with dedup and rate limits, error reporters, Sentry |

### Batches

| Page | What it covers |
|---|---|
| [Batches](batches/batches.md) | Dispatch with `Then`, `Catch` and `Finally`, progress, cancel, labels, IDs |
| [Batch completion](batches/completion.md) | How a batch learns a job ended, Hatchet's source, retrying callbacks |
| [Batch durability](batches/durability.md) | Where batch state lives, the SQL store, resuming a dispatch |
| [Native group runs](batches/native-groups.md) | Bulk triggers on Hatchet, and the batch coordinator |
| [Change a batch after dispatch](batches/lifecycle.md) | `Before` and `Progress` callbacks, `Add`, `RetryFailed`, `Prune`, `EnqueueMany` |
| [Wait for a batch in a workflow](batches/await.md) | `AwaitStep`: a step that dispatches a batch and waits for it |

### Reliability

| Page | What it covers |
|---|---|
| [Unique jobs](reliability/unique-jobs.md) | Unique for a time, until the run finishes, by workflow and arguments, by tagged fields, by holder state, returning the existing run, across processes |
| [Debounce](reliability/debounce.md) | One run per burst, with shared state |
| [Claim check](reliability/claim-check.md) | Big payloads and outputs in a blob store, deleted when the run ends |
| [Transactional enqueue](reliability/transactional-enqueue.md) | `EnqueueTx` inside your own SQL transaction |
| [Outbox](reliability/outbox.md) | Jobs written in your transaction and relayed to any client |
| [Failover](reliability/failover.md) | A second backend for enqueues, outage classification, cool-off |

### Testing

| Page | What it covers |
|---|---|
| [Test app code with the Fake](testing/fake.md) | A client that runs nothing, assertions, matchers, operator actions, behaving like production |
| [Record a real client](testing/recorder.md) | Recording the enqueues of a real driver |
| [Drive time in tests](testing/fake-clock.md) | A fake clock for delays, retries, sleeps and crons |
| [Test batches, chains and a subset of workflows](testing/batches-and-chains.md) | Batch and chain assertions, `FakeExcept` and `FakeOnly` |
| [Test one step](testing/run-step.md) | `RunStep` and its outcomes: succeeded, failed, permanent, discarded, released |
| [Test queues, retries and pauses](testing/operations.md) | `AssertEnqueuedOn`, `AssertRetried`, `AssertDeleted`, `AssertPaused` |
| [Run everything now in tests](testing/run-all.md) | Memory RunAll: what it hurries (delays, backoffs, snoozes, sleeps) and what it does not (timeouts); ErrRunAllStalled |

### Operations

| Page | What it covers |
|---|---|
| [Deploy workers](operations/deploy.md) | Several workers on one database, web nodes that only enqueue, clean shutdown without spending a retry |
| [Migrations](operations/migrations.md) | The ledger, each store's migrations, rollbacks, your own migration tool, table names |
| [Retention](operations/retention.md) | Keeping, pruning and archiving finished runs |
| [Tuning](operations/tuning.md) | Housekeeping and Hatchet limits |
| [Worker balancing](operations/balancing.md) | Moving workers between queues by depth or wait, bounds, the balance loop, your own strategy |
| [Worker recycling](operations/recycling.md) | Stop a worker after a number of jobs, a time or a memory size so its supervisor starts a fresh one |

### Admin

| Page | What it covers |
|---|---|
| [Admin operations](admin/operations.md) | The `admin` module's Go operations over any driver: runs, failures, retry, delete, cancel, force, bulk actions with dry run, reschedule, cancel a step, pause, schedules with trigger-now, queue stats, captured output, run-end charts, nodes and health |
| [Admin JSON API](admin/http-api.md) | A closed-by-default HTTP API with typed `Identify` and `Authorize` hooks |
| [Admin web UI](admin/ui.md) | The dashboard the admin handler serves: screens (a run's attempts, output, progress and result, a schedule with Run now, charts, system health), cookie login, CSP, keys |
| [Queue alerts](admin/alerts.md) | `admin.Monitor` thresholds on queue depth and wait, and the OpenTelemetry depth gauge |
| [The workflow command](admin/cli.md) | The CLI over the admin API: auth flags and env vars, every command, --json, --dry-run, exit codes, embedding |

### Extending

| Page | What it covers |
|---|---|
| [Write a driver](extending/writing-a-driver.md) | A complete driver under 300 lines, held to the conformance suite |
| [Write a plugin](extending/writing-a-plugin.md) | What a plugin can provide, and a tenancy plugin |
| [Write middleware](extending/writing-middleware.md) | Step and enqueue middleware of your own |
| [Custom stores](extending/custom-stores.md) | Your own store for unique locks, debounce, circuits, blobs or batches |

### Reference

| Page | What it covers |
|---|---|
| [Feature index](reference/features.md) | Every feature, with its package, guide section and example |
| [Capabilities](reference/capabilities.md) | Every capability and which driver has it |
| [Errors](reference/errors.md) | Every sentinel error, which call returns it and what to do |
| [Benchmarks](reference/benchmarks.md) | Enqueue and end-to-end throughput and pickup latency per driver, method, caveats and how to rerun |
| [Names](reference/names.md) | Reserved labels, tables, migration owners, Hatchet metadata, IDs, span and metric names |

The runnable programs are listed in [examples/README.md](../examples/README.md).
