# Examples

Runnable programs, one per task an app actually has. Each folder is a complete `main` package with a `README.md` that walks through it, and a `main_test.go` that runs the same code and checks its output, so a recipe that stops working fails `task check`. The guides under [docs/](../docs/) copy their Go code from marked regions of these programs, and `task docs:snippets` fails when a guide and its program drift apart.

This folder is its own Go module (`github.com/ubgo/workflow/examples`), so a database driver, the Hatchet SDK or OpenTelemetry that an example needs never becomes a dependency of the library. Run every example with `task examples:test`, or one with `go run ./quickstart` from this folder.

## Recipes

86 programs, grouped in the order the [learning path](../docs/README.md#learning-path) reaches them.

### Start here

| Recipe | What it shows | Key packages |
|---|---|---|
| [quickstart](quickstart) | One workflow on the in-memory driver: register, start, enqueue, see the step run | `workflow`, `memory` |
| [first-workflow](first-workflow) | Four steps: a retried payment, two parallel steps, a join of their outputs, a permanent failure running OnFailure, run ends through OnRunFinished | `workflow`, `memory` |
| [capabilities](capabilities) | What each driver declares; checking a workflow, schedule and job before running; refusals, validation, sentinel errors, the outage marker, reserved labels | `workflow`, `memory`, `sqlstore`, `hatchet` |

### Jobs, schedules and workflows

| Recipe | What it shows | Key packages |
|---|---|---|
| [retries](retries) | Backoff with jitter, RetryAfter/DelayFor, Permanent and NoRetryOn, per-step RetriesNone, Hatchet refusing jitter and DelayFor | `workflow`, `memory`, `clock`, `hatchet` |
| [delayed-jobs](delayed-jobs) | RunAt and ScheduledID, Status, cancel a delayed and a running run | `workflow`, `memory`, `clock` |
| [idempotency](idempotency) | One run per key, key freed at the end, optional/required/off, refusal without CapIdempotencyKey | `workflow`, `memory`, `workflowtest` |
| [concurrency](concurrency) | Per-key concurrency and strategies, rate limits, priority, KeyFunc vs KeyExpr refusals | `workflow`, `memory`, `clock`, `hatchet` |
| [cron-schedules](cron-schedules) | Cron syntax, Vixie rule, parse errors, DST, CronLocation, Schedule/Unschedule, upsert by Name on SQLite, misfire policies and Misfire.Due, WithCronParser, Hatchet refusals | `workflow`, `cron`, `memory`, `sqlstore`, `hatchet` |
| [signals-and-sleep](signals-and-sleep) | Wait steps with KeyFrom and Key, Signal, durable sleep, wait timeout plus OnFailure, fan-in with StepInput, StepInfo, step timeout | `workflow`, `memory`, `clock`, `hatchet` |
| [named-queues](named-queues) | Workflow.Queue and Job.Queue, memory.WithQueues with priority and a worker cap per queue, pausing one queue while the others run | `workflow`, `memory` |
| [retry-limits](retry-limits) | Job.Retry per dispatch, RetryPolicy.Until, MaxErrors letting releases through, Discard | `workflow`, `memory`, `clock` |
| [discard-exhausted](discard-exhausted) | The precedence Job.Retry over Step.Retry over Workflow.Retry over the default, and a run out of retries ending failed or, with DiscardOnExhaust, discarded with no OnFailure | `workflow`, `memory` |
| [release-jobs](release-jobs) | middleware.Release while a condition holds, and a unique lock freed when the run starts (UniqueUntilStarted) | `middleware`, `memory`, `clock` |
| [run-inline](run-inline) | workflow.RunInline: a workflow run once in the caller's goroutine, with retries, middleware and events, no client | `workflow` |
| [run-states](run-states) | StatusRetrying with the next attempt's number and time, StatusDiscarded after Discard, MaxErrors still ending failed, admin.Discarded, a batch's DiscardedAs, a switch with a default branch | `workflow`, `memory`, `clock`, `batch`, `admin` |
| [deferred-dispatch](deferred-dispatch) | DeferHTTP sending a request's jobs only on success, dropping them on a 402 or a panic, and a Deferred flushed or discarded by hand | `workflow`, `middleware`, `memory` |
| [chain-edits](chain-edits) | Job.Chain, PrependToChain and AppendToChain from a running step, a retried step adding its jobs once, a failed run ending the chain, the Hatchet refusal | `workflow`, `memory`, `hatchet` |
| [closures](closures) | A Go function run as an in-process job with retries, status and cancel, and the SQL and Hatchet refusals | `workflow`, `memory`, `sqlstore`, `hatchet` |
| [functions](functions) | A Go function registered by name with a JSON argument: retry and catch, an unregistered name refused, a delayed run surviving a restart on SQLite | `workflow`, `memory`, `sqlstore`, `clock` |
| [cron-builder](cron-builder) | Cron expressions from named frequencies, what the builder refuses, and one driving a workflow's cron | `cron`, `memory`, `clock` |
| [schedule-hooks](schedule-hooks) | Windows, ScheduleWhen, tick events, per-schedule hooks with heartbeat pings, schedule list and pause | `workflow`, `middleware`, `memory`, `clock` |
| [task-output](task-output) | A nightly task's log lines and result captured, appended to a file every night and mailed when it fails | `workflow`, `middleware`, `memory`, `clock` |
| [schedule-groups](schedule-groups) | Schedules that share options built by one Go helper; the group label finds them in the list, filters their runs and pauses the group | `workflow`, `memory`, `clock` |
| [schedule-overlap](schedule-overlap) | A tick skipped while the previous run is unfinished, Run now refused then accepted with the manual label, the last fire with its run and status, and the same on the Fake | `workflow`, `memory`, `clock`, `workflowtest` |
| [output-store](output-store) | Captured output kept per attempt in the database with OutputStore, read back, and served to the admin through OutputSourceFunc | `sqlstore`, `middleware`, `admin` |
| [schedule-specs](schedule-specs) | Calendar specs, the seconds dialect, LoadZone, NextRuns in two zones across DST, an active period, environment gating, a schedule as JSON, a per-node LocalCron | `workflow`, `cron`, `clock` |
| [snooze-heartbeat](snooze-heartbeat) | Snooze without spending an attempt, a hung attempt failed by HeartbeatTimeout, RunTimeout running OnTimeout | `workflow`, `memory`, `clock` |
| [results-progress](results-progress) | ReportProgress and progress events, AwaitResult and DecodePayload, a two-sink result, RunFailedError, runs by tenant | `workflow`, `memory`, `clock` |
| [typed-payloads](typed-payloads) | JobOf, DecodePayload, MaxPayload and WithPayloadLimit, PayloadVersion with DefineVersioned and an upcaster | `workflow`, `memory`, `middleware` |
| [run-reschedule](run-reschedule) | Runs due in the next hour, Reschedule, CancelStep with and without a fallback | `workflow`, `memory`, `clock` |
| [queue-weights](queue-weights) | Weighted queues on one worker with a seeded draw, FIFO per concurrency key | `workflow`, `memory` |
| [queue-limit](queue-limit) | QueueConfig.Limit capping a queue across clients: peak concurrency within the limit on memory and on two SQL nodes sharing one SQLite file | `workflow`, `memory`, `sqlstore` |
| [upstream-limits](upstream-limits) | A leaky bucket on a fake clock with Take, Observe, Wait and TakeOrSnooze; PeriodKey in Berlin and OncePerPeriod | `workflow`, `memory`, `clock` |
| [flow-memo](flow-memo) | Memo around a charge on a retried step, MapEach and Loop rerunning only what failed, ProcessChunks with progress, SetRunData/RunData, a Choice, ErrNoJournal | `workflow`, `memory` |
| [flow-children](flow-children) | Spawn with parent labels, AwaitChildren joining results in order without spending parent attempts, a failed child failing the parent | `workflow`, `memory` |
| [events-webhooks](events-webhooks) | A Bus with filtered subscriptions and dedup by event ID, a webhook handler's 405/401/413/200/503 answers with HMAC, Standard Webhooks refusing a changed body and a replay | `events`, `workflow`, `memory`, `clock` |

### Drivers and operations

| Recipe | What it shows | Key packages |
|---|---|---|
| [sqlite-worker](sqlite-worker) | One workflow on memory, then on SQLite with production options (workers, polling, leases, classifier, ID generator) | `memory`, `sqlstore` |
| [postgres-cluster](postgres-cluster) | Three workers on one database (SQLite, or Postgres with PG_DSN): each job once, a cron tick once, clean shutdown | `sqlstore` |
| [hatchet-worker](hatchet-worker) | What the Hatchet driver refuses before it connects, plus a live run when HATCHET_CLIENT_TOKEN is set | `hatchet` |
| [migrations-export](migrations-export) | Migrate and its ledger, MigrationScripts as .sql files applied by hand, UniqueStore.Migrate alone, ErrSchemaTooNew, WithSchema | `sqlstore` |
| [retention](retention) | WithRetention (ages, KeepLast, PerWorkflow, Undelivered), WithArchive, Prune, housekeeping options | `sqlstore` |
| [manage-runs](manage-runs) | List failed runs with their error, retry from the failed step, forget a finished run, cancel by filter, pause a workflow | `workflow`, `memory`, `clock` |
| [label-search](label-search) | Runs found by an exact label, a label value prefix and "has the key" on the SQLite label index, the IndexLabels backfill, Hatchet refusing a prefix | `workflow`, `sqlstore`, `hatchet` |
| [graceful-drain](graceful-drain) | Drain with a deadline, and WithStopWhenIdle for a batch-style process; both close Done | `workflow`, `memory`, `clock` |
| [graceful-drain-report](graceful-drain-report) | ShutdownHookReport draining within a grace and listing the attempts it cut off: cancelled on memory, requeued and rerun by a second worker on SQL | `workflow`, `memory`, `sqlstore` |
| [sql-queue-workers](sql-queue-workers) | Two SQL workers per queue, a pause shared through the database, list, retry and delete of a failed run, and drain | `sqlstore` |
| [hatchet-manage](hatchet-manage) | Queues served by their own Hatchet workers, run list, retry from the failed step, named schedules and their listing, drain (live part needs HATCHET_CLIENT_TOKEN) | `hatchet` |
| [hatchet-pause](hatchet-pause) | Workflow and schedule pause through Config.API and NewAPI, and the refusals without it (live part needs HATCHET_CLIENT_TOKEN) | `hatchet` |
| [admin](admin) | The admin JSON API behind a token login, failed-run retry, queue pause, a depth alert and the OpenTelemetry depth gauge | `admin`, `otel`, `memory` |
| [admin-ui](admin-ui) | The admin web UI behind a session cookie: closed to anonymous callers, the CSP, a label-prefix count, a viewer refused a schedule pause; serves for a browser with ADMIN_UI_ADDR | `admin`, `memory` |
| [worker-balancing](worker-balancing) | ByDepth and ByWait on fixed loads, per-queue bounds and MaxShift, a BalanceLoop moving workers to a backed-up queue | `workflow`, `memory`, `clock` |
| [worker-recycling](worker-recycling) | Worker generations recycled after two jobs on SQLite with the backlog kept, and a memory recycle through an injected reader | `workflow`, `memory`, `sqlstore`, `clock` |
| [enqueue-only](enqueue-only) | A web client with WithEnqueueOnly that never starts, two workers on one SQLite file, a step cut short by a clean stop finished on the same attempt | `sqlstore` |
| [worker-wakeup](worker-wakeup) | A worker polling every 5s started at once by a notifier: in-process on SQLite, pgnotify LISTEN/NOTIFY with PG_DSN | `sqlstore`, `sqlstore/pgnotify` |
| [redis-worker](redis-worker) | What the Redis driver declares and refuses; with REDIS_URL, a web client that only enqueues, two workers on one prefix, one run per key, a retried step, a schedule triggered by hand | `redis` |
| [run-repair](run-repair) | Each failed attempt's error with a stack for a panic, a failed run force-completed, a stuck run force-failed with a reason and no OnFailure | `workflow`, `memory`, `middleware` |
| [node-health](node-health) | Node identity, the node list and probe routes on memory; scheduler-only and worker-only nodes on Redis (REDIS_URL) | `workflow`, `memory`, `redis` |
| [hybrid-placement](hybrid-placement) | Crons on a scheduler client, runs on a jobs client, and what the split refuses | `hybrid`, `memory` |
| [admin-bulk](admin-bulk) | RetryAll and DeleteAll with dry run and Max, run-end charts from MemoryMetrics | `admin`, `memory` |
| [admin-cli](admin-cli) | The workflow CLI embedded with cli.Run against admin.NewHandler: bulk retry, health, nodes, drain, exit codes | `cli`, `admin`, `memory` |

### Middleware and observability

| Recipe | What it shows | Key packages |
|---|---|---|
| [middleware-stack](middleware-stack) | Step and enqueue middleware: chain order, Logging levels, Recover, Timeout, Skip, ThrottleErrors with a custom CircuitStore, FailOn, Route, Unique, and the refusal of Unique twice | `middleware`, `memory` |
| [encrypted-payloads](encrypted-payloads) | AES-GCM payload encryption with WithCodec and DecodeStep, key rotation, and the plaintext and dropped-key refusals | `middleware`, `memory` |
| [opentelemetry](opentelemetry) | Spans and metrics to an in-memory exporter: span names, parent or new root with a link, labels on spans and metrics, Enqueue twice refused | `otel`, `memory` |
| [run-finished](run-finished) | The run-finished hook: CapRunFinished, register before Start, success, failure and a cancelled delayed run; Hatchet part behind HATCHET_CLIENT_TOKEN | `workflow`, `memory`, `hatchet` |
| [failure-alerts](failure-alerts) | Alerts on final failures deduplicated for 10 minutes with a held-back count, and a reporter for every final and attempt failure | `middleware`, `memory`, `clock` |
| [sentry-reporting](sentry-reporting) | One Sentry event per failed run through an in-memory transport: tags, an opted-in label, the fingerprint, no payload on the wire | `sentry`, `middleware`, `memory` |
| [lifecycle-events](lifecycle-events) | The lifecycle feed on a fake clock, a failed run's RunError, Stuck reporting a hung attempt | `workflow`, `memory`, `middleware` |

### Batches

| Recipe | What it shows | Key packages |
|---|---|---|
| [batches](batches) | Batches with Then, Catch and Finally, progress, cancel, labels, WithIDFunc, callback retry; Hatchet coordinator with BatchSource behind HATCHET_CLIENT_TOKEN | `batch`, `memory`, `hatchet` |
| [batch-durable](batch-durable) | Batch state in SQLite with BatchStore, and a dispatch whose process stalled, resumed by another process | `batch`, `sqlstore` |
| [batch-lifecycle](batch-lifecycle) | Before and Progress callbacks, Add from a job, RetryFailed, AwaitStep in a workflow, EnqueueMany, Prune | `batch`, `memory`, `clock` |

### Reliability

| Recipe | What it shows | Key packages |
|---|---|---|
| [unique-jobs](unique-jobs) | Unique for a time, until finished, by workflow and arguments, across processes on SQLite | `middleware`, `sqlstore` |
| [debounce](debounce) | One run per burst, in-process and across two sqlstore clients | `middleware`, `sqlstore` |
| [claim-check](claim-check) | Big payloads and outputs via MemoryBlobStore, cleanup on finish | `middleware` |
| [transactional-enqueue](transactional-enqueue) | EnqueueTx commit, rollback and same key; Wake | `sqlstore` |
| [outbox](outbox) | Write in a tx, Relay, dead entries, ListDead, Requeue, AllowDuplicates | `sqlstore/outbox` |
| [failover](failover) | ErrKeyRequired, failover, cool-off, observer, Default vs Strict classifier | `failover` |
| [failover-schedules](failover-schedules) | Schedules on the primary only, or on both with WithSchedulesOnBoth: listing, ticks and pause per copy | `failover`, `memory`, `clock` |
| [failover-manage](failover-manage) | Runs merged across both backends with prefixed refs, RunQuery.Ref, Retry and Delete on the right backend, CancelAll and Pause on both | `failover`, `memory`, `clock` |
| [unique-states](unique-states) | UniqueWith States, ArgsTagged, ReturnExisting with WithEnqueueOutcome, an idempotency key's duplicate outcome | `workflow`, `memory`, `middleware` |
| [secret-guard](secret-guard) | SecretGuard refusing keys and PEM values, a SecretRef resolved in the step, SecretRules allow lists | `workflow`, `middleware`, `memory` |

### Testing and extending

| Recipe | What it shows | Key packages |
|---|---|---|
| [app-tests](app-tests) | Fake, assertions, matchers, Recorder, fake clock | `workflowtest`, `clock` |
| [batch-tests](batch-tests) | Fake batch and chain assertions, FakeExcept pass-through, RunStep, queue and retry-budget overrides, pause, retry and delete assertions | `workflowtest`, `batch`, `memory` |
| [custom-plugin](custom-plugin) | A plugin with enqueue middleware, step middleware and a run-finished observer, added with workflow.Use; a unique-lock store over any key-value service | `workflow`, `middleware`, `memory` |
| [client-wrapper](client-wrapper) | A hand-written client wrapper that keeps the optional interfaces reachable by embedding workflow.Forward, and what a wrapper without it hides | `workflow`, `memory` |
| [custom-driver](custom-driver) | A minimal driver of workflow.Client (under 300 lines) that refuses what it lacks and passes the conformance suite | `workflow`, `clock`, `workflowtest` |
| [run-all](run-all) | Memory RunAll: a delay, a backoff and a week-long sleep finish at once; waiting and stalled runs | `memory` |

## Writing an example

- `main.go` keeps `main` tiny and puts the program in `run(ctx, out io.Writer) error`, so the test drives the same code.
- Mark each region a guide copies with `// snippet:<name>` and `// snippet:end`; the guide puts `<!-- snippet: examples/<folder>/main.go#<name> -->` on the line before its code block.
- Examples that need a database use SQLite in memory so they run with nothing else installed; parts that need Postgres, MySQL or Hatchet skip when their environment variable is unset.

## Doc checkers

[internal/doccheck](internal/doccheck) holds the four checkers `task check` runs: `mdwrap` (no hard-wrapped prose), `mdlinks` (every relative link and anchor resolves), `snippets` (every marked code block matches its source region) and `words` (no forbidden word in the Markdown or the Go sources; the list lives in the `docs:words` task).
