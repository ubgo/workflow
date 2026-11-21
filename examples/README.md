# Examples

Runnable programs, one per task an app actually has. Each folder is a complete `main` package with a `README.md` that walks through it, and a `main_test.go` that runs the same code and checks its output, so a recipe that stops working fails `task check`. The guides under [docs/](../docs/) copy their Go code from marked regions of these programs, and `task docs:snippets` fails when a guide and its program drift apart.

This folder is its own Go module (`github.com/ubgo/workflow/examples`), so a database driver, the Hatchet SDK or OpenTelemetry that an example needs never becomes a dependency of the library. Run every example with `task examples:test`, or one with `go run ./quickstart` from this folder.

## Recipes

161 programs, grouped in the order the [learning path](../docs/README.md#learning-path) reaches them.

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
| [release-errors](release-errors) | RetryAfter marks a 429 as a release, IsRelease finds it through wrapping, Permanent wins, and MaxErrors stops on the second real error while releases count against Max only | `workflow`, `memory` |
| [delayed-jobs](delayed-jobs) | RunAt and ScheduledID, Status, cancel a delayed and a running run | `workflow`, `memory`, `clock` |
| [idempotency](idempotency) | One run per key, key freed at the end, optional/required/off, refusal without CapIdempotencyKey | `workflow`, `memory`, `workflowtest` |
| [consumer-inbox](consumer-inbox) | A message delivered three times processed once with InboxConsumer, a failed attempt run again, sqlstore Inbox.MarkTx writing one order from four concurrent deliveries, a webhook with an Inbox | `workflow`, `events`, `memory`, `sqlstore` |
| [concurrency](concurrency) | Per-key concurrency and strategies, rate limits, priority, KeyFunc vs KeyExpr refusals | `workflow`, `memory`, `clock`, `hatchet` |
| [cron-schedules](cron-schedules) | Cron syntax, Vixie rule, parse errors, DST, CronLocation, Schedule/Unschedule, upsert by Name on SQLite, misfire policies and Misfire.Due, WithCronParser, Hatchet refusals | `workflow`, `cron`, `memory`, `sqlstore`, `hatchet` |
| [signals-and-sleep](signals-and-sleep) | Wait steps with KeyFrom and Key, Signal, durable sleep, wait timeout plus OnFailure, fan-in with StepInput, StepInfo, step timeout, the key a wait step resolves, a Define handler and a StepInfo read called directly | `workflow`, `memory`, `clock`, `hatchet` |
| [named-queues](named-queues) | Workflow.Queue and Job.Queue, memory.WithQueues with priority and a worker cap per queue, pausing one queue while the others run | `workflow`, `memory` |
| [step-slots](step-slots) | Step.Slots on a pool of four: a three-slot render beside at most one webhook, a step bigger than the pool refused at Register, SlotPool driven by hand | `workflow`, `memory` |
| [accelerator-queue](accelerator-queue) | One step on GPU nodes: two SQL worker clients over one SQLite file, a gpu queue with one worker per card, AwaitChild from the cpu workflow with the SQL journal store | `workflow`, `sqlstore` |
| [retry-limits](retry-limits) | Job.Retry per dispatch, RetryPolicy.Until, MaxErrors letting releases through, Discard | `workflow`, `memory`, `clock` |
| [discard-exhausted](discard-exhausted) | The precedence Job.Retry over Step.Retry over Workflow.Retry over the default, and a run out of retries ending failed or, with DiscardOnExhaust, discarded with no OnFailure | `workflow`, `memory` |
| [release-jobs](release-jobs) | middleware.Release while a condition holds, and a unique lock freed when the run starts (UniqueUntilStarted) | `middleware`, `memory`, `clock` |
| [run-inline](run-inline) | workflow.RunInline: a workflow run once in the caller's goroutine, with retries, middleware and events, no client | `workflow` |
| [run-states](run-states) | StatusRetrying with the next attempt's number and time, StatusDiscarded after Discard, MaxErrors still ending failed, admin.Discarded, a batch's DiscardedAs, a switch with a default branch | `workflow`, `memory`, `clock`, `batch`, `admin` |
| [run-transitions](run-transitions) | The run state machine as data: the moves out of retrying with their triggers and capabilities, CanTransitionTo answers, the moves a driver can report by its capabilities, and a guard a driver of your own runs before storing a status | `workflow`, `memory` |
| [deferred-dispatch](deferred-dispatch) | DeferHTTP sending a request's jobs only on success, dropping them on a 402 or a panic, and a Deferred flushed or discarded by hand | `workflow`, `middleware`, `memory` |
| [chain-edits](chain-edits) | Job.Chain, PrependToChain and AppendToChain from a running step, a retried step adding its jobs once, a failed run ending the chain, the Hatchet refusal | `workflow`, `memory`, `hatchet` |
| [closures](closures) | A Go function run as an in-process job with retries, status and cancel, and the SQL and Hatchet refusals | `workflow`, `memory`, `sqlstore`, `hatchet` |
| [functions](functions) | A Go function registered by name with a JSON argument: retry and catch, an unregistered name refused, a delayed run surviving a restart on SQLite | `workflow`, `memory`, `sqlstore`, `clock` |
| [cron-builder](cron-builder) | Cron expressions from named frequencies, what the builder refuses, and one driving a workflow's cron | `cron`, `memory`, `clock` |
| [schedule-hooks](schedule-hooks) | Windows, ScheduleWhen, tick events, per-schedule hooks with heartbeat pings, schedule list and pause | `workflow`, `middleware`, `memory`, `clock` |
| [latest-only](latest-only) | Skip the work of stale scheduled runs with middleware.LatestOnly after a misfire catch-up, on a fake clock | `workflow`, `middleware`, `memory`, `clock` |
| [schedule-keep-runs](schedule-keep-runs) | The tick each run stands for, a cursor in the schedule's DataStore folder, KeepRuns and CronKeepRuns, and the embedded zone database, on a fake clock | `workflow`, `cron`, `cron/tzdata`, `memory`, `clock` |
| [task-output](task-output) | A nightly task's log lines and result captured, appended to a file every night and mailed when it fails | `workflow`, `middleware`, `memory`, `clock` |
| [schedule-groups](schedule-groups) | Schedules that share options built by one Go helper; the group label finds them in the list, filters their runs and pauses the group | `workflow`, `memory`, `clock` |
| [schedule-overlap](schedule-overlap) | A tick skipped while the previous run is unfinished, Run now refused then accepted with the manual label, the last fire with its run and status, and the same on the Fake | `workflow`, `memory`, `clock`, `workflowtest` |
| [output-store](output-store) | Captured output kept per attempt in the database with OutputStore, read back, and served to the admin through OutputSourceFunc | `sqlstore`, `middleware`, `admin` |
| [schedule-specs](schedule-specs) | Calendar specs, the seconds dialect, LoadZone, NextRuns in two zones across DST, an active period, environment gating, a schedule as JSON, a per-node LocalCron | `workflow`, `cron`, `clock` |
| [schedule-tools](schedule-tools) | Extended L/W/# cron, H hashing, Describe, exclusion calendars, backfill, limited and one-shot schedules with pruning, startup runs, fire jitter | `workflow`, `cron`, `memory`, `middleware`, `clock` |
| [schedule-export](schedule-export) | One schedule written as a Kubernetes CronJob, a systemd timer, a Task Scheduler task and a one-shot launchd handoff on a fixed clock, the typed refusals, and a seven-field cron with a year that stops | `schedexport`, `cron`, `clock` |
| [snooze-heartbeat](snooze-heartbeat) | Snooze without spending an attempt, a hung attempt failed by HeartbeatTimeout, RunTimeout running OnTimeout | `workflow`, `memory`, `clock` |
| [results-progress](results-progress) | ReportProgress and progress events, AwaitResult and DecodePayload, a two-sink result, RunFailedError, runs by tenant | `workflow`, `memory`, `clock` |
| [typed-payloads](typed-payloads) | JobOf, DecodePayload, MaxPayload and WithPayloadLimit, PayloadVersion with DefineVersioned and an upcaster | `workflow`, `memory`, `middleware` |
| [run-reschedule](run-reschedule) | Runs due in the next hour, Reschedule, CancelStep with and without a fallback; a moveRun helper with a cancel-and-enqueue fallback | `workflow`, `memory`, `clock` |
| [queue-weights](queue-weights) | Weighted queues on one worker with a seeded draw, FIFO per concurrency key | `workflow`, `memory` |
| [fair-share](fair-share) | Round robin across tenants and weighted shares (3:1) on one worker, the same order on memory and SQLite | `workflow`, `memory`, `sqlstore` |
| [queue-limit](queue-limit) | QueueConfig.Limit capping a queue across clients: peak concurrency within the limit on memory and on two SQL nodes sharing one SQLite file | `workflow`, `memory`, `sqlstore` |
| [upstream-limits](upstream-limits) | A leaky bucket on a fake clock with Take, Observe, Wait and TakeOrSnooze; PeriodKey in Berlin and OncePerPeriod | `workflow`, `memory`, `clock` |
| [flow-memo](flow-memo) | Memo around a charge on a retried step, MapEach and Loop rerunning only what failed, ProcessChunks with progress, SetRunData/RunData, a Choice, ErrNoJournal | `workflow`, `memory` |
| [scoped-data](scoped-data) | Folder, workflow and global data shared across runs with SetData, GetData and UpdateData; the in-memory store, then sqlstore.DataStore on SQLite with two clients adding to one total | `workflow`, `memory`, `sqlstore` |
| [flow-children](flow-children) | Spawn with parent labels, AwaitChildren joining results in order without spending parent attempts, a failed child failing the parent | `workflow`, `memory` |
| [flow-stages](flow-stages) | Stages of parallel steps, a conditional step passing its input on, a milestone holding a step until the run is paid, a soft deadline before the hard timeout | `workflow`, `memory`, `middleware` |
| [flow-joins](flow-joins) | The first mirror to answer wins and the rest are cancelled, two of four make a quorum, a paged map of ten products with a failure budget, a multi-merge starting one notification per branch | `workflow`, `memory` |
| [flow-cancel](flow-cancel) | CancelChildren cancelling every render a step spawned (listed by ChildLabels), CancelSteps withdrawing two hanging feeds that fall back so the run goes on, a SignalWaiting nudge kept for no later run, "paid and packed, or cancelled" as a child run answering the parent's race | `workflow`, `memory` |
| [flow-promises](flow-promises) | A run waiting on a promise an API call resolves, SignalWithStart, an update answered by the run, a loop continued as new runs | `workflow`, `memory` |
| [virtual-objects](virtual-objects) | A cart per customer: serial calls per key, durable state, ObjectState, a key's calls listed by LabelObjectKey, an unknown method failing its call | `workflow`, `memory` |
| [escalation](escalation) | An order paid by its ID with SignalRun and an OwnRun wait, a receipt check escalating to its expense report with SignalParent, a migration hearing its slow shards between steps with OnSignals while it joins them, approvals, a race and an update answered by run ID with ApproveRun, RejectRun, SignalChoiceRun and UpdateRun | `workflow`, `memory` |
| [escalation-wrappers](escalation-wrappers) | An order paid by its prefixed ref with SignalRun behind regional, failover, cutover and hybrid; a plugin binding its client with ClientBinder to read a run's pending signals; PendingSignals and TakeSignal by hand; the RunEscalationFlows suite in its test | `workflow`, `memory`, `regional`, `failover`, `cutover`, `hybrid`, `workflowtest` |
| [flow-replay](flow-replay) | A refund as one function with locals across a three-day sleep and a promise an approver resolves, each recorded call run once over four wakes, a recorded permanent error taking the same branch on every wake, Replaying for once-only logs, code branching on an unrecorded flag failing with NondeterminismError, a payout awaiting a child run with ReplayChild (spawned once over three wakes) and sleeping until pay day with SleepUntil | `workflow`, `memory`, `clock` |
| [state-machine](state-machine) | An order statechart per key: a guard, entry, exit and transition actions, a parallel payment and shipping state joined by a done event, deep history after a hold, a 30-minute timer on a fake clock, a rejected event, MachineStatus and the log | `workflow`, `memory`, `clock` |
| [machine-dsl](machine-dsl) | A state machine defined in JSON: guards and actions registered by name, dsl.LoadMachine, compound states, a 48-hour timer on a fake clock, all three history modes built from edited definitions with dsl.BuildMachine, an unknown guard refused with its JSON path, a MarshalMachine round trip, ForgetMachine | `dsl`, `workflow`, `memory`, `clock` |
| [workflow-json](workflow-json) | A workflow defined in JSON: handlers registered by name, dsl.Load, a wait keyed by a label, two orders run on memory, an unknown handler refused with its JSON path, a Marshal and Parse round trip, dsl.Parse refusing a misspelled field with its JSON path (FieldError) | `dsl`, `workflow`, `memory` |
| [partitioned-runs](partitioned-runs) | A backfill run per day with a status per partition, a retry of only the failed day, a set enqueued twice while running starting each tenant once | `workflow`, `memory` |
| [flow-patterns](flow-patterns) | Trigger rules over a tolerated step with an exit handler, a saga with compensation inside one step and across steps, a multi-choice with a local merge, a race decided by the first signal and one with a deadline default, a polling sensor, a sleep until a time, an approval with a signed link, a step group, an early end, a recursion limit, typed enqueue and result, a shape guard | `workflow`, `memory` |
| [flow-boundary](flow-boundary) | A non-interrupting Boundary timer reminding about a slow step, a missed deadline routed to a fallback step by a trigger rule, setup and teardown with a Finally step and run data | `workflow`, `memory` |
| [then-redo](then-redo) | Then and redo runs: a reset and a fork of a succeeded import carry LabelRedo and start no second index, ThenAfterRedo starting one per redo, a failing rule's ThenError sent to ThenErrors | `workflow`, `memory` |
| [edit-run](edit-run) | RunEditor: a scheduled run gets a new payload, a higher priority and an earlier start before it runs; an edit after it ran is refused; the same edit through Admin.EditRun and POST /runs/{id}/edit behind an Authorizer | `workflow`, `memory` |
| [run-control](run-control) | Runs found by a field of their JSON payload, one run suspended between steps and resumed, a schedule tick replacing the running run (GuardOverlap), a duplicate replacing a scheduled job's arguments (UniqueConfig.Replace); a detour: hold, resume, reset from the failed step | `workflow`, `memory`, `middleware`, `clock` |
| [job-guards](job-guards) | Expiry, a sweep of waiting runs, a bounded queue and a tenant quota, quiet hours, payload validation, a retry ladder, an invariant and a precondition, a pool, a critical section, auto-disable and the switch shared by two workers through `sqlstore.DisableStore` | `workflow`, `middleware`, `memory`, `sqlstore`, `clock` |
| [queue-guards](queue-guards) | Bounded and Quota with counts of the app's own: a DepthFunc that bounds each workflow's waiting runs, a CountFunc reading a tenant table, and a failing count that fails the enqueue | `middleware`, `memory` |
| [http-status](http-status) | httprun: start a run over HTTP (202 and Location), its status and result, a Server-Sent Events stream with output streamed by Emit | `httprun`, `workflow`, `memory` |
| [push-delivery](push-delivery) | A step pushing each job to an HTTP endpoint signed with Standard Webhooks, push.Handler running it on the receiving side, a 500 retried, a 422 failing for good | `push`, `workflow`, `events`, `memory` |
| [events-webhooks](events-webhooks) | A Bus with filtered subscriptions and dedup by event ID, a webhook handler's 405/401/413/200/503 answers with HMAC, Standard Webhooks refusing a changed body and a replay | `events`, `workflow`, `memory`, `clock` |
| [events-more](events-more) | CloudEvents over HTTP, payload versions upcast, an event log and Replay, cancel on event and divert, workflow.Then starting the next workflow | `events`, `workflow`, `memory`, `clock` |
| [events-signal](events-signal) | What a bus reports beyond starting runs: WithSignal waking a waiting run, ReplayResult with a failed delivery, Cancellation, ValidExtensionName | `events`, `memory` |
| [saga-choreography](saga-choreography) | Three services that only react to events: a declined card compensated by payment.failed then order.cancelled then a stock release, and events.Trail listing each order's runs as a tree of events | `events`, `workflow`, `memory` |
| [datasets](datasets) | Steps announcing dataset versions, an any-of audit and an all-of report trigger, a retried step starting nothing, lineage of producers and inputs | `events`, `workflow`, `memory` |
| [datasets-shared](datasets-shared) | MemoryLineage on its own, and two processes sharing sqlstore.LineageStore on one SQLite file | `events`, `sqlstore` |

### Drivers and operations

| Recipe | What it shows | Key packages |
|---|---|---|
| [sqlite-worker](sqlite-worker) | One workflow on memory, then on SQLite with production options (workers, polling, leases, classifier, ID generator) | `memory`, `sqlstore` |
| [sql-options](sql-options) | The SQL driver's everyday options: a dialect per database, workers and Wake after EnqueueTx, an error classifier with WithTxAttempts, WithClock/WithLogger/WithMiddleware/WithJitterSource, Client.DB, and NewDataStore/NewJournalStore without a client | `sqlstore` |
| [postgres-cluster](postgres-cluster) | Three workers on one database (SQLite, or Postgres with PG_DSN): each job once, a cron tick once, clean shutdown | `sqlstore` |
| [sql-leader-election](sql-leader-election) | Leader election (WithLeaderElection, NewLeaderElector, WithElector), the database clock with WithLeaseSkew, and leases: an ErrLeaseLost fence, a takeover, ErrAbandoned with WithMaxReclaims(0) | `sqlstore` |
| [memory-options](memory-options) | Every memory driver option on a fake clock: pool, clock, logger, middleware, pinned jitter, named queues, run retention, stop when idle, the schedule heap | `workflow`, `memory`, `clock` |
| [rolling-deploy](rolling-deploy) | Workflow versions while old and new workers share one SQLite file: a v2 run waits for a v2 worker, which also drains a v1 run and branches on StepInfo.Version | `workflow`, `sqlstore` |
| [hatchet-worker](hatchet-worker) | What the Hatchet driver refuses before it connects, plus a live run when HATCHET_CLIENT_TOKEN is set | `hatchet` |
| [hatchet-schedules](hatchet-schedules) | Hatchet defaults, Config, an enqueue-only client, the OverlapSkip refusal, the quirk settings; with a token TriggerSchedule, last fire, a web-node enqueue and ErrCancelTooLate | `workflow`, `hatchet` |
| [migrations-export](migrations-export) | Migrate and its ledger, MigrationScripts as .sql files applied by hand, UniqueStore.Migrate alone, migrations per owner constant (outbox included), ErrSchemaTooNew, WithSchema | `sqlstore` |
| [retention](retention) | WithRetention (ages, KeepLast, PerWorkflow, Undelivered), an ArchiveFunc reading errors and step outputs, a failing archive that keeps the chunk, Prune, housekeeping options | `sqlstore` |
| [sql-archive](sql-archive) | WithArchiveTable moving pruned runs to cold tables, ArchivedRuns paging with FinishedBefore, WithFetchBatch, WithIDGenerator per IDKind | `sqlstore` |
| [insert-contract](insert-contract) | Runs written by plain SQL as another language would (due now, delayed, keyed), then run by a Go worker | `sqlstore` |
| [manage-runs](manage-runs) | List failed runs with their error, retry from the failed step, forget a finished run, cancel by filter, pause a workflow; reset from an earlier step, each operation through a capability check and type assertion | `workflow`, `memory`, `clock` |
| [reset-fork](reset-fork) | Reset a run that succeeded from a step, fork a run into a new run from a step; Then does not fire twice | `workflow`, `memory` |
| [label-search](label-search) | Runs found by an exact label, a label value prefix and "has the key" on the SQLite label index, the IndexLabels backfill, Hatchet refusing a prefix | `workflow`, `sqlstore`, `hatchet` |
| [graceful-drain](graceful-drain) | Drain with a deadline, and WithStopWhenIdle for a batch-style process; both close Done | `workflow`, `memory`, `clock` |
| [graceful-drain-report](graceful-drain-report) | ShutdownHookReport and, by hand, DrainContext + DrainWithReport + DrainOutcome; the report's dispositions: cancelled on memory, requeued (CapShutdownRequeue) and rerun by a second worker on SQL | `workflow`, `memory`, `sqlstore` |
| [sql-queue-workers](sql-queue-workers) | Two SQL workers per queue, a pause shared through the database, list, retry and delete of a failed run, and drain | `sqlstore` |
| [hatchet-manage](hatchet-manage) | Queues served by their own Hatchet workers, run list, retry from the failed step, named schedules and their listing, drain (live part needs HATCHET_CLIENT_TOKEN) | `hatchet` |
| [hatchet-pause](hatchet-pause) | Workflow and schedule pause through Config.API and NewAPI, and the refusals without it (live part needs HATCHET_CLIENT_TOKEN) | `hatchet` |
| [admin](admin) | The admin JSON API behind a token login, failed-run retry, queue pause, a depth alert and the OpenTelemetry depth gauge | `admin`, `otel`, `memory` |
| [admin-routes](admin-routes) | Every admin JSON API route called once with a summary of its answer, then each error code mapped to what a client does next | `admin`, `workflow`, `memory`, `middleware` |
| [admin-inspect](admin-inspect) | Runs page by page, failures and discarded runs, counts, queue stats with a bounded scan, schedules with their last fire, nodes, health and captured output, from Go | `admin`, `workflow`, `memory` |
| [admin-reset-fork](admin-reset-fork) | Reset, reset a succeeded run, fork, signal the runs waiting now, cancel a step, force complete and fail, edit and reschedule a delayed run from Go, plus workflow.Supports behind a wrapper | `admin`, `workflow`, `memory` |
| [admin-alerts](admin-alerts) | admin.Monitor with both alert kinds, WithRepeat, WithErrorHandler and Run on fake clocks | `admin`, `workflow`, `memory`, `clock` |
| [admin-ui](admin-ui) | The admin web UI behind a session cookie: closed to anonymous callers, the CSP, a label-prefix count, a viewer refused a schedule pause; serves for a browser with ADMIN_UI_ADDR | `admin`, `memory` |
| [worker-balancing](worker-balancing) | ByDepth and ByWait on fixed loads, per-queue bounds and MaxShift, a BalanceLoop moving workers to a backed-up queue | `workflow`, `memory`, `clock` |
| [worker-recycling](worker-recycling) | Worker generations recycled after two jobs on SQLite with the backlog kept, and a memory recycle through an injected reader | `workflow`, `memory`, `sqlstore`, `clock` |
| [enqueue-only](enqueue-only) | A web client with WithEnqueueOnly that never starts, two workers on one SQLite file, a step cut short by a clean stop finished on the same attempt | `sqlstore` |
| [recorder-split](recorder-split) | A run recorder across an enqueue-only web node (EnqueuePlugin) and a worker (Plugin) on one SQLite file: one record with the web node's source and input and the worker's step, event and end | `recorder`, `sqlrecorder`, `sqlstore` |
| [worker-wakeup](worker-wakeup) | A worker polling every 5s started at once by a notifier: in-process on SQLite, pgnotify LISTEN/NOTIFY with PG_DSN | `sqlstore`, `sqlstore/pgnotify` |
| [redis-worker](redis-worker) | What the Redis driver declares and refuses; with REDIS_URL, a web client that only enqueues, two workers on one prefix, one run per key, a retried step, a schedule triggered by hand | `redis` |
| [redis-parity](redis-parity) | What the Redis driver declares and refuses; with REDIS_URL every worker option, a durable wait, a concurrency key across clients, a rate limit, a queue, a pause, run-finished, a chain, ScheduleWhen, clean-stop requeue and the standalone shared stores | `workflow`, `redis`, `clock`, `cron` |
| [run-repair](run-repair) | Each failed attempt's error with a stack for a panic, a failed run force-completed, a stuck run force-failed with a reason and no OnFailure | `workflow`, `memory`, `middleware` |
| [node-health](node-health) | Node identity, the node list and probe routes on memory; scheduler-only and worker-only nodes on Redis (REDIS_URL) | `workflow`, `memory`, `redis` |
| [start-gate](start-gate) | StartWhen with ClockAfter on a device whose clock boots at 1970, ErrNotStarted until the gate opens, a ReadyFunc whose error ends the wait, a ctx deadline | `workflow`, `memory`, `clock` |
| [sleep-wake](sleep-wake) | clock.WakeChecked on a simulated laptop that sleeps: a plain clock runs a 23:00 reminder 30 minutes after the resume, a wake-checked one within one check | `clock`, `memory` |
| [hybrid-placement](hybrid-placement) | Crons on a scheduler client, runs on a jobs client, and what the split refuses | `hybrid`, `memory` |
| [regional-routing](regional-routing) | One client per region, ByLabel and RouterFunc routing, region-prefixed refs, unknown region refused, Signal across regions, WithSignalRouter keeping an event in one region, schedules placed by the router, WithScheduleRegion for declared Cron, Runs merged | `regional`, `memory` |
| [namespaces](namespaces) | Two environments on one database: WithTablePrefix per namespace, runs and schedules invisible across them, bad prefix and WithSchema on SQLite refused; WithPrefix on Redis with REDIS_URL | `sqlstore`, `redis` |
| [backend-cutover](backend-cutover) | Move to a new backend while running: new jobs on the new backend, old refs still answering, Runs over both, schedules moved (cutover.Moved), SchedulesOn(SideOld), Remaining | `cutover`, `workflow`, `memory` |
| [admin-bulk](admin-bulk) | RetryAll and DeleteAll with dry run and Max, run-end charts from MemoryMetrics | `admin`, `memory` |
| [admin-cli](admin-cli) | The workflow CLI embedded with cli.Run against admin.NewHandler: bulk retry, health, nodes, drain, exit codes | `cli`, `admin`, `memory` |
| [cli-tour](cli-tour) | Every workflow CLI command run once, embedded as an app's admin subcommand, with the environment variables and exit codes 0, 1 and 2 | `cli`, `admin`, `memory` |
| [admin-tenants](admin-tenants) | Admin API scoped per tenant with WithTenantScope: own runs only, 404 across tenants, 403 for system routes, AllTenants operator | `admin`, `memory` |
| [admin-audit](admin-audit) | Admin audit log of every write action (a refused one included) and a trace link per run from otel.TraceIDOf | `admin`, `otel`, `memory`, `clock` |

### Middleware and observability

| Recipe | What it shows | Key packages |
|---|---|---|
| [middleware-stack](middleware-stack) | Step and enqueue middleware: chain order, Logging levels, Recover, Timeout, Skip, ThrottleErrors with a custom CircuitStore, FailOn, Route, Unique, and the refusal of Unique twice | `middleware`, `memory` |
| [step-middleware](step-middleware) | Recover, Timeout, FailOn and Skip one at a time, each on a client of its own: a panic that a retry policy refuses to retry (stack kept in the error history), a step stopped by the timeout, a declined card that fails at once beside a busy gateway that keeps its retries, a step skipped by its input | `middleware`, `memory` |
| [route-jobs](route-jobs) | middleware.Route as a routing table: bulk invoices to a workflow of their own, a default cost-center label, the enqueue site's label winning, the caller's map left unchanged | `middleware`, `memory` |
| [encrypted-payloads](encrypted-payloads) | AES-GCM payload encryption with WithCodec and DecodeStep, key rotation, and the plaintext and dropped-key refusals | `middleware`, `memory` |
| [opentelemetry](opentelemetry) | Spans and metrics to an in-memory exporter: span names, parent or new root with a link, labels on spans and metrics, Enqueue twice refused | `otel`, `memory` |
| [tenant-spans](tenant-spans) | otel.WithTenant: Job.Tenant as the workflow.tenant attribute on the enqueue and step spans, off by default, on an in-memory span exporter | `workflow`, `memory`, `middleware`, `otel` |
| [message-attributes](message-attributes) | A traced job crossing an HTTP hop as headers with workflow.Attributes and its step continuing the producer's trace; the same job as SQS attributes, packed labels, a Pub/Sub refusal | `workflow`, `otel`, `memory` |
| [run-recorder](run-recorder) | A run history the app owns: the recorder on the memory driver writing to SQLite through sqlrecorder, a required trigger source, step events, a partial outcome, a redacted card number, reads by run, by source and outcome, and through the admin API, and a Reconciler pass | `workflow`, `recorder`, `recorder/sqlrecorder`, `middleware`, `admin`, `memory` |
| [recorder-sql](recorder-sql) | The run history in SQL with sqlrecorder on SQLite in memory: dialects, table prefix, contention retries, two-stage retention with an archive hook, PruneEvery | `recorder`, `recorder/sqlrecorder`, `memory` |
| [recorder-custom-store](recorder-custom-store) | A recorder.Store of your own on the shared merge rules, proven with recordertest; a Redactor of your own, the error hook for failed writes, a tuned Reconciler | `recorder`, `recorder/recordertest`, `workflowtest`, `memory` |
| [trigger-source](trigger-source) | WithSource on a job, the schedule and schedule-manual sources SourceOf derives from a cron fire and TriggerSchedule, RequireSource refusing a job with no source and a typo outside its allow-list, ValidateSource | `workflow`, `middleware`, `memory`, `clock` |
| [step-events](step-events) | RecordEvent and RecordEventLevel reaching a sink of the app's own and CaptureOutput, SetOutcome unsupported without a recorder, then the events and a partial outcome stored by a recorder, and the default outcome of a run that set none | `workflow`, `middleware`, `memory`, `recorder` |
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
| [batch-completion](batch-completion) | Where a batch hears that its jobs ended: the status poller on a client without the run-finished hook, an app's own batch.Source, and Manager.RunFinished ignoring runs outside any batch | `batch`, `memory`, `clock` |

### Reliability

| Recipe | What it shows | Key packages |
|---|---|---|
| [unique-jobs](unique-jobs) | Unique for a time, until finished, by workflow and arguments, across processes on SQLite | `middleware`, `sqlstore` |
| [debounce](debounce) | One run per burst, in-process and across two sqlstore clients | `middleware`, `sqlstore` |
| [aggregate-digest](aggregate-digest) | Seven comments become three digest mails with AggregatePlugin, and a chunked fan-out to 25 recipients that resumes | `middleware`, `batch`, `workflow`, `memory` |
| [claim-check](claim-check) | Big payloads and outputs via MemoryBlobStore, cleanup on finish | `middleware` |
| [transactional-enqueue](transactional-enqueue) | EnqueueTx commit, rollback and same key; Wake | `sqlstore` |
| [complete-in-transaction](complete-in-transaction) | A step completed inside the app's transaction with sqlstore CompleteTx: the payment row and the step's success commit together, a declined card rolls both back | `sqlstore`, `workflow` |
| [outbox](outbox) | Write in a tx, Relay, dead entries, ListDead, Requeue, AllowDuplicates | `sqlstore/outbox` |
| [change-triggers](change-triggers) | Row changes made by plain SQL become jobs through a trigger generated by outbox.ChangeTrigger, carrying only the chosen columns | `sqlstore/outbox`, `workflow`, `memory` |
| [failover](failover) | ErrKeyRequired, failover, cool-off, observer, Default vs Strict classifier | `failover` |
| [failover-schedules](failover-schedules) | Schedules on the primary only, or on both with WithSchedulesOnBoth: listing, ticks and pause per copy | `failover`, `memory`, `clock` |
| [failover-manage](failover-manage) | Runs merged across both backends with prefixed refs, RunQuery.Ref, Retry and Delete on the right backend, CancelAll and Pause on both | `failover`, `memory`, `clock` |
| [failover-observer](failover-observer) | Tune and watch a failover client: your own outage classifier, an observer counting failover events, the cool-off, reset, edit and transient signals on either backend, schedules on both | `failover`, `memory`, `clock` |
| [unique-states](unique-states) | UniqueWith States, ArgsTagged, ReturnExisting with WithEnqueueOutcome, an idempotency key's duplicate outcome | `workflow`, `memory`, `middleware` |
| [secret-guard](secret-guard) | SecretGuard refusing keys and PEM values, a SecretRef resolved in the step, SecretRules allow lists | `workflow`, `middleware`, `memory` |

### Testing and extending

| Recipe | What it shows | Key packages |
|---|---|---|
| [app-tests](app-tests) | Fake, assertions, matchers, Recorder, fake clock | `workflowtest`, `clock` |
| [batch-tests](batch-tests) | Fake batch and chain assertions, FakeExcept pass-through, RunStep, queue and retry-budget overrides, pause, retry and delete assertions | `workflowtest`, `batch`, `memory` |
| [fake-forks](fake-forks) | Test app code with the Fake and no worker: forks, resets, signals to waiting runs, promises, virtual object calls and batches, read back and asserted | `workflow`, `workflowtest`, `batch` |
| [custom-plugin](custom-plugin) | A plugin with enqueue middleware, step middleware and a run-finished observer, added with workflow.Use; a unique-lock store over any key-value service | `workflow`, `middleware`, `memory` |
| [client-wrapper](client-wrapper) | A hand-written client wrapper that keeps the optional interfaces reachable by embedding workflow.Forward, and what a wrapper without it hides | `workflow`, `memory` |
| [custom-driver](custom-driver) | A minimal driver of workflow.Client (under 300 lines) that refuses what it lacks and passes the conformance suite, plus the core helpers a driver calls for the capabilities it adds | `workflow`, `clock`, `workflowtest` |
| [driver-conformance](driver-conformance) | Split the conformance suite into shares checked against GroupNames, and run every core-helper case (Run*Flows) with its harness, on the memory driver | `workflowtest`, `memory`, `clock` |
| [run-all](run-all) | Memory RunAll: a delay, a backoff and a week-long sleep finish at once; waiting and stalled runs | `memory` |

## Writing an example

- `main.go` keeps `main` tiny and puts the program in `run(ctx, w io.Writer) error`, so the test drives the same code: `main` passes `os.Stdout`, the test passes a buffer and compares what was written. Older examples use `run(ctx) error` and print to standard output; their test reads it with `capture.Stdout` from [internal/capture](internal/capture). Both shapes are fine; a new example takes the writer.
- Every error is handled, deferred ones included: a `Close` or `os.RemoveAll` joins the function's error with `errors.Join`, so `volt ci` (errcheck) stays clean without exclusions.
- Mark each region a guide copies with `// snippet:<name>` and `// snippet:end`; the guide puts `<!-- snippet: examples/<folder>/main.go#<name> -->` on the line before its code block.
- Examples that need a database use SQLite in memory so they run with nothing else installed; parts that need Postgres, MySQL or Hatchet skip when their environment variable is unset.

## Doc checkers

[internal/doccheck](internal/doccheck) holds the four checkers `task check` runs: `mdwrap` (no hard-wrapped prose), `mdlinks` (every relative link and anchor resolves), `snippets` (every marked code block matches its source region) and `words` (no forbidden word in the Markdown or the Go sources; the list lives in the `docs:words` task).
