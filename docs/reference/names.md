# Names

Every name the library reserves or writes on your behalf: the job labels its packages stamp, the tables and migration owners in your database, the metadata keys on a Hatchet run, the IDs it generates, and the span, attribute and metric names it exports. Keep app names out of these spaces; where a collision would break something, the library refuses it with an error rather than overwriting or dropping your value.

## Labels

`Job.Labels` are the app's own key-value pairs, carried onto the run and back on every `RunEvent`. Three prefixes are reserved, each owned by one package, which stamps labels under it on the jobs it passes down:

| Prefix | Constant | Owner | Keys it writes |
|---|---|---|---|
| `ubgo.workflow/` | `workflow.LabelPrefixInternal` | `middleware` | `ubgo.workflow/unique-key` (`middleware.UniqueKeyLabel`) and `ubgo.workflow/unique-owner` (`middleware.UniqueOwnerLabel`), by `Unique`; `ubgo.workflow/claim-check` (`middleware.ClaimCheckLabel`), by `ClaimCheck.Enqueue`. `ubgo.workflow/closure` (`workflow.LabelClosure`), the closure's name on every closure run. Owned by core: `ubgo.workflow/schedule` (`workflow.LabelSchedule`, the schedule ID) and `ubgo.workflow/schedule-name` (`workflow.LabelScheduleName`), stamped by every driver with `CapTickEvents` on each run a schedule fires (`workflow.FiredLabels`), and `ubgo.workflow/trigger=manual` (`workflow.LabelTrigger`, `LabelTriggerManual`) on a run `TriggerSchedule` started (`workflow.TriggeredLabels`); `ubgo.workflow/tick-at` (`workflow.LabelTickAt`), the tick a fired run stands for, by drivers with `CapTickTime` (`workflow.FiredLabelsAt`); `ubgo.workflow/payload-version` (`workflow.LabelPayloadVersion`), by `middleware.PayloadVersion`; `ubgo.workflow/parent-run` and `ubgo.workflow/parent-step` (`workflow.LabelParentRun`, `LabelParentStep`), by `Spawn` on each child run; `ubgo.workflow/event`, `ubgo.workflow/event-id` and `ubgo.workflow/subscription` (`events.LabelEvent`, `LabelEventID`, `LabelSubscription`), by an `events.Bus` on each run it starts; `ubgo.workflow/replay` (`events.LabelReplay`), by `Bus.Replay` on each run a replay starts; `ubgo.workflow/correlation` and `ubgo.workflow/causation` (`events.LabelCorrelation`, `LabelCausation`), by an `events.Bus` with `WithCorrelation` on each run it starts; `ubgo.workflow/dataset-trigger` (`events.LabelDatasetTrigger`), by a `DatasetTrigger` on each run it starts. Also owned by core: `ubgo.workflow/continued-from` (`workflow.LabelContinuedFrom`), by `ContinueAsNew`; `ubgo.workflow/object-key` (`workflow.LabelObjectKey`), by `CallObject` (also by `SendEvent` and `StartMachine`, the state machine's instance key); `ubgo.workflow/partition-set` and `ubgo.workflow/partition-key` (`workflow.LabelPartitionSet`, `LabelPartitionKey`), by `EnqueuePartitions`; `ubgo.workflow/run-key` (`workflow.LabelRunKey`), by `Backfill` and `FireAtStartup`, with `LabelTrigger` values `backfill` (`LabelTriggerBackfill`) and `startup` (`LabelTriggerStartup`); `ubgo.workflow/depth` (`workflow.LabelDepth`), by `RecursionLimit`; `ubgo.workflow/shape` (`workflow.LabelShape`), by `ShapeGuard`; `ubgo.workflow/then-from` (`workflow.LabelThenFrom`), by `Then`; `ubgo.workflow/expires-at` (`middleware.LabelExpiresAt`), by `ExpireAt`, `ExpireAfter` and `Expire` `ubgo.workflow/redo` (`workflow.LabelRedo`), on a run reset after it succeeded and on a fork of a succeeded run; `ubgo.workflow/fork-of` (`workflow.LabelForkOf`) and `ubgo.workflow/fork-step` (`workflow.LabelForkStep`), on a fork; by the drivers' `ResetRunWith` and `ForkRun`. `ubgo.workflow/run:<RunID>` is the event key that addresses one run (`workflow.RunKey`), used by `SignalRun`, `SignalParent` and `OwnRun` waits; not a label, but under the same reserved prefix so an app key never collides with it. |
| `workflow.batch.` | `workflow.LabelPrefixBatch` | `batch` | `workflow.batch.id` (`batch.LabelBatch`) and `workflow.batch.index` (`batch.LabelIndex`), on every job of a batch; `workflow.batch.attempt` (`batch.LabelAttempt`), on the jobs `Manager.RetryFailed` starts again (`batch.JobLabels` builds all three) |
| `otel.` | `workflow.LabelPrefixOTel` (`otel.LabelPrefix`) | `otel` | The trace context carried from `Enqueue` to the step: with the default W3C propagator, `otel.traceparent` and, when the trace has state, `otel.tracestate` |

`workflow.ReservedLabelPrefixes()` returns the three as a fresh slice, `workflow.ReservedLabel(key)` says whether a key falls under one and which, and `workflow.ValidateLabels(labels)` refuses app labels under any of them with `workflow.ErrInvalid`. `ValidateSchedule` calls it, and each owning package refuses incoming labels under its own prefix, so an app label can never be mistaken for one the library wrote. Every driver's public `Enqueue` (and `EnqueueTx`, `EnqueueMany` and group dispatch) calls `workflow.CheckJobLabels(ctx, job)`, which refuses a reserved label on a job or a chained job with `workflow.ErrInvalid` unless the package that owns it allowed it on the context (`workflow.AllowReservedLabels`, `AllowReservedPrefix`); internal paths (a fired schedule, a chained job, a retry) carry labels the library wrote and are not checked. It guards against a reserved label typed by an app or copied from a request, not against code in the same process that calls the allow functions itself.

The Hatchet driver stores labels as the run's additional metadata, beside keys of its own and of Hatchet's. A label that would shadow one of them is refused with an `UnsupportedError` on `Job.Labels`:

| Reserved by | Keys |
|---|---|
| The Hatchet driver | Every key starting `wf__`: `wf__idempotency_key`, `wf__workflow`, `wf__schedule_token`, `wf__enqueued_at`, `wf__queue`, `wf__priority` |
| Hatchet and its SDK | Every key starting `hatchet__`, and `traceparent` |

## Tables

The SQL driver and its stores create these tables in your database. `<prefix>` is `wf_` by default and is set with `WithTablePrefix` (it must match `^[a-z][a-z0-9_]{0,30}$`); on Postgres, `WithSchema` puts the tables in a schema of their own.

| Table | Created by | Holds |
|---|---|---|
| `<prefix>runs` | `sqlstore.Client.Migrate` | One row per run |
| `<prefix>steps` | `Client.Migrate` | One row per step of a run, with its lease |
| `<prefix>signals` | `Client.Migrate` | Signals sent, matched to waits by name and key |
| `<prefix>consumed` | `Client.Migrate` | Which run took which signal |
| `<prefix>schedules` | `Client.Migrate` | Declared crons and `Schedule` calls |
| `<prefix>ticks` | `Client.Migrate` | One row per cron tick, so a tick fires once across nodes |
| `<prefix>locks` | `Client.Migrate` | Lock rows that serialise admission per workflow; on SQLite also `claim:fair`, created at `Start` when a round-robin workflow is registered, which queues fair claims on SQLite's one writer |
| `<prefix>starts` | `Client.Migrate` | Start times for rate limits |
| `<prefix>batches` | `Client.Migrate` | The durable `batch.Store` (`Client.BatchStore`) |
| `<prefix>schedule_misfire` | `Client.Migrate` | The misfire policy of schedules that set one |
| `<prefix>unique_locks` | `Client.Migrate`, or `UniqueStore.Migrate` alone | The locks of `sqlstore.UniqueStore` |
| `<prefix>debounce` | `Client.Migrate`, or `DebounceStore.Migrate` alone | The bursts of `sqlstore.DebounceStore` |
| `<prefix>run_ext` | `Client.Migrate` (migration `003_queues_admin`) | Per run: its queue, idempotency key, per-job retry policy, start time and whether it was discarded; backfilled for runs written before 003 |
| `<prefix>step_ext` | `Client.Migrate` (`003_queues_admin`) | Per step: the error count and first attempt time that `RetryPolicy.MaxErrors` and `Until` read |
| `<prefix>pauses` | `Client.Migrate` (`003_queues_admin`) | Durable workflow, queue and global pauses (`Pauser`), shared by every client on the database |
| `<prefix>schedule_ext` | `Client.Migrate` (`003_queues_admin`) | Per schedule: its windows and whether it is paused |
| `<prefix>step_err` | `Client.Migrate` (`004_run_states_chain`) | Per step in retry backoff: the last failed attempt's error, which `RunInfo.Error` shows while the run is retrying |
| `<prefix>run_chain` | `Client.Migrate` (`004_run_states_chain`) | Per run with a `Job.Chain`: the rest of the chain as JSON, and the next workflow when the client that finished the run had not registered it. The same migration rewrites runs discarded by earlier releases (stored cancelled) to the `discarded` status |
| `<prefix>run_labels` | `Client.Migrate` (`005_run_labels`) | The label index: one row per label of a run (key and value cut to 191 characters), indexed on key, value and run, read by `RunQuery.Labels` and `LabelPrefixes`; filled at enqueue and, for older runs, by `Client.IndexLabels`, which keeps its watermark in the `<prefix>locks` row `labels:indexed` |
| `<prefix>run_version` | `Client.Migrate` (`010_run_version`) | Per versioned run: its `Workflow.Version` (no row for version 0), read by admission and claims so a worker takes only the versions it runs |
| `<prefix>fair_keys` | `Client.Migrate` (`011_fair_turns`) | Per round-robin key: where its last admitted run ends in its workflow's turns (rows behind the workflow's current turn are deleted) |
| `<prefix>run_fair` | `Client.Migrate` (`011_fair_turns`) | Per admitted round-robin run: its turn (start tag); pruned with the run |
| `<prefix>schedule_last` | `Client.Migrate` (`006_schedule_history`) | Per schedule: its overlap policy, last fire time and last run, backfilled for existing schedules |
| `<prefix>attempt_errors` | `Client.Migrate` (`006_schedule_history`) | Per-attempt error history (`RunInfo.Errors`), bounded per run by `WithErrorHistory`, deleted with the run |
| `<prefix>run_forced` | `Client.Migrate` (`006_schedule_history`) | Runs ended by `ForceFail`, so their error matches `workflow.ErrForced` by a flag, not by text; cleared by `Retry` and `ForceComplete` |
| `<prefix>outputs` | `Client.Migrate`, or `OutputStore.Migrate` alone | Captured step output of `sqlstore.OutputStore`, bounded per run |
| `<prefix>data` | `Client.Migrate`, or `DataStore.Migrate` alone | Scoped values of `sqlstore.DataStore` (folder, workflow and global data): one row per scope and name, with its version |
| `<prefix>inbox` | `Client.Migrate`, or `Inbox.Migrate` alone | The consumer inbox of `sqlstore.Inbox`: one row per consumer and message ID, held or done, until its lease or keep ends |
| `<prefix>aggregate_items`, `<prefix>aggregate_groups` | `AggregateStore.Migrate` (not `Client.Migrate`) | The items and groups of `sqlstore.AggregateStore` (`middleware.Aggregate`) |
| `<prefix>disabled_endpoints` | `DisableStore.Migrate` (not `Client.Migrate`) | The switches of `sqlstore.DisableStore` (`middleware.AutoDisable`): one row per key with its consecutive failures and whether it is disabled |
| `<prefix>lineage` | `LineageStore.Migrate` (not `Client.Migrate`) | The append-only lineage records of `sqlstore.LineageStore` (`events.Datasets`): one row per produced or consumed dataset version |
| `<prefix>outbox` | `outbox.Outbox.Migrate` | Outbox entries |
| `<prefix>migrations` | Any of the `Migrate` calls above | The migration ledger, shared by all of them for one prefix |
| `<prefix>migrate_lock` | Any `Migrate` on SQLite | The migration lock (Postgres and MySQL use an advisory or named lock instead) |

Each list of migrations is recorded in the ledger under its own owner, so a database that only needs one store gets only its table:

| Owner | Constant | Migrations |
|---|---|---|
| `ubgo/workflow/sqlstore` | `sqlstore.MigrationOwner` | The queue tables |
| `ubgo/workflow/sqlstore/unique` | `sqlstore.UniqueMigrationOwner` | `<prefix>unique_locks` |
| `ubgo/workflow/sqlstore/debounce` | `sqlstore.DebounceMigrationOwner` | `<prefix>debounce` |
| `ubgo/workflow/sqlstore/output` | `sqlstore.OutputMigrationOwner` | `<prefix>outputs` |
| `ubgo/workflow/sqlstore/data` | `sqlstore.DataMigrationOwner` | `<prefix>data` |
| `ubgo/workflow/sqlstore/inbox` | `sqlstore.InboxMigrationOwner` | `<prefix>inbox` |
| `ubgo/workflow/sqlstore/aggregate` | `sqlstore.AggregateMigrationOwner` | `<prefix>aggregate_items`, `<prefix>aggregate_groups` |
| `ubgo/workflow/sqlstore/disable` | `sqlstore.DisableMigrationOwner` | `<prefix>disabled_endpoints` |
| `ubgo/workflow/sqlstore/lineage` | `sqlstore.LineageMigrationOwner` | `<prefix>lineage` |
| `ubgo/workflow/sqlstore/outbox` | `outbox.MigrationOwner` | `<prefix>outbox` |

Under `WithSchema` the recorded owner is `<owner>@<schema>`, because the ledger table itself is never schema-qualified.

## Generated IDs and engine names

| Name | Format | Where |
|---|---|---|
| sqlstore run, delayed start, step and schedule IDs | `run_`, `sched_`, `step_` or `sch_` and 32 hex characters (`sqlstore.DefaultIDGenerator`; replace with `WithIDGenerator`) | `RunRef.RunID`, `RunRef.ScheduledID`, `StepInfo`, `ScheduleID` |
| Outbox entry ID | `obx_` and 32 hex characters (`outbox.DefaultIDGenerator`; replace with `outbox.WithIDGenerator`) | `Outbox.Write`, the default idempotency key the relay sends |
| Failover run references | `primary:` or `secondary:` in front of the driver's own ID | Every `RunRef`, `RunEvent` and `StepInfo` of a `failover.Client` |
| Fake run references | `fake-run-<n>`, `fake-delayed-<n>`, `fake-schedule-<n>` | `workflowtest.Fake` |
| Hatchet worker name | `ubgo-workflow` (`hatchet.DefaultWorkerName`) unless `Config.WorkerName`; with `Config.Queues`, one worker per queue named `<WorkerName>-<queue>` | Hatchet's dashboard |
| Postgres wake-up channel | `ubgo_workflow_<prefix>wake`, or `ubgo_workflow_<schema>_<prefix>wake` with `WithSchema`; a name over 63 bytes is hashed to `ubgo_workflow_<32 hex>` | `sqlstore.WithNotifier` (`pgnotify`), shared by the clients of one set of tables |
| Tick skip reason for overlap | `overlap` (`workflow.TickReasonOverlap`) | `Tick.Reason` of a tick `OverlapSkip` held back |
| Sentry tags and context | `workflow.name`, `workflow.step`, `workflow.status`, `workflow.final` (`sentry.TagWorkflow`, `TagStep`, `TagStatus`, `TagFinal`); opted-in labels as `workflow.label.<key>` (`TagLabelPrefix`); the context `workflow` (`ContextName`) with run ID, scheduled ID, attempt and time | Events of `sentry.Reporter` |
| Default queue | `default` (`workflow.DefaultQueue`) | The queue of a workflow and job that name none |
| Hatchet batch coordinator | The engine workflow `ubgo-workflow-batch-coordinator` | Registered by every Hatchet client with the coordinator on |
| Function runs | The workflow is the `Func`'s own name, with one step `func` (`workflow.FuncStep`) | `RunInfo`, `RunEvent`, `StepInfo` of a run of a `Func` |
| Closure runs | The workflow `ubgo.workflow.closure` (`workflow.ClosureWorkflow`, refused by `Validate` as a registered name) with one step `closure` (`workflow.ClosureStep`); the closure's name in the label `ubgo.workflow/closure` (`workflow.LabelClosure`) | `RunInfo`, `RunEvent`, `StepInfo` of a run started by `EnqueueClosure` |
| Idempotency keys the library derives | `spawn/<run ID>/<journal key>/<index>` (`Spawn`), `event/<subscription>/<event ID>` (`events.Bus`), `webhook/<workflow>/<delivery ID>` (`events.NewWebhook`), `period/<scope>/<period key>` (`OncePerPeriod`), `hybrid/<schedule ID>/<tick, RFC 3339 with nanoseconds>` (`hybrid`) | `Job.IdempotencyKey` of the runs they start |
| Period keys | `2026-10-14` (day), `2026-W42` (ISO week), `2026-10` (month), `2026-Q4` (quarter), `2026` (year) | `PeriodKey` |
| Steps of a `Choice` | `<name>.decide` (`workflow.ChoiceDecideSuffix`), `<name>.<branch>` per branch, and the join step `<name>` | The steps `Choice` returns |
| Journal keys | `<length of step>:<step>/<name>`, with `#<index>` for `MapEach`, `Loop` and `Spawn` items; `data/<name>` for run data | `JournalStore` entries |
| Webhook headers | `webhook-id`, `webhook-timestamp`, `webhook-signature` (`events.HeaderWebhookID`, `HeaderWebhookTimestamp`, `HeaderWebhookSignature`); secrets prefixed `whsec_` | `events.NewStandardWebhooks` |
| Event names the library signals | `ubgo.workflow.promise` (`workflow.PromiseEvent`), `ubgo.workflow.approval` (`workflow.ApprovalEvent`) | `ResolvePromise`, `RejectPromise`, `Approve`, `Reject` |
| Journal run IDs of library stores | `ubgo.workflow/promises` (the `PromiseStore` on a `JournalStore`), `ubgo.workflow/object/<hash>` (one per virtual object key); the run data key `ubgo.workflow/suspended` (`Suspension`) | `sqlstore.PromiseStore`, `DefineObject`, `SuspendRun` |
| Node IDs and modes | `<host name>:<process ID>` (`workflow.DefaultNodeID`) unless `WithNodeID`; modes `full`, `worker`, `scheduler`, `enqueue-only` (`workflow.NodeFull`, `NodeWorker`, `NodeScheduler`, `NodeEnqueueOnly`) | `StepInfo.Node`, `RunInfo.AttemptedBy`, `NodeInfo` |
| Lifecycle event kinds | `inserted`, `started`, `retrying`, `snoozed`, `progress`, `finished` (`workflow.LifecycleInserted` and the rest) | `LifecycleEvent.Kind` |
| Unique struct tag | `unique` (`middleware.UniqueTag`) | Payload fields `middleware.ArgsTagged` reads |
| Schedule JSON | `{"v": 1, ...}` (`workflow.ScheduleJSONVersion`), the zone by IANA name | `json.Marshal` of a `Schedule` |
| Message attributes | `ubgo-workflow`, `ubgo-tenant`, `ubgo-queue`, `ubgo-idempotency-key`, `ubgo-priority`, `ubgo-run-at`, `ubgo-l-<label key>` or `ubgo-labels` (JSON) under `workflow.DefaultAttributePrefix` (`Attributes.Prefix`); trace labels as `traceparent`, `tracestate`, `baggage` (`workflow.DefaultTraceAttributes`, `Attributes.Trace`) | `workflow.Attributes` encoding a job for a broker or HTTP hop |
| Admin command line environment | `WORKFLOW_ADMIN_URL`, `WORKFLOW_ADMIN_TOKEN`, `WORKFLOW_ADMIN_HEADER` | The `workflow` command (`cli`) |

## Metrics

The `otel` package records under the instrumentation scope `github.com/ubgo/workflow/otel` (`otel.ScopeName`).

| Metric | Constant | Type and unit | What it counts |
|---|---|---|---|
| `workflow.step.duration` | `otel.MetricStepDuration` | Histogram, `s` | The duration of each step attempt |
| `workflow.step.outcomes` | `otel.MetricStepOutcomes` | Counter, `{attempt}` | Step attempts by outcome |
| `workflow.enqueue.count` | `otel.MetricEnqueueCount` | Counter, `{job}` | Enqueues by outcome |
| `workflow.queue.wait` | `otel.MetricQueueWait` | Histogram, `s` | How long each step attempt waited for a worker, from `StepInfo.ReadyAt` to its start (buckets `otel.QueueWaitBuckets()`, 5 ms to 1 h); no sample when the driver sets no `ReadyAt` |
| `workflow.runs.finished` | `otel.MetricRunsFinished` | Counter, `{run}` | Run ends by `workflow.name` and `workflow.run.status`, recorded by the observer `Instrumentation.RunFinished()` returns; delivery is at least once, so a redelivered end counts again |
| `workflow.queue.depth` | `otel.MetricQueueDepth` | Observable gauge, `{run}` | Runs waiting per queue, read at each collection through `Instrumentation.ObserveQueueDepth(fn)` (for example `admin.Admin.QueueDepths`) |

Spans are named `workflow.step <workflow>/<step>` per step attempt and `workflow.enqueue <workflow>` per enqueue, unless renamed with `WithStepSpanName` and `WithEnqueueSpanName`. The step metrics carry `workflow.name`, `workflow.step`, `workflow.queue` and `workflow.outcome`, the enqueue counter `workflow.name` and `workflow.outcome`, plus `workflow.queue` when `Job.Queue` is set; run IDs never become metric attributes. The attributes:

| Attribute | Constant | Value |
|---|---|---|
| `workflow.name` | `otel.AttrWorkflow` | The workflow name |
| `workflow.step` | `otel.AttrStep` | The step name |
| `workflow.run_id` | `otel.AttrRunID` | The run ID |
| `workflow.attempt` | `otel.AttrAttempt` | The attempt number |
| `workflow.queue` | `otel.AttrQueue` | The queue a step ran on (`StepInfo.Queue`); on the enqueue span and counter only when `Job.Queue` is set, since the workflow's own queue is resolved later by the driver |
| `workflow.outcome` | `otel.AttrOutcome` | `ok`, `error`, `permanent`, `cancelled` or `discarded` (`otel.OutcomeOK`, `OutcomeError`, `OutcomePermanent`, `OutcomeCancelled`, `OutcomeDiscarded`) |
| `workflow.run.status` | `otel.AttrRunStatus` | On `workflow.runs.finished`: `succeeded`, `failed`, `cancelled`, or `discarded` on a driver with `CapRunStates` |
| `workflow.tenant` | `otel.AttrTenant` | `Job.Tenant` (`StepInfo.Tenant`), on step and enqueue spans only with `WithTenant` |
| `workflow.label.<key>` | `otel.LabelAttributePrefix` | A job label, only on opt-in: on spans with `WithLabelFilter` or `WithAllLabels`, on metrics with `WithMetricLabels`, one key at a time |

## Related

- [Labels](../concepts/labels.md): what labels are for and how they travel.
- [Migrations](../operations/migrations.md): the ledger, `MigrationScripts` and running `Migrate` in a deploy.
- [OpenTelemetry](../observability/opentelemetry.md): wiring the spans and metrics.
- [Feature index](features.md).
