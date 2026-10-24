// Package sqlstore is the durable SQL driver of workflow.Client: every run,
// step result, wait, sleep, schedule, signal and lease is a row, so a
// restart or a dead worker loses nothing. It runs on SQLite (durable on one
// machine), Postgres and MySQL (many workers on many machines over one
// database), through database/sql and github.com/ubgo/sqlb.
//
//	db, _ := driversqlite.Open("app.db")      // or driverpgx.Open(dsn)
//	c, _ := sqlstore.New(db, sqlb.SQLiteDialect{})
//	_ = c.Migrate(ctx)                         // explicit, versioned; Start never migrates
//	_ = c.Register(wf)
//	_ = c.Start(ctx)                           // nothing runs before this
//
// # Capabilities
//
// Every capability, each proven by the workflowtest conformance suite on
// SQLite, Postgres and MySQL 8: CapDurable, CapDelayedStart, CapWaitForEvent,
// CapDurableSleep, CapConcurrencyKey, CapRateLimit, CapIdempotencyKey,
// CapCron, CapCancel, CapRunFinished, CapRetryJitter and CapRetryDelay
// (retry delays are computed in Go by workflow.RetryPolicy.NextDelay;
// WithJitterSource pins the draws in tests), CapCronZone (declared crons
// run in Workflow.CronLocation, schedules in Schedule.Location) and
// CapMisfireSkip, CapMisfireFireOnce and CapMisfireFireAll, and the queue,
// retry, admin, pause, schedule and drain capabilities (CapQueue,
// CapQueueOverride, CapJobRetry, CapRetryUntil, CapRetryMaxErrors,
// CapDiscard, CapDiscardExhausted, CapQueueLimitGlobal, CapDrainReport,
// CapRunQuery, CapRetryRun, CapDeleteRun, CapCancelAll,
// CapPauseWorkflow, CapPauseQueue, CapSchedulePause, CapScheduleWhen,
// CapScheduleWindow, CapTickEvents, CapScheduleList, CapDrain), and
// CapRunStates, CapChain and CapWorkerScale, and CapOverlapSkip,
// CapScheduleTrigger, CapScheduleHistory, CapEnqueueOnly, CapErrorHistory,
// CapShutdownRequeue and CapForceRun, and CapTenant, CapResult,
// CapDueQuery, CapNodeIdentity, CapSnooze, CapHeartbeat, CapRunTimeout,
// CapKeyFIFO, CapCronSeconds, CapScheduleActive, CapScheduleKeep and
// CapTickTime (see Run facts and schedules below). CapClosure is not declared: a
// Go function cannot be stored, so it could not survive the restart this
// driver exists for, and workflow.EnqueueClosure refuses the client with
// an UnsupportedError; register a workflow instead. Keys are Go functions
// (workflow.KeyFunc); an expression key (KeyExpr) is refused at Start with
// an UnsupportedError, because SQL has no engine expression language.
//
// # How work moves
//
//   - Enqueue writes a run row. A run with RunAt in the future is
//     "scheduled" until the dispatcher sees its time come.
//   - Admission moves a pending run to active and writes a row per step. A
//     workflow with a concurrency or rate policy admits under its lock row
//     (a row-level lock on Postgres and MySQL, the single writer on
//     SQLite), so the bucket counts a decision reads hold until it commits.
//   - Workers claim ready steps: FOR UPDATE SKIP LOCKED on Postgres and
//     MySQL, a single conditional UPDATE on SQLite. A claim is a lease
//     renewed every third of WithLease; a step whose lease expires (its
//     worker died) is claimed again by any client, so a step runs at least
//     once. A client that stops hands its running steps back at once. An
//     attempt taken over more than WithMaxReclaims times (default 5) is not
//     run again: it fails with an error wrapping ErrAbandoned that names
//     how many workers abandoned it, and the retry policy then applies as
//     for any failure (a retry starts with a fresh count), so a step that
//     kills every worker it lands on cannot loop through the cluster.
//   - Every transaction that changes a run's steps first updates the run
//     row, which serialises them per run on every engine.
//   - Retries follow workflow.EffectiveRetries; a failed attempt's step
//     becomes ready again after the backoff. A step that fails for good
//     marks the run failing, OnFailure runs once with the failing step's
//     input (under a lease, so a crash reruns it on another client: at
//     least once), then the run is failed.
//   - Sleeps and wait deadlines are due times in the steps table. Signals
//     are rows; a run takes only signals stored after its enqueue (its
//     signal floor), and the consumed table's primary key means one run
//     never takes one signal twice. Signals no unfinished run can take are
//     pruned.
//   - Cron schedules (declared and Client.Schedule) are rows with their
//     next fire time, parsed with cron.Parse or WithCronParser (every
//     client of one database must use the same parser). A tick is fired
//     by moving the schedule's next fire time on from the value read and
//     inserting the (schedule, fire time) row, in one transaction: the
//     client that wins enqueues the run, any other fails and enqueues
//     nothing, so a tick runs once across any number of clients. Under
//     the default misfire policy a tick missed while no client ran fires
//     once on return and older missed ticks are not replayed; a
//     workflow.Misfire on the schedule or workflow (stored in
//     <prefix>schedule_misfire) fires what Misfire.Due picks instead:
//     nothing late (MisfireSkip), the newest tick within Within
//     (MisfireFireOnce), or up to Max missed ticks, oldest first, each
//     with its own tick row (MisfireFireAll).
//   - Client.Schedule with a Name is an upsert by (Workflow, Name): the
//     same name gives the same ScheduleID and row on every node and boot,
//     so an app can declare its schedules at start-up; an unchanged
//     declaration keeps its next fire time. A schedule without a Name is
//     new on every call.
//   - Queues: every run's queue (workflow.EffectiveQueue) is stored in
//     <prefix>run_ext with its Job.Retry override and idempotency key. A
//     client built WithQueues claims only its queues, at most each
//     queue's Workers at once, higher Priority tiers first; without it, it
//     claims every queue. Runs of a queue no started client serves wait.
//     QueueConfig.Limit caps a queue's in-flight step attempts across every
//     client sharing the database (CapQueueLimitGlobal): the claim takes a
//     per-queue lock row, then counts the queue's running steps with a live
//     lease, so two nodes cannot both take the last slot and a dead node's
//     slots free when its leases expire. Workers stays per client.
//   - Pauses (Pauser, SchedulePauser) are rows in <prefix>pauses and
//     <prefix>schedule_ext. The claim statement itself excludes held runs,
//     so a pause is obeyed by every client from its commit on and survives
//     restarts. A paused schedule's due ticks are reported TickSkipped
//     (reason paused) once across the cluster and never fire, not even
//     on resume: its next fire time moves past them.
//   - A failed attempt is judged by workflow.RetryPolicy.Next with the
//     run's Job.Retry (else the step's or workflow's policy) and the step's
//     stored counts: steps.failures is Attempts.N, <prefix>step_ext holds
//     Errors and First, so Until and MaxErrors hold across restarts and
//     nodes. A retry waits as a ready step whose due time is the end of
//     its backoff; while one does (and no attempt of the run executes) the
//     run reads StatusRetrying on every client, and RunInfo.NextAttempt,
//     NextAttemptAt and Error give that step's next attempt and its last
//     attempt's error (kept in <prefix>step_err). A step returning
//     workflow.Discard ends its run with the stored status discarded
//     (StatusDiscarded) and an Err matching ErrDiscarded and ErrCancelled;
//     OnFailure does not run and Retry refuses it. A run out of retries,
//     MaxErrors or Until ends failed, unless the policy sets
//     RetryPolicy.DiscardOnExhaust (CapDiscardExhausted), which ends it
//     discarded, with no OnFailure. Retention keeps a
//     discarded run as long as a cancelled one (Retention.Cancelled).
//   - Chains: Job.Chain, and the edits a step makes with PrependToChain
//     and AppendToChain, are a row of <prefix>run_chain. An attempt's
//     edits are written in the transaction that records its success, so a
//     failed attempt's edits are dropped and a crash loses none. The run's
//     success inserts the next job as a new run carrying the rest of the
//     chain, in the same transaction; when that job's workflow is not
//     registered on the finishing client, the row waits until a client
//     that registers it hands it on. A failed, discarded or cancelled run
//     stops its chain; Retry of a failed run resumes it on success. Enqueue
//     middleware does not run for the jobs a chain hands on.
//   - Worker scaling (workflow.WorkerScaler): QueueLoads reads the depth
//     and oldest wait of the ready steps of each queue given to
//     WithQueues, and SetWorkers changes that queue's cap on this client
//     while it runs (not stored; a restart starts from WithQueues). Drive
//     it with workflow.BalanceLoop; nothing balances unless the app runs
//     one.
//   - Runs lists runs of every workflow in the database; Retry puts a
//     failed run back to pending with its succeeded steps kept, and
//     admission resumes it from the steps not yet done; Delete and
//     retention remove a run with its steps and side rows.
//   - Schedule windows and ScheduleWhen are decided on the firing node
//     before the firing transaction; a skipped tick still gets its tick
//     row, so one node reports it. OnTick observers hear every fired,
//     skipped and misfired tick after its commit (best effort on a crash).
//     Every run a schedule fires carries workflow.LabelSchedule.
//   - EnqueueMany writes several runs in one transaction (all or none);
//     BatchStore implements batch.Pruner.
//   - Drain stops claiming, waits for every attempt from claim to written
//     outcome, then stops; WithStopWhenIdle drains once no unfinished run
//     of the client's workflows (on its queues) existed for the grace.
//     DrainWithReport (workflow.ReportingDrainer, CapDrainReport) also
//     returns the attempts still running at the deadline, each with the
//     disposition workflow.DrainRequeued: it was handed back as ready
//     without spending an attempt.
//   - WithRecycle (workflow.Recycle: MaxJobs, MaxTime, MaxMemory) drains
//     the client the same way once a limit is reached, so a supervisor
//     restarts the process; StopCause returns the *workflow.RecycleError
//     and workflow.Serve returns it. Runs it never claimed stay queued.
//   - A label filter (RunQuery.Labels and, with CapLabelPrefix,
//     RunQuery.LabelPrefixes, in Runs and CancelAll) reads the run_labels
//     index: one row per label of a run, key and value cut to 191
//     characters, written with the run and removed with it. A prefix is a
//     LIKE pattern with its wildcards escaped. The exact labels are then
//     checked in Go, so a long value never matches wrongly.
//   - Run ends are delivered to OnRunFinished observers from the table,
//     claimed under a lease and marked after delivery: at least once,
//     including ends that happened while no client was running.
//
// # Time
//
// Due times (delays, sleeps, deadlines, backoff, timeouts, rate windows,
// cron) come from WithClock. Leases and the poll interval are wall time
// (WithLeaseClock), because they measure whether a worker is alive: a
// fake scheduling clock moved by an hour must not make every running step
// look abandoned. Times are stored as BIGINT Unix nanoseconds.
//
// Nodes whose clocks disagree compare due times and lease ends against
// different "now"s. WithDatabaseClock makes the database server's clock
// the one source for both (DatabaseClock: read at Start and every resync,
// advanced in between on the local monotonic clock, so a wall-clock jump on
// a node moves nothing). A lease holder fences itself: when a renewal has
// not landed a whole lease after the last one began, measured on its own
// lease clock, the attempt is cancelled with ErrLeaseLost and its outcome
// is not written, because another node may already hold the step.
// WithLeaseSkew makes the other nodes wait that much longer past a lease's
// end before taking it over (steps, OnFailure, run-end delivery, the prune
// lock), so clocks that disagree by less than the margin never let two
// nodes hold one lease.
//
// # Clusters
//
// Clients over one database and table prefix are workers of one queue. A
// client only claims, admits, fires and delivers for the workflows it has
// registered, so services with different workflows can share the tables.
// Cancel reaches a step running on another client at that client's next
// lease renewal.
//
// A client finds work another client made due at its next poll
// (WithPollInterval, a second by default). WithNotifier adds wake-ups on top:
// with pgnotify.New(db) on Postgres, an enqueue, a finished step, a retry or
// a resume on one client sends a NOTIFY and every idle client listening on
// the tables' channel starts at once. Measured on the host Postgres with a
// one-second poll, 15 samples each, enqueue to step start on another client:
// polling p50 591ms, p95 897ms; with pgnotify p50 38ms, p95 99ms (shared
// development machine under load). Polling stays as the fallback, so a lost
// notification only delays work.
//
// Every client fires cron ticks, and the unique tick row lets one fire per
// tick win: no election, and it survives a split brain. Where the fire must
// happen on one known node, WithLeaderElection (a LeaderElector over the
// tables' leaders row: a renewed lease, self-fencing, a Term that grows on
// each handover) or WithElector (any Elector: a Redis lock, a Kubernetes
// Lease) limits cron firing and housekeeping to the leader; the tick row
// stays as the second guard. Steps run on every client either way.
//
// WithFetchBatch sets how many rows one dispatcher query reads (default
// 100). Workers claim one step per query, each into a free worker, because
// a claimed step is leased and a lease held for a busy worker would keep
// the step from every other node.
//
// # Schedule overlap, trigger and history
//
// Each schedule has a schedule_last row with its overlap policy
// (Schedule.Overlap, Workflow.CronOverlap) and its last fire: the time and
// the run of the last tick or TriggerSchedule. Under OverlapSkip a tick is
// skipped (TickSkipped, TickReasonOverlap) while that run is unfinished, and
// TriggerSchedule refuses with an error matching workflow.ErrOverlap. The
// fire moves the last run with a compare-and-set on the run its decision
// read, so two fires deciding at once on two nodes start one run. A last
// run that was deleted or pruned does not block. Schedules reports Overlap
// and Last, with the last run's status read at list time.
//
// # Enqueue-only clients
//
// WithEnqueueOnly builds a client that only writes work (web nodes), while
// worker clients over the same tables run it; see WithEnqueueOnly for what
// a job of a workflow it did not register still gets.
//
// # Run facts and schedules
//
// Migration 008 adds run_info, one row per run: Job.Tenant (StepInfo,
// RunInfo, RunQuery.Tenant), the result stored when the run succeeds
// (workflow.RunOutput of its final steps; Result, RunInfo.Output), the
// latest workflow.ReportProgress (RunInfo.Progress) and the nodes that
// claimed its attempts (RunInfo.AttemptedBy; the node of each recorded
// attempt error is in attempt_err_node). Reschedule moves a scheduled run;
// RunQuery.DueFrom and DueTo select delayed runs by RunInfo.RunAt. A
// workflow.Snooze puts its step back as the same attempt (status snoozed,
// run Waiting); a step's HeartbeatTimeout fails an attempt that stops
// calling workflow.Heartbeat (a beat also renews the lease, at most every
// third of it); Workflow.RunTimeout fails the run from its admission, and
// OnTimeout replaces OnFailure for every timeout (the kind is stored, so
// the run's error matches its sentinel after a restart). CancelStep stops a
// running attempt with workflow.ErrStepCancelled, on another client at its
// next lease renewal. Concurrency.FIFO keeps runs of one key in enqueue
// order past a rate limit too. Schedule.Seconds, Active and KeepRuns (and
// the Workflow.Cron equivalents) are stored in schedule_spec; KeepRuns is
// applied at each fire and at housekeeping; every fired run carries
// workflow.LabelTickAt.
//
// Every client reports itself in the nodes table every third of its lease
// (Nodes; WithNodeID) and leaves it on a clean stop; WithSchedulerOnly
// makes a client that fires schedules and runs no step, WithoutScheduler
// one that runs steps and fires nothing. Health answers liveness and
// readiness (started, database ping). OnLifecycle reports Inserted,
// Started, Retrying, Snoozed, Progress and Finished for the runs and
// attempts this client handles. QueueConfig.Weight draws among the queues
// of one priority by weight on the jitter source. WithPayloadLimit refuses
// big payloads. UniqueStore also records each key's holder run
// (middleware.UniqueHolders, migration 002_unique_holder of its list).
//
// # Operator actions and error history
//
// ForceComplete and ForceFail end a run from outside (workflow.RunForcer);
// Retry re-runs a force-failed one. Every failed attempt is kept in
// attempt_errors (RunInfo.Errors), the newest workflow.DefaultErrorHistory
// per run unless WithErrorHistory says otherwise. A step cut short by a
// clean stop (Start ctx cancelled, Stop) is handed back with its attempt
// refunded: the next worker runs it as the same attempt, and it is neither
// counted against the retry policy nor recorded as an error. A crash is not
// a clean stop: the step's lease expires and the takeover counts.
//
// # Captured output
//
// OutputStore (Client.OutputStore, or NewOutputStore with its own Migrate
// for an app without the queue) is a middleware.OutputSink that keeps each
// attempt's captured output in <prefix>outputs, bounded per run and per
// attempt (OutputLimits), and RunOutput reads a run's attempts back for an
// admin screen. A Client deletes a run's output with the run;
// OutputStore.PruneBefore bounds the table where no Client runs.
//
// # Batches
//
// Client.BatchStore is a durable batch.Store in the same database: one row
// per batch, Update atomic per batch on every engine.
//
// # Retention
//
// Finished runs are kept forever unless WithRetention says otherwise:
// deleting history is the app's decision. Retention sets a keep-for per
// terminal status (zero keeps forever) and a KeepLast count per workflow
// (only the newest N finished runs stay; either rule deletes), with
// per-workflow overrides. Prune deletes what the rules select in chunks
// of 100 (WithPruneChunk), each in a short transaction
// and each passed first to the WithArchive hook, whose error keeps the
// chunk; a run's steps and consumed-signal markers go with it. Prune never
// deletes a run that holds an idempotency key, whose end has not been
// delivered to an OnRunFinished observer (unless Retention.Undelivered,
// for clusters with no observer; without it such a cluster deletes
// nothing, which Prune reports in PruneResult.HeldUndelivered and the
// background prune logs as a warning), or that belongs to an open batch
// in Client.BatchStore. A batch kept in another store cannot be seen
// here, and its status poller reads each job's Status, so with retention
// on keep batches in Client.BatchStore or a job's run may be deleted
// before its batch finishes. A started client prunes every WithPruneInterval (default an
// hour) when it wins the prune lock's lease, so one client of a cluster
// prunes per interval; WithoutAutoPrune leaves Prune to the app's own
// scheduler. Old cron tick rows (older than WithTickRetention, default 24
// hours), unreachable signals and expired uniqueness locks are pruned by
// the dispatcher every WithHousekeepingEvery passes (default 50) whatever
// the retention.
//
// The queue's tables are hot: claims and admissions rewrite their rows, so
// on Postgres every finished run kept there adds index entries and every
// update a dead row for VACUUM. Retention keeps them small. To keep the
// history anyway, WithArchiveTable makes Prune move each run, with its
// steps' outputs, into the cold tables <prefix>runs_archive and
// <prefix>steps_archive in the transaction that deletes it, append-only and
// never read by the queue; Client.ArchivedRuns reads them back.
//
// # Transactional enqueue
//
// Client.EnqueueTx writes a run inside the app's own *sql.Tx, so it commits
// or rolls back with the app's writes (the transactional outbox, with no
// relay because the queue is this database). The run is written pending and
// admitted after the commit by the dispatcher, within WithPollInterval, or
// at once on a client that is woken with Client.Wake after the commit.
// Idempotency keys hold as for Enqueue; the insert runs under a savepoint, so
// a duplicate key never aborts the caller's transaction. Enqueue middleware
// does not run, because its effects would not roll back with the
// transaction. To enqueue transactionally on another backend (Hatchet,
// memory) or through middleware, write to the sqlstore/outbox package and
// let its relay enqueue after the commit, at least once.
//
// An app whose transactions are pgx transactions passes them through the
// sqlstore/pgxtx package: pgxtx.Run hands EnqueueTx (or outbox.Write) a
// *sql.Tx whose statements run on the app's pgx.Tx.
//
// # Uniqueness locks
//
// UniqueStore is a durable middleware.UniqueStore (ADR-0005): locks are
// rows of <prefix>unique_locks, so middleware.Unique holds across every
// process sharing the database, with MemoryUnique's semantics exactly. Take
// it from a Client (Client.UniqueStore, created by Client.Migrate) or build
// it with NewUniqueStore over any *sql.DB, which needs no running Client:
// an app on Hatchet with a database gets durable uniqueness that way. Its
// table has its own migration list (owner ubgo/workflow/sqlstore/unique) in
// the same ledger, so UniqueStore.Migrate creates only that table. Expired
// locks are deleted by the dispatcher's prune, or by PruneExpired.
//
// # Debounce state
//
// DebounceStore is a durable middleware.DebounceStore: each debounce key's
// pending burst is a row of <prefix>debounce, so middleware.Debounce with
// WithDebounceStore debounces a key enqueued from every process sharing
// the database. Swap is atomic across processes (it locks the key's row
// before reading the burst it replaces; a first-insert race is retried), so
// every replaced run is cancelled once. Take it from a Client
// (Client.DebounceStore, created by Client.Migrate) or build it with
// NewDebounceStore over any *sql.DB; its table has its own migration list
// (owner ubgo/workflow/sqlstore/debounce, migration 001_debounce) in the
// shared <prefix>migrations ledger. Bursts whose run has started are
// deleted by the dispatcher's housekeeping, or by PruneExpired.
//
// # Journal and bucket state
//
// JournalStore is a durable workflow.JournalStore (the memo of workflow.Memo
// and the helpers built on it): a run's memoised results are rows of
// <prefix>journal, so a step retried after a crash, on any node, gets them
// back. BucketStore is a durable workflow.BucketStore, the leaky bucket
// behind workflow.UpstreamLimit: one row per bucket key in <prefix>buckets,
// and Take is atomic across processes (it locks the key's row before
// reading the level; a first-insert race is retried), so every worker
// calling one upstream draws from one bucket; time is the now the caller
// passes. Take each from a Client (Client.JournalStore, Client.BucketStore,
// created by Client.Migrate) or build it with NewJournalStore or
// NewBucketStore over any *sql.DB; each has its own migration list (owners
// ubgo/workflow/sqlstore/journal and ubgo/workflow/sqlstore/bucket, both
// 001) in the shared ledger. Migration 009 adds schedule_tenant:
// Schedule.Tenant, the Job.Tenant of every run the schedule fires or is
// triggered to run, reported in ScheduleInfo.Tenant.
//
// # Database policies
//
// Three things about the database are pluggable, each with today's
// behaviour as the default:
//
//   - WithErrorClassifier: how a driver error is sorted into a lost unique
//     race, a retryable lock conflict or an outage (DefaultErrorClassifier
//     knows the drivers sqlb's contrib modules open). A transaction that
//     loses a lock race is tried WithTxAttempts times (default 5).
//   - Errors the classifier calls unavailable (a refused or dropped
//     connection, a server shutting down) are marked with
//     workflow.Unavailable, so errors.Is(err, workflow.ErrUnavailable)
//     holds and a failover client can tell an outage from a bad request.
//   - WithIDGenerator: how RunIDs, ScheduledIDs, step IDs and unnamed
//     ScheduleIDs are made (default "<kind>_<32 hex>"); an ID is 1 to 64
//     bytes, and a generator that errors fails the operation.
//
// # Placement
//
// WithTablePrefix starts every table name ("wf_" by default). On Postgres,
// WithSchema puts the tables in a schema of their own; the migration ledger
// then stays in the search_path with its rows owned as "<owner>@<schema>".
// SQLite and MySQL refuse WithSchema.
//
// # Schema versions
//
// The schema is a list of numbered migrations (migrations.go; 001_initial
// is the baseline; UniqueStore's table is a second list under its own
// owner) applied by Migrate through sqlb's contrib/migrate ledger,
// in a table named <prefix>migrations, under the engine's migration lock, so
// concurrent Migrate calls are safe. A released migration is never edited
// (its checksum would refuse every database that applied it); a schema
// change is a new migration appended to the list (002_schedule_misfire adds
// the misfire table, 003_queues_admin the queue, retry, pause and
// schedule-window tables and backfills a run_ext row for every existing
// run, 004_run_states_chain the run_chain and step_err tables and rewrites runs earlier
// releases stored as cancelled with run_ext.discarded = 1 to discarded,
// 005_run_labels the run_labels label index, whose backfill of existing runs
// is Client.IndexLabels, run by Migrate after the scripts and safe to run
// twice and from several processes, 006_schedule_history the schedule_last
// table (each schedule's overlap policy and last fire, backfilled for every
// existing schedule) and the attempt_errors table (each run's error history),
// 007_leader_archive the leaders table (LeaderElector) and the cold
// runs_archive and steps_archive tables (WithArchiveTable),
// 008_run_info_nodes the run_info, attempt_err_node, step_cancel,
// schedule_spec and nodes tables (backfilling a run_info row for every
// existing run);
// Start refuses a database without them until Migrate runs). Migrate and Start refuse a database a newer release migrated, with
// an error matching ErrSchemaTooNew that names the unknown migrations.
// There are no down migrations.
//
// MigrationScripts returns the same migrations as SQL text, per dialect and
// table options, for teams that ship DDL through goose, Atlas, Flyway,
// Liquibase or a DBA review: apply the statements there (each is safe to
// run twice). Start checks the tables, not the ledger, so a schema applied
// that way starts without Migrate; call Client.IndexLabels once after
// applying 005_run_labels, since its backfill decodes JSON in Go.
package sqlstore
