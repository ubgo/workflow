// Package hatchet is the Hatchet driver of github.com/ubgo/workflow: a
// workflow.Client whose runs, DAG steps, durable waits and sleeps,
// retries, timeouts, crons, concurrency keys and idempotency keys are
// Hatchet's own (github.com/hatchet-dev/hatchet sdks/go).
//
// It is a separate module so an app that never uses Hatchet never
// downloads the SDK. Build a client with New, Register workflows, then
// Start; nothing connects before Start. Native returns the SDK client as
// the one escape hatch.
//
// What Hatchet cannot express is refused with an error wrapping
// workflow.ErrUnsupported that names the field (and, where one applies,
// the missing capability), never dropped. The guide's Limitations table
// (docs/drivers/hatchet.md) lists every refusal and difference below with
// the engine fact behind it and what to use instead. Workflows are refused
// at Register, schedules at Schedule, jobs at Enqueue:
// Concurrency.Key.Func (keys are CEL, use workflow.KeyExpr),
// WaitSpec.KeyFrom.Expr (use workflow.KeyFunc), a Workflow.CronLocation or
// Schedule.Location other than UTC (Hatchet evaluates crons in UTC only;
// no CapCronZone), any Workflow.Misfire or Schedule.Misfire policy
// (Hatchet never catches up a missed tick and has no lateness window, so
// only the driver default holds: a tick missed while the engine was down
// is dropped; no CapMisfire*), cron macros and @every, a retry Backoff
// whose Initial is not Factor seconds (Hatchet waits Factor^n seconds), a
// Backoff.Jitter (no CapRetryJitter) and a RetryPolicy.DelayFor such as
// workflow.RetryAfterDelay (the engine never takes a retry's delay from
// the error; no CapRetryDelay), a negative Job.Priority, a label under
// the wf__ or hatchet__ prefix or named traceparent (Job.Labels) and a
// JSON object payload with a top-level _wf_payload key (Job.Payload),
// both the driver's own run data, a step named wf__start or a workflow
// name ending in __wf_delayed (ErrInvalid: the start gate's and the
// delayed variants' names), Job.IdempotencyKey together with Job.RunAt
// on a workflow with a Concurrency, past Config.IdempotencyMaxHold or on
// a client that did not register the workflow (see below), a
// RateLimit.Per other than
// a second, minute, hour, day or week, and RateLimits on a workflow with
// more than one first step (Hatchet limits tasks, not runs), a
// RetryPolicy.Until or MaxErrors (Hatchet fixes a task's retries when it
// is declared; no CapRetryUntil, CapRetryMaxErrors), a Job.Retry (no
// CapJobRetry, same reason), a Job.Queue other than the workflow's (a run
// goes to whichever worker registered its workflow; no CapQueueOverride),
// a QueueConfig.Priority or a non-zero QueueConfig.Limit (refused at
// Start: no CapQueueLimitGlobal, since the engine does not count a whole
// queue's in-flight steps across workers), RetryPolicy.DiscardOnExhaust
// (the engine has no discarded status; no CapDiscardExhausted),
// Workflow.ScheduleWhen and
// Workflow.CronWindows or Schedule.Windows (the engine fires crons itself;
// no CapScheduleWhen, CapScheduleWindow), OverlapSkip on
// Workflow.CronOverlap or Schedule.Overlap (the engine fires a tick
// without asking a worker and has no setting that skips one while the
// previous run is unfinished; no CapOverlapSkip), Workflow.CronActive and
// Schedule.Active (a Hatchet cron has no start or end; no
// CapScheduleActive), Workflow.CronKeepRuns and Schedule.KeepRuns (the
// engine keeps runs for its retention and has no run delete; no
// CapScheduleKeep), Workflow.RunTimeout and OnTimeout (the engine times
// out tasks, not runs, with its own error text; no CapRunTimeout; the
// worker enforces Step.Timeout instead, see below),
// Step.HeartbeatTimeout (see below; no CapHeartbeat) and a
// QueueConfig.Weight (refused at Start: each queue is its own worker, so
// no worker picks between queues). The core's
// cron parser is the only one: the driver checks expressions with
// cron.Parse and has no option for another dialect, since the engine
// parses them itself. Start refuses Config tunings past the SDK's limits
// (GroupChunkRuns over 1000, GroupChunkBytes over 2 MiB) and a
// BatchCoordinatorRetry Hatchet cannot express (as above, or with
// NoRetryOn).
//
// Step.Timeout (or Workflow.Timeout) is enforced by the worker, not left
// to the engine, so a step timeout behaves as on the other drivers: the
// attempt's context is cancelled with workflow.TimeoutError(ErrStepTimeout,
// ...), the attempt fails with it whatever the handler returns unless a
// step tolerated by workflow.WithRules claimed it (workflow.ClaimTimeout,
// recorded as FailureTimeout for RuleOneFailedOf), and the run-finished
// error of such a run matches ErrStepTimeout. The engine's execution
// timeout is the step's timeout rounded up to whole seconds plus five
// seconds, so it only ends a handler that ignores its context (with the
// engine's own error text) and is when a step on a dead worker is
// reclaimed. A step without a timeout runs under Config.StepTimeout on
// the engine only. The workflowtest harness says so with
// Harness.StepTimeoutHandshake, which runs the step-timeout conformance
// cases without CapRunTimeout.
//
// The engine holds an idempotency key until the first task created with
// the run ends (it releases a key when a task carrying it reaches a
// terminal state), not until the run ends, and at most
// Config.IdempotencyMaxHold: during the later steps of a multi-step run
// an Enqueue with the same key starts a second run (checked live).
//
// A delayed start with an idempotency key (Job.RunAt and
// Job.IdempotencyKey, which workflow.Spawn always sets) cannot be a
// Hatchet scheduled run, which takes no key. It is started at once
// instead, as a run of the workflow's delayed variant: a second engine
// workflow (the name plus __wf_delayed) that Start declares beside each
// workflow without IdempotencyOff or a Concurrency, with the same steps,
// idempotency expression, rate limits and on-failure step, no cron, and a
// start gate, a durable task (wf__start) before the first steps that
// sleeps on the engine until RunAt (about a second late at most). The
// gate is the variant's first task, so the engine holds the key from the
// Enqueue until RunAt, and keys are shared with the workflow's other
// runs. Meanwhile the run reads StatusScheduled (Status, Runs, a RunQuery
// for StatusScheduled), RunInfo.RunAt is its RunAt and StartedAt stays
// zero; its RunRef holds the RunID and no ScheduledID, a duplicate Enqueue
// (delayed or not) gets the same RunRef, Cancel, CancelAll and
// workflow.CancelChildren cancel it (freeing the key), and Pause and
// Resume of the workflow pause and resume its variant too (Paused lists
// only the workflow). Reschedule refuses it with an UnsupportedError: a
// durable sleep cannot be moved. The gate is not on the workflow itself
// because it would be every run's first task, which would release every
// key a moment after the enqueue. A workflow with a Concurrency has no
// variant and refuses such a start, because the engine keeps concurrency
// slots per workflow, so a variant could not share the workflow's; so does
// a RunAt further ahead than Config.IdempotencyMaxHold, and a client that
// did not register the workflow (an enqueue-only client cannot tell
// whether the workers declared a variant). A delayed start without a key
// stays a Hatchet scheduled run.
//
// An engine call that fails because the engine could not be reached (a
// gRPC Unavailable status, a network error from the REST client, an HTTP
// 502, 503 or 504) returns an error matching workflow.ErrUnavailable, so a
// failover client can tell an outage from a refusal; the caller's own
// cancellation or deadline never does.
//
// Tunings: Config.GroupChunkRuns and GroupChunkBytes size bulk triggers,
// BatchCoordinatorMaxInput caps a coordinator trigger, BatchCoordinatorRetry
// is the coordinator's retry policy, RegisterTimeout bounds Start's wait
// for the worker, and VisibilityLag is how long the engine's read side may
// trail a write. Config.Logger receives the background errors (run-finished
// polls, observer panics, worker stop) and Config.Clock drives what is
// local to the process (poll tickers, the delivered-run window, FinishedAt
// of a delayed start cancelled before it fired); times compared with the
// engine's timestamps stay on the wall clock.
//
// Rate limits are Hatchet's own: a KeyExpr limit becomes a dynamic limit
// (KeyExpr plus LimitValueExpr, so the engine creates each bucket on
// first use). The engine keys a dynamic bucket by the key's value alone,
// per tenant, so the driver prefixes the value with the limit's
// declaration (namespace, workflow, index): two workflows whose keys
// evaluate to the same value have separate buckets, as on the other
// drivers (TestRateLimitScopeLive; before, a run of one waited for the
// other's bucket). A keyless RateLimit is refused (field RateLimits[i].Key):
// its static-limit mapping missed the long-run bound by 1.2s in its live
// test, so it is not claimed; workflow.KeyExpr(`"all"`) gives one bucket
// through the proven dynamic path. Hatchet's
// limiter is a fixed window that is not aligned to the clock: a bucket
// is refilled to Limit once Per has passed since its last refill
// (pkg/repository/sqlcv1/rate_limits.sql, lastRefill), so up to 2×Limit
// runs can start in one Per-long span across a refill, which the core
// RateLimit contract allows. A run held back by the limit stays queued
// and starts after a refill; it is never dropped or failed. Refills
// happen when the engine next checks the bucket, so the next run of a
// full bucket starts up to about 30s after the window ends.
//
// OnRunFinished (workflow.RunNotifier) is implemented by polling the
// engine's run list, and CapRunFinished is not claimed: Hatchet has no
// run-end callback, worker-side hooks cannot see cancelled or dropped
// runs, and a restarted client replays only the ends of runs created
// within Config.RunFinishedCatchUp. A delayed start cancelled before it
// fires is reported only by the process that cancelled it. See notify.go.
//
// Batches (package batch) run natively: the Client implements
// batch.GroupRunner as Hatchet bulk triggers, one RunMany call per chunk of
// up to 1000 jobs of one workflow and about 2 MiB of input, so a batch
// costs about N/1000 calls instead of N Enqueues; each run carries what
// Enqueue stamps. BatchSource is the batch completion source to use with
// it: one run-list call per open batch each tick, filtered by the batch's
// label, instead of one Status call per job; it also resumes a dispatch
// whose process died (batch.Manager.ResumeDispatches), using
// Client.FindBatchRuns (batch.RunFinder) to list which jobs the dead
// process had started, by label, whatever their status.
//
// With Config.BatchCoordinator the Client is also a batch.GroupSpawner:
// Dispatch starts an eligible batch (no RunAt or IdempotencyKey, at most
// about 2 MiB of input; larger ones fall back to bulk triggers) as one
// coordinator run, whose task starts every job as a child run in bulk
// calls, each child keyed by its job index. A retried coordinator gets
// the existing child back for a key the engine has seen, so a worker that
// dies mid-spawn does not start a job twice (proven live by
// TestBatchCoordinatorCrashLive). The children have no refs at Dispatch,
// since the engine names a child only when it is started, so the batch
// keeps the coordinator's ref, BatchSource finds the children by label
// (Dispatch refuses a coordinated batch without it), and a job the
// coordinator never started (it was cancelled, or ran out of retries) is
// recorded as cancelled once the coordinator has ended. The engine does
// not cancel children with their parent (checked live, for a running and
// a completed parent), so CancelGroup cancels the parent and then every
// run carrying the batch's label in one bulk cancel; while a cancelled
// batch is open, BatchSource repeats that each tick, so a child started
// late is cancelled too. The coordinator trigger is idempotent per batch
// for a day (TTL), so a resumed dispatch gets the same coordinator.
// Config.BatchCoordinatorTimeout bounds a coordinator attempt, and is how
// long a coordinator on a dead worker waits for the engine's retry.
//
// Run states, chains, worker scaling and closures are not claimed. The
// engine's run statuses are queued, running, completed, failed and
// cancelled, so a run in retry backoff reads StatusQueued and a
// discarded run StatusFailed (no CapRunStates); a RunQuery or CancelAll
// filter naming StatusRetrying or StatusDiscarded is refused (field
// RunQuery.Statuses) rather than answered with a page that says "none".
// A RunQuery.LabelPrefixes filter is refused the same way by Runs and
// CancelAll (field RunQuery.LabelPrefixes, no CapLabelPrefix): the
// engine filters metadata by exact value only. A RunQuery.Payload filter
// is refused by Runs (field RunQuery.Payload, no CapPayloadSearch) for
// the same reason.
// A Job.Chain is refused at Enqueue, RunGroup and SpawnGroup (field
// Job.Chain, no CapChain), and no chain editor is put on a step's ctx,
// so PrependToChain and AppendToChain return an UnsupportedError: a
// Hatchet run's shape is fixed when its workflow is declared; a step
// that needs follow-up work enqueues it as a new run. The client does
// not implement workflow.WorkerScaler (no CapWorkerScale): the SDK takes
// a worker's slots once, when the worker is created, and has no call to
// change them. It does not implement workflow.ClosureEnqueuer (no
// CapClosure), so workflow.EnqueueClosure refuses: a Go closure cannot be
// sent to the engine. It declares no CapWorkflowVersion and Register
// refuses Workflow.Version and MinVersion: the engine hands a run to any
// worker that registered the workflow's name, so it cannot keep a run
// started by new code away from an old worker during a rolling deploy.
// A durable closure by name (workflow.NewFunc,
// RegisterFunc, FuncJob) needs nothing of the driver: it is a one-step
// workflow, registered and declared like any other, so a process that
// registered the name runs it and an unregistered name is refused at
// Enqueue with ErrNotFound. Its FuncRetry follows the Retry.Backoff rule
// below.
//
// Worker recycling (Config.Recycle, workflow.Recycle): after MaxJobs
// finished handler attempts, MaxTime on Config.Clock since Start, or a
// heap above MaxMemory, the client drains itself as Drain does with no
// deadline: its workers are paused on the engine, the handlers running
// here finish, the workers stop, Done closes and StopCause returns the
// *workflow.RecycleError, so workflow.Serve returns it and a supervisor
// starts a fresh process. A requested stop (the Start ctx ended, Drain)
// leaves StopCause nil. The engine may have handed a worker its next
// attempt just before the pause landed; that attempt runs to the end
// here, so up to Slots attempts can start past MaxJobs (tested with one
// slot by TestRecycleLive). Durable wait and sleep steps do not count,
// since they hold no worker.
//
// Queues, run management, pause and drain: Workflow.Queue and
// Config.Queues give one Hatchet worker per served queue, with that
// queue's Workers as slots (CapQueue); Enqueue stamps the queue and the
// priority on the run. Client.Runs (workflow.RunQuerier) merges the run
// list with the unfired delayed starts and reads each run's details for
// its failed step, attempts and waiting status; CancelAll
// (workflow.BulkCanceller) lists then cancels; Retry
// (workflow.RunRetrier) is Hatchet's replay of the failed and cancelled
// step tasks, which keeps the steps that succeeded; Schedules
// (workflow.ScheduleLister) lists the engine's cron triggers; Drain
// (workflow.Drainer) pauses the client's workers on the engine, waits for
// the handlers running here, then stops them; DrainWithReport
// (workflow.ReportingDrainer, CapDrainReport) also lists the handlers cut
// off at the deadline with the disposition workflow.DrainEngineOwned,
// because whether the engine requeues them is the engine's decision, and
// reads FinishedRuns back from the engine (a worker is not told when a run
// ends): the runs of the handlers that returned are looked up for up to two
// seconds and the terminal ones counted. A named Schedule is one
// trigger per (workflow, name) with one ID ("named:" plus the pair), the
// same across a changed definition, which replaces the engine trigger
// (keeping its pause when Config.API is set). Not claimed:
// CapDiscard (a Discard fails the run, as Permanent does), CapDeleteRun
// (the engine has no run delete), CapPauseQueue (a worker pause is not
// durable across nodes) and CapTickEvents (the engine fires crons without
// the driver). CapPauseWorkflow and CapSchedulePause are declared only
// with Config.API (NewAPI): SDK v0.107.1 has no call for the engine's
// workflow pause or cron enable and exposes no REST client, so the driver
// calls the REST API itself. PauseAll and PauseSchedule(All) are refused
// (the engine stores a pause per workflow and per trigger, with nothing
// that would also hold ones made later); Resume(PauseAll()) and
// ResumeSchedule(All) clear every pause of the namespace. A cron tick
// that falls due while its trigger is disabled is dropped. Retry does not take the idempotency key
// again on the engine: it refuses with ErrDuplicate when another
// unfinished run holds the key, but an Enqueue with that key while the
// retried run is unfinished starts a second run.
//
// Schedule trigger, history and enqueue-only clients:
// TriggerSchedule (workflow.ScheduleTriggerer, CapScheduleTrigger) reads
// a cron trigger, declared or made by Schedule, and enqueues its workflow
// now with the trigger's input and labels plus workflow.TriggeredLabelsAt,
// on a disabled trigger too, leaving its cron untouched. ScheduleInfo.Last
// (CapScheduleHistory) is not stored by the driver: Schedules reads it
// from the engine's run list, as the newest run the engine fired for the
// trigger (by the hatchet__cron_name or, for a declared trigger,
// hatchet__cron_expression metadata the engine stamps on every cron run,
// with Fire from hatchet__cron_scheduled_at) or TriggerSchedule started
// for it (by workflow.LabelSchedule), within
// Config.ScheduleHistoryWindow (a week by default), so it survives
// restarts as long as the engine keeps the runs, costs two run-list calls
// and one run read per trigger, and reads as never fired past the window.
// Config.EnqueueOnly (CapEnqueueOnly) makes a client for web nodes:
// Enqueue, Status, Cancel, Signal, Schedule and the listings work with no
// Start and no worker, connecting on first use; Start starts nothing; a
// registered workflow keeps its checks, an unregistered one gets
// workflow.CheckJob only; OnRunFinished is refused. RunEvent.Step names
// the failed step of a failed run, as RunInfo.FailedStep does. Not
// claimed: CapErrorHistory (the run details carry task events with an
// attempt and an error message, but the driver has not proven that they
// list every failed attempt, and the separate task event list returned
// no rows on hatchet-lite v0.110.5), CapShutdownRequeue (a worker stopped
// by its Start ctx cancels the steps running on it and the engine counts
// that attempt; Drain lets them finish instead) and CapForceRun (the
// engine has no call that completes or fails a run from outside, only
// cancel, replay and restore of an evicted durable task), so the client
// does not implement workflow.RunForcer. The SDK's panic on a client
// config that does not load (a missing token) is turned into an error by
// Start and by the enqueue-only connect.
//
// Tenants, results, rescheduling and the rest of batch 5:
// Job.Tenant (CapTenant) travels as run metadata, reaches StepInfo.Tenant,
// RunInfo.Tenant and RunEvent.Tenant, and RunQuery.Tenant filters on the
// engine. Schedule.Tenant is stored on the cron trigger's metadata, which
// the engine copies onto every run it fires; TriggerSchedule sets it as
// the run's Job.Tenant, Schedules lists it as ScheduleInfo.Tenant, and a
// named schedule whose tenant changed is replaced. Every run the engine's cron fires carries workflow.LabelTickAt
// (CapTickTime), read from the tick time the engine stamps on it
// (hatchet__cron_scheduled_at, the fire time its scheduler computed, whole
// seconds), and TriggerSchedule sets it to the trigger time. A six-field
// expression with seconds first (Schedule.Seconds, Workflow.CronSeconds;
// CapCronSeconds) is sent as is: the engine's scheduler reads six fields
// as seconds first, proven by TestCronSecondsTickLive firing every two
// seconds. Result (workflow.ResultReader, CapResult) and RunInfo.Output
// are workflow.RunOutput of the final steps' outputs, which the engine
// keeps with the run's tasks; a client that never registered the workflow
// rebuilds the steps from the run's shape, so with several final steps
// the result's keys come in name order there, and a token the engine
// withholds payloads from cannot read a result. RunQuery.DueFrom and DueTo
// (CapDueQuery) filter on RunInfo.RunAt, the RunAt of a delayed start (or
// the time Reschedule moved it to), kept after it fires; the filter is
// applied to each listed page, so a page can be short. Concurrency.FIFO
// (CapKeyFIFO) needs nothing of the driver: the engine queues a key's
// waiting runs by insert order (the concurrency slot's sort_id), proven
// for 40 runs over two keys by TestKeyFIFOLive and by the conformance
// case, and a run's Priority still goes first. ConcurrencyRoundRobin maps
// onto the engine's GROUP_ROUND_ROBIN, which serves keys in equal turns
// and takes no weight: Concurrency.Weight is refused (no CapKeyWeights),
// and CapRoundRobin is not declared until a live test measures the
// engine's turns the way the workflowtest "fair" group does.
// Workflow.ConcurrencyLimits are refused at Register (no
// CapConcurrencyLimits): the SDK takes several concurrency expressions per
// workflow, but that the engine admits a run on all of them at once and
// never lets a later run overtake a waiting one on a shared key is not
// proven against a live engine, so the driver refuses them rather than
// honour part of the contract. Concurrency.Hold set to HoldWhileRunning is
// refused at Register too (no CapHoldWhileRunning): the engine holds a
// run's concurrency slot until the run ends, retry backoffs and durable
// waits included, and has no way to give it back while the run waits.
//
// Reschedule (workflow.RunRescheduler) creates the delayed start again at
// the new time with the same token, payload, labels and priority, then
// deletes the old one, so the RunRef is unchanged; if the delete fails the
// copy is removed again and the error says whether that worked. A start
// due within 15 seconds is refused with ErrWrongState: the engine loads a
// scheduled run into its ticker up to five seconds before it is due and
// then fires it at the loaded time whatever a later change says. The
// ScheduledID still names the first RunAt, and new tokens are UUIDv7, so
// Status, Cancel and Runs find a run moved earlier by searching from when
// it was scheduled. The engine deletes a scheduled record as it fires it,
// a moment before its read side lists the run, so a ScheduledID whose
// record is gone and whose run is not listed reads cancelled only after
// Config.VisibilityLag (never by comparing with the first RunAt, which a
// move earlier makes wrong), or at once when this client's Cancel deleted
// it at least 15 seconds before it was due or watched it past its due
// time with no run fired. Status of a delayed start that
// another process cancelled therefore takes up to VisibilityLag.
//
// Config.NodeID (workflow.DefaultNodeID by default) is StepInfo.Node and
// is set as a label on every worker the client registers. Nodes
// (workflow.NodeLister) lists the nodes whose labelled workers the engine
// shows ACTIVE, with their queues, slots and heartbeats; enqueue-only
// clients register no worker and are not listed. CapNodeIdentity is not
// claimed: RunInfo.AttemptedBy and AttemptError.Node need the worker of
// each attempt, and the run details' task events, which would hold it,
// come back empty on hatchet-lite v0.110.5. Health
// (workflow.HealthChecker) is live until a recycle stops the client, and
// ready while it is started, the engine answers its version call and
// every worker it registered is ACTIVE. Enqueue refuses a payload over
// Config.PayloadLimit (DefaultPayloadLimit, 1.5 MiB, sized for the
// engine's default 4 MiB gRPC message, since a JSON object payload is
// stored about 2.34 times over) with a *workflow.PayloadTooLargeError
// (workflow.PayloadLimiter), reports a new run or a duplicate key through
// workflow.WithEnqueueOutcome, and a failed run's RunEvent.Err is a
// *workflow.RunError naming the run, workflow, failed step and its
// attempts, whose text (and RunInfo.Error) is the step's own message.
//
// Not claimed in batch 5: CapSnooze (the engine counts every failed
// attempt; workflow.Snooze is an ordinary error here), CapHeartbeat (the
// engine's RefreshTimeout adds to a task's one execution deadline, so it
// cannot fail an attempt for a missed beat while keeping Step.Timeout;
// workflow.Heartbeat is a no-op), CapRunTimeout, CapScheduleActive and
// CapScheduleKeep (above). The client does not implement
// workflow.StepCanceller (the engine's cancel ends a task as cancelled,
// while the contract has the handler's return decide the step and a
// failure end the run without a retry), workflow.LifecycleNotifier (the
// engine has no insert or start callback) or a progress store
// (workflow.ReportProgress returns an UnsupportedError), nor
// workflow.TransientSignaller (the engine cannot tell which runs are
// suspended on a wait at one moment), workflow.RunEditor,
// workflow.RunResetter or workflow.RunForker (a run's input and shape are
// the engine's once it is created, and its replay re-runs failed tasks
// only). It offers none of the durable stores the core helpers take
// (JournalStore, PromiseStore, DataStore, BucketStore, Inbox, the unique,
// debounce, aggregate, disable and lineage stores): the engine keeps no
// app data, so those helpers keep their in-memory defaults unless given
// the SQL or Redis driver's store.
//
// Hatchet engine quirks (hatchet-lite v0.110.5, SDK v0.107.1) and the
// workaround for each:
//   - Scheduled runs store triggerAt without a zone, so a local time fires
//     hours off: Enqueue sends RunAt in UTC.
//   - A scheduled-run record is deleted when it fires: the run carries a
//     token in its metadata and the ScheduledID encodes it, so Status and
//     Cancel find the fired run by that token.
//   - The engine's ticker polls every five seconds for scheduled runs due
//     within five seconds and arms a timer for each; a delete is noticed
//     only at its next poll, at or after the due time, so a start deleted
//     after it was loaded still fires. Measured by TestLateCancelLive
//     (15 samples per offset, the old Cancel): deleted 10s or 5s before
//     due, 0 of 15 started; 3s before, 6 of 15; 1s before, 12 of 15; 0.5s
//     before, 13 of 15, while Cancel returned nil and reported a cancelled
//     RunEvent. Cancel now deletes a start due within 15 seconds and then
//     watches for its run until the due time plus Config.VisibilityLag
//     (so it can block that long, bounded by ctx): no run listed, it
//     returns nil and the start will not run; a run listed, it cancels
//     that run (unless it already ended) and returns ErrCancelTooLate,
//     which matches workflow.ErrWrongState. The RunEvent is reported only
//     on nil. CancelAll deletes every start first and watches the late
//     ones together; one that fired anyway has its run cancelled and is
//     counted, with no error. Reschedule refuses a start due within 15
//     seconds (ErrWrongState).
//   - A run is missing from the REST read side for about a second after
//     RunNoWait: Status retries a 404 for up to Config.VisibilityLag (5s by
//     default) before ErrNotFound.
//   - Between a failed attempt and its retry the engine shows the task,
//     and often the run, as FAILED for tens of milliseconds, then QUEUED
//     again; during a retry backoff the task is QUEUED. Taken at face
//     value a run that later succeeds would read failed (Status, Runs,
//     Retry, the run-finished hook, BatchSource, a batch's Catch). One
//     function judges every FAILED read (failed.go): it is final when the
//     failure carries the driver's marker (every failure the driver makes
//     non-retryable: Permanent, Discard, NoRetryOn, a wait timeout, an
//     unreadable input, the last attempt the policy allows), when the task used the Retries the engine
//     registered for it, or when the task failed more than
//     Config.FailedSettle ago (5s by default); otherwise the run reads
//     Running or Queued, as between attempts. The marker is stripped from
//     RunInfo.Error and RunEvent.Err, so users read the step's own text,
//     but Hatchet's dashboard shows it. The REST task event list, which
//     would name a retry, returned no rows on hatchet-lite v0.110.5 and is
//     not used.
//   - The engine sometimes retries a failed task past the Retries it was
//     registered with, a step declared with RetriesNone included (seen
//     live: 2 to 4 attempts of such a step in about one run of twelve
//     workflows in two). The driver therefore marks the error of the
//     last attempt the policy allows non-retryable, so the attempt count
//     is the policy's and does not rest on the engine's count alone.
//   - Runs whose failures were stored before the marker existed have no
//     marker: a Permanent failure of such a run with retries left reads
//     Queued for Config.FailedSettle after it failed, then Failed, as
//     before. It matters only for runs failing while the new driver
//     rolls out, and ends when those runs end.
//   - A task held by a rate limit waits outside the scheduler until its
//     bucket's next window, then is requeued, and the scheduler fails it,
//     with an empty error and no attempt, once its schedule timeout
//     (counted from creation) has passed. The engine's default is five
//     minutes, so a run a busy limit held past that failed (the ratelimit
//     conformance group passed 1 of 5 times before the fix, 5 of 5
//     after). Every step and the batch coordinator declare
//     Config.ScheduleTimeout (a day by default) instead
//     (TestScheduleTimeoutLive). The on-failure task takes no options in
//     the SDK and keeps the five minutes.
//   - The SDK panics when a worker is stopped before it registers: Start
//     returns only once the engine lists the worker.
//   - Delete endpoints answer 204, which the SDK reports as an error: a
//     204 is treated as success.
//   - A DAG wait condition exposes neither the event payload nor which
//     condition fired: wait steps run as durable tasks (WaitFor).
//   - A wait with WaitSpec.OwnRun resolves its key inside that durable
//     task from the engine's workflow run ID (the RunID refs and StepInfo
//     carry), so SignalRun and SignalParent reach one run
//     (CapWaitOwnRun, TestOwnRunWaitLive). The driver has no
//     workflow.RunSignalReader: the engine keeps no record of which
//     events a run took that a step could read, so workflow.OnSignals is
//     refused at Use.
//   - A durable wait filtered by a CEL expression never resumed once a
//     non-matching event of the same name arrived in its look-back window:
//     the correlation key is part of the event key and scope instead, and
//     events count from the run's enqueue time.
//   - A step whose worker process dies is reclaimed when the step's
//     execution timeout expires and then retried (it needs a retry
//     left): the live crash case abandons a client mid-step with a 15s
//     timeout and the step reruns about 28s later. In production the
//     delay is the step's timeout plus five seconds (Config.StepTimeout,
//     24h by default, for a step without one), so set Step.Timeout near
//     the step's real duration. Reclaim through missed worker heartbeats
//     is not tested.
//   - A durable task's sleep counts against its execution timeout (seen
//     live): the start gate is declared with Config.IdempotencyMaxHold
//     plus five seconds, and a keyed delayed start further ahead than
//     IdempotencyMaxHold is refused. A wait step is a durable task too,
//     and its event wait counts the same way, so the driver declares a
//     wait step's execution timeout as long as the wait can last plus
//     five seconds: its Timeout, or the time left to its Deadline when
//     the client starts, in whole seconds; Step.Timeout (else
//     Workflow.Timeout) raises it and never lowers it. A wait that
//     reaches its Timeout or Deadline fails with workflow.TimeoutError
//     (ErrWaitTimeout), as on the other drivers. A wait with neither
//     gets Config.StepTimeout (or a longer Step.Timeout), after which the
//     engine ends it with its own text. A sleep step is not affected:
//     its sleep is a condition the engine holds before the task starts
//     (TestLongWaitLive).
//   - A task's error message comes back as a JSON string, in double
//     quotes, in the run details and the run list: RunInfo.Error and the
//     run-finished error keep the text as read (marker stripped), and a
//     step or wait timeout is matched inside the quotes.
//   - The engine rejects a rate limit without Key even when KeyExpr is
//     set ("Validation failed" at worker registration): every limit
//     carries a Key naming it.
//   - Runs are listed by creation time only, with no "finished since"
//     filter: the run-end notifier tracks unfinished runs between polls.
//   - A run's stored input nests the trigger input under "input": Runs
//     reads the payload from there.
//   - Workflows reach the engine only through a worker's registration:
//     Enqueue of a workflow of a queue that no process has ever served
//     fails, because the engine does not know the workflow yet.
//   - The workflow list's name filter matches by substring, and the SDK's
//     Workflows().Get returns the first row: pausing "x" paused "x_ctl"
//     in a live test. The driver compares every row's full name.
//   - Workflow pause takes a mandatory queue TTL, after which the engine
//     CANCELS the held runs (checked live with a 10s TTL in
//     TestPauseLive), and an empty or unparsable TTL silently becomes five
//     minutes: Pause sends pauseQueueTTL, a hundred years, pinned by
//     TestPauseRequestTTL; TestPauseHoldLive (WFTEST_PAUSE_TTL=1) holds a
//     run for six minutes and then runs it.
//
// Every claim in Capabilities is proven by the shared workflowtest suite
// against a real engine (live_test.go, `task test:live`).
package hatchet
