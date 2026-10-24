// Package memory is the in-process driver of workflow.Client: runs, waits,
// sleeps, schedules and queues live in maps and goroutines inside the
// client value, with no database and no server.
//
// It is for tests, local runs and apps that can afford to lose queued work.
// Everything is lost when the process exits, so it declares every
// capability except workflow.CapDurable (including the retry jitter and
// delay, misfire and cron-zone capabilities), and Start refuses nothing on that
// ground: durability is never inferred from a workflow (see
// workflow.Requires), the app picks a durable driver when it needs one.
//
// # Time
//
// Every timer (delayed starts, sleep steps, wait deadlines, retry backoff,
// step timeouts, rate-limit windows, cron fires) comes from the injected
// clock.Clock, so a test that passes clock.NewFake makes "sleep three days"
// run in microseconds. Handlers still run on real goroutines.
//
// # Execution model
//
//   - A run is admitted when its concurrency bucket has room and its rate
//     limits allow a start; until then it is StatusQueued.
//   - An admitted run drives its steps as a DAG: a step starts once every
//     After parent has succeeded, with its input built by
//     workflow.StepInput. A wait step outputs the signal's payload; a sleep
//     step passes its input through.
//   - Run steps execute on a fixed pool of workers (WithWorkers). Waits and
//     sleeps do not hold a worker. Among step attempts waiting for a
//     worker, the higher QueueConfig.Priority of their queue goes first,
//     then the higher Job.Priority, then first come first served. An
//     attempt held by a pause (Pause) or by its queue's Workers cap waits
//     without blocking the others.
//   - A failed attempt is retried per workflow.EffectiveRetriesFor
//     (Job.Retry, else the step's, else the workflow's) as decided by
//     RetryPolicy.Next on the injected clock: Max, MaxErrors (releases
//     marked by RetryAfter do not count), the delay (DelayFor, else the
//     backoff with its Jitter drawn from WithJitterSource) and Until from
//     the step's first attempt. Permanent errors, NoRetryOn and
//     cancellation are never retried. When a step fails for good the
//     run's other steps are cancelled, OnFailure runs once with the failing
//     step's input, and the run becomes StatusFailed (also when MaxErrors
//     or Until stops it) unless the policy sets RetryPolicy.DiscardOnExhaust,
//     which ends it StatusDiscarded with no OnFailure (CapDiscardExhausted,
//     CapRunStates). While a step waits on its retry backoff and no
//     attempt executes, the run reads StatusRetrying and RunInfo carries
//     NextAttempt and NextAttemptAt. A step returning workflow.Discard
//     ends the run StatusDiscarded at once, with no retry and no
//     OnFailure (workflow.EndStatus; CapRunStates).
//   - A wait that reaches its Deadline or Timeout fails the run (waits are
//     not retried). A Signal is kept: a run that reaches a matching wait
//     step after the Signal takes the first one sent after the run was
//     enqueued, and never takes the same signal twice. Signals older than
//     the oldest unfinished run are dropped.
//
// # Policies
//
//   - Idempotency: a key is held from Enqueue until the run is terminal; a
//     colliding Enqueue starts nothing and returns the holder's RunRef with
//     a nil error, as workflow.Client documents.
//   - Concurrency: only Go-function keys (workflow.KeyFunc); an expression
//     key (concurrency, rate limit or WaitSpec.KeyFrom) is refused at Start
//     with an UnsupportedError. QueueNewest and
//     RoundRobin hold the new run until its bucket has room (each bucket is
//     capped at MaxRuns, so no key can starve another); DropNewest and
//     CancelNewest record the new run as StatusCancelled; CancelInProgress
//     cancels the bucket's oldest running run to make room. A cancelled
//     run releases its slot at once, even if a handler that ignores its
//     context is still returning.
//   - Rate limits: a sliding window of start times per key on the injected
//     clock; a limited run waits queued until the window frees.
//   - Cron: Workflow.Cron fires in its CronLocation (UTC when nil) and
//     Client.Schedule in its Location, both from Start (or the Schedule
//     call) onwards. Expressions are parsed by cron.Parse unless
//     WithCronParser supplies another dialect, which gives up "the same
//     spec on every driver" for what only that parser understands; a
//     parser that returns neither a schedule nor an error is refused.
//   - Misfires: the default is fire-once with no age limit: after a long
//     pause (a fake clock jumping several ticks) the newest tick fires once
//     and the older ones are reported as one TickMisfired. Every explicit
//     workflow.Misfire policy is honoured (CapMisfireSkip,
//     CapMisfireFireOnce, CapMisfireFireAll) through workflow.Misfire.Plan
//     over the ticks since the last catch-up.
//
// # Queues
//
// Without WithQueues the workers serve every queue. With it they serve
// only the listed queues, each with its own Workers cap and Priority; a
// workflow or a Job.Queue on a queue not listed is refused (ErrInvalid),
// since nothing in this process would ever run it. QueueConfig.Limit caps
// the step attempts of a queue running at once; this driver is one
// process, so the process is the whole cluster and the cap holds across it
// (CapQueueLimitGlobal), where Workers is the same kind of per-client cap.
//
// # Operating on runs
//
// The client keeps finished runs within RunRetention (by default a day
// and at most 10,000, DefaultRunRetention; WithRunRetention changes it)
// and unfinished runs always, so it implements the run query family: Runs (newest enqueue first, a position cursor), Retry
// (the failed run keeps its RunRef and resumes after the steps that
// succeeded; its concurrency and rate limits apply again), Delete and
// CancelAll. Pause and Resume hold runs by workflow, queue or all.
//
// # Schedules
//
// Declared crons get the stable ID "cron:<workflow>:<index>", are listed
// by Schedules and can be paused but not unscheduled. A named
// Client.Schedule is upserted by (Workflow, Name). Each tick passes the
// misfire policy, then the schedule's Windows, then the workflow's
// ScheduleWhen; fired runs carry workflow.FiredLabels, and every outcome
// goes to OnTick observers on the notifier goroutine. A paused schedule
// reports its ticks as skipped and never fires them, not even on resume;
// it resumes at its next tick.
//
// # Chains, scaling and closures
//
// Job.Chain is kept with the run: each step attempt gets a
// workflow.ChainEdits on its ctx, whose PrependToChain and AppendToChain
// edits apply only if the attempt succeeds, and once the run succeeds the
// next job (workflow.NextInChain) is enqueued under the same lock that
// recorded the success. A chained job that cannot be enqueued (its
// workflow not registered, the client stopping) ends the chain and is
// logged. A run that Retry re-opens keeps its edited chain. Like every run
// here, a chain lives only in this process.
//
// The client implements workflow.WorkerScaler over the queues given to
// WithQueues: SetWorkers changes a queue's Workers cap while it runs and
// QueueLoads reports each queue's backlog, oldest wait, cap and running
// attempts. The caps share the WithWorkers pool, so give the pool at least
// a balancer's Total. Nothing balances unless the app runs a
// workflow.BalanceLoop.
//
// EnqueueClosure (workflow.ClosureEnqueuer, CapClosure) runs a Go function
// as a run of workflow.ClosureWorkflow through the same queues, retries,
// middleware and observers as any run. It is lost with the process, as
// every run of this driver is; a durable driver refuses closures.
//
// # Run ends
//
// The client implements workflow.RunNotifier and declares
// workflow.CapRunFinished: every run that reaches a terminal status is
// delivered once to each observer, including runs cancelled before they
// started (a cancelled RunAt run, whose Ref carries only the ScheduledID),
// DropNewest and CancelNewest runs, and runs cancelled by shutdown. A
// cancelled run's Err matches workflow.ErrCancelled; a failed run's Err
// wraps the step's original error. FinishedAt comes from the injected
// clock. Observers run on one notifier goroutine, in registration order
// and in the order runs ended, outside the client's lock, so they may call
// back into the client; a panicking observer is logged and skipped. An
// idempotency collision starts no run and produces no event.
//
// # Shutdown
//
// Cancelling the context passed to Start, or calling Stop, shuts the
// client down: every unfinished run becomes StatusCancelled (and is
// delivered to run-finished observers, whose ctx is by then cancelled), every
// goroutine the client started exits, and later calls fail with
// ErrNotStarted. Stop also waits for that to finish, so prefer it in tests.
// Drain stops claiming attempts, waits for the running ones until its ctx
// ends, then stops the same way; WithStopWhenIdle drains by itself once
// the last unfinished run has ended and nothing new came for the grace;
// WithRecycle drains by itself once a workflow.Recycle limit (finished
// attempts, time since Start, memory) is reached, and StopCause then
// returns the *workflow.RecycleError, which workflow.Serve returns.
// Done closes once everything has stopped. Because a stop cancels every
// unfinished run, the client does not declare
// workflow.CapShutdownRequeue: there is no store for a stopped attempt to
// go back to. DrainWithReport (workflow.ReportingDrainer, CapDrainReport)
// is Drain that also returns a workflow.DrainReport: the steps and runs
// that finished during the drain and every attempt still running at the
// deadline, listed with the disposition DrainCancelled, because the run
// became StatusCancelled.
//
// # Operator calls and history
//
// Schedules honour workflow.OverlapSkip (Schedule.Overlap,
// Workflow.CronOverlap) from the status of their last run, which the
// client keeps per schedule with its fire time (ScheduleInfo.Last,
// workflow.CapScheduleHistory; a named upsert keeps it). TriggerSchedule
// runs a schedule now (workflow.ScheduleTriggerer). ForceComplete and
// ForceFail end a run from outside (workflow.RunForcer). RunInfo.Errors
// keeps each run's failed attempts, the newest workflow.DefaultErrorHistory
// unless WithErrorHistory says otherwise; Retry keeps them.
//
// The client has no enqueue-only mode (workflow.CapEnqueueOnly): its runs
// live in this process, so a client that never started would hold work
// nobody runs. Enqueue keeps needing Start. For the same reason it has no
// scheduler-only or worker-only mode (workflow.NodeScheduler,
// workflow.NodeWorker): nothing else would run what it fires. To fire
// crons here and run jobs on a durable driver, use package hybrid.
//
// # Run data, steps and nodes
//
// A step may Snooze (put back without spending an attempt; the run reads
// StatusWaiting), Heartbeat (with Step.HeartbeatTimeout, an attempt that
// stops beating fails with workflow.ErrHeartbeatTimeout) and
// ReportProgress (kept as RunInfo.Progress). Workflow.RunTimeout ends a
// run that runs too long and Workflow.OnTimeout replaces OnFailure for any
// timeout. Job.Tenant is kept and filtered by RunQuery.Tenant. A
// succeeded run keeps its result (workflow.RunOutput; Result,
// RunInfo.Output). Reschedule moves a delayed run, CancelStep withdraws
// one running or parked step, OnLifecycle streams inserted, started,
// retrying, snoozed, progress and finished events. The failed run's error
// is a workflow.RunError. Concurrency.FIFO holds: queued runs of one key
// are admitted in enqueue order. QueueConfig.Weight shares workers between
// queues of equal priority by a weighted draw from the jitter source.
// Every attempt names this node (WithNodeID, else
// workflow.DefaultNodeID); Nodes lists the one node and Health reports
// it live until stopped and ready while started and not draining.
// WithPayloadLimit refuses oversized payloads at Enqueue. Schedules honour
// Seconds (cron.ParseSeconds), Active, KeepRuns and stamp
// workflow.LabelTickAt on every fired run.
//
// RunAll, for tests, runs everything now: delayed starts, retry backoffs,
// snoozes and sleeps end at once, and it returns once no run can move
// (finished or waiting for an event), with a RunAllReport, or with
// ErrRunAllStalled when runs are left that only the clock, a resume or a
// free slot can move. Timeouts keep following the clock.
package memory
