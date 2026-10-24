// Package redis is the Redis driver of workflow.Client, on go-redis v9:
// every run, schedule, lease and idempotency key is a Redis key, so a
// restart or a dead worker loses nothing, and any number of clients on
// any number of machines share one Redis.
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
//	c, _ := redis.New(rdb, redis.WithPrefix("myapp"))
//	_ = c.Register(wf)
//	_ = c.Start(ctx) // nothing runs before this; cancel ctx to stop
//
// The package is named redis like go-redis itself, so import one of the
// two under another name (goredis above).
//
// # Capabilities
//
// Declared, each proven by the workflowtest conformance suite against a
// real Redis: CapDurable, CapDelayedStart, CapDurableSleep,
// CapWaitForEvent, CapIdempotencyKey, CapConcurrencyKey (every strategy)
// and CapKeyFIFO, CapRateLimit (sliding windows), CapCron, CapCancel,
// CapRunFinished, CapRetryJitter and CapRetryDelay (delays computed in Go
// by workflow.RetryPolicy.Next; WithJitterSource pins the draws), CapQueue
// and CapQueueOverride, CapJobRetry, CapRetryUntil, CapRetryMaxErrors,
// CapDiscard, CapDiscardExhausted (RetryPolicy.DiscardOnExhaust) and
// CapRunStates (StatusRetrying with the next attempt,
// StatusDiscarded), CapSnooze, CapHeartbeat, CapRunTimeout, CapTenant,
// CapNodeIdentity, CapResult, CapDueQuery, CapMisfireSkip,
// CapMisfireFireOnce, CapMisfireFireAll, CapCronZone, CapCronSeconds,
// CapScheduleActive, CapScheduleKeep, CapTickTime, CapScheduleWhen,
// CapScheduleWindow, CapTickEvents, CapSchedulePause, CapRunQuery,
// CapLabelPrefix, CapRetryRun, CapDeleteRun, CapCancelAll,
// CapPauseWorkflow, CapPauseQueue, CapChain, CapDrain, CapDrainReport,
// CapScheduleList,
// CapScheduleTrigger, CapScheduleHistory, CapOverlapSkip, CapEnqueueOnly,
// CapErrorHistory, CapShutdownRequeue, CapForceRun and CapWorkerScale
// (QueueLoads and SetWorkers over the WithQueues list; each ready set has a
// companion scored by when its runs became claimable, so a queue's oldest
// wait is one read; measured with 3,000 runs, no throughput cost). The client also
// implements the interfaces that need no capability: RunRescheduler,
// StepCanceller, HealthChecker, LifecycleNotifier and PayloadLimiter
// (512 MiB, the largest value Redis stores; WithPayloadLimit lowers it).
//
// Not declared, so Start, Enqueue or Schedule refuse what needs them with
// an UnsupportedError instead of ignoring it: closures (a closure cannot
// be stored) and CapQueueLimitGlobal (a non-zero QueueConfig.Limit is
// refused at Start: a run is claimed whole, so a cluster-wide count of a
// queue's in-flight step attempts is not kept; Workers is the per-client
// cap). Concurrency, rate and wait keys are Go
// functions (workflow.KeyFunc); Start refuses an expression key (KeyExpr).
// QueueConfig.Weight orders queues of equal priority by a weighted draw
// (workflow.PickWeighted on the jitter source).
//
// # How work moves
//
//   - Enqueue writes a run hash ({prefix}:run:<id>) and puts its ID in the
//     ready set of its queue and workflow ({prefix}:ready:<queue>/<workflow>,
//     ordered by priority, then by ID, which starts with the creation
//     time), or in the due set ({prefix}:due, by due time) for a RunAt in
//     the future. A run of a workflow with a concurrency or rate policy,
//     or of a workflow the enqueuing client did not register (an
//     enqueue-only client, a chained job, a schedule fired elsewhere),
//     first waits in {prefix}:pending:<workflow>: the dispatcher of a
//     client that registered the workflow evaluates its queue and keys in
//     Go and admits it in one script against the stored concurrency
//     buckets (the runs holding a slot until they end) and rate windows
//     (sliding: no span of Per holds more than Limit starts), so the
//     limits hold across every client. An idempotency key is a field of
//     {prefix}:idem checked and set in the same script, so two clients
//     racing on one key make one run; the key is released when the run
//     ends. A published message wakes idle workers at once.
//   - A run is claimed whole. The claim script moves due runs to their
//     ready sets, hands back runs whose lease expired, then takes the best
//     ready run of the workflows this client registered, and records the
//     claim token and the lease end ({prefix}:leases). The holder renews
//     the lease every third of WithLease. A client claims only workflows
//     it registered, and with WithQueues only the queues it serves, the
//     higher QueueConfig.Priority first; a run is claimed whole, so
//     QueueConfig.Workers caps the runs of a queue one client executes at
//     once. A pause ({prefix}:paused: everything, a workflow or a queue) is
//     checked by the claim and by the holder between rounds, so every
//     client obeys it and it survives a restart.
//   - The holder runs every step whose parents are done, in parallel,
//     round by round, and records each attempt's outcome in the run hash.
//     Every write the holder makes compares its token with the run's
//     owner, so a holder whose lease expired and whose run another client
//     took over writes nothing: each step outcome is recorded once per
//     lease. A step whose result was lost with its worker runs again (at
//     least once).
//   - A retry with a backoff, or a sleep step, hands the run back to the
//     due set at the time it is ready again; the run reads StatusQueued
//     (StatusWaiting for a sleep) meanwhile and holds no worker. Retries
//     follow workflow.EffectiveRetriesFor and RetryPolicy.Next with the
//     step's stored counts, so Until and MaxErrors hold across restarts.
//   - A wait step looks for a kept signal of its name and key sent since
//     the run was enqueued (signals are numbered; a run takes only those
//     above the number current at its enqueue) and otherwise registers in
//     {prefix}:waiters:<name>\n<key> and hands the run back until its
//     deadline (StatusWaiting, holding no worker). Signal keeps the signal
//     and resolves every registered wait on its name and key, one per run,
//     in one script, and makes those runs ready. A wait past its deadline
//     fails its step (OnFailure runs). Kept signals are dropped once no
//     unfinished run may take them.
//   - A step that fails for good runs OnFailure once with its input (under
//     the lease, so a crash reruns it: at least once), then the run is
//     failed. A step returning workflow.Discard ends the run
//     StatusDiscarded, without retries or OnFailure. Retry re-opens a
//     failed run from its failed step, admitted again under its
//     workflow's policies.
//   - A run's chain (Job.Chain and the edits its steps made with
//     PrependToChain and AppendToChain, applied only when the attempt
//     succeeds) is stored with the run; the script that ends the run
//     succeeded creates the next job's run, so no crash loses a link.
//   - Every script that ends a run records it in {prefix}:ended:<workflow>.
//     A client with OnRunFinished observers claims ends of its workflows
//     under a lease, calls the observers and then removes the entry: at
//     least once, on one client, including ends that happened while no
//     client was running.
//   - A run whose lease expires more than WithMaxReclaims times in a row
//     (default 5) with no step finishing in between fails with
//     ErrAbandoned, so a step that kills every worker it lands on cannot
//     loop through the fleet.
//   - Stopping a client (cancelling the Start ctx, or Drain once its ctx
//     ends) hands every run it holds back at once, without counting the
//     cut-off attempts (CapShutdownRequeue): the next worker runs them
//     with the same StepInfo.Attempt. DrainWithReport lists those
//     attempts with the disposition workflow.DrainRequeued. A crashed client hands back nothing;
//     its runs are claimed again once their leases expire.
//   - Cancel, ForceComplete and ForceFail end the run in one script and
//     publish a stop message; the holder cancels its steps (the context
//     cause is workflow.ErrCancelled or workflow.ErrForced) and its later
//     writes are fenced off.
//   - Schedules ({prefix}:sched:<id>, next fire times in {prefix}:scheds)
//     fire on any client that registered their workflow (its ScheduleWhen
//     is code only such a client has), except a WithoutScheduler worker
//     node; WithSchedulerOnly makes a node that fires schedules and runs
//     no worker. Windows, ScheduleWhen and pauses are decided per tick on
//     the firing client and reported to OnTick observers (TickFired,
//     TickSkipped with its reason, TickMisfired), once, by the client that
//     moved the schedule. A tick is fired by a script that moves the
//     schedule's last catch-up time from the value the firing client read
//     and creates the tick's runs in the same step, so a tick runs once
//     across any number of clients. The default misfire policy fires the
//     newest missed tick once; a workflow.Misfire fires what Misfire.Due
//     picks. Under OverlapSkip a tick, or a TriggerSchedule, starts
//     nothing while the run of the schedule's last fire is unfinished,
//     decided in the same script. Fired runs carry workflow.FiredLabels.
//   - Client.Schedule with a Name is an upsert by (Workflow, Name); an
//     unchanged expression, zone and misfire policy keep the next fire
//     time, so an app can declare its schedules at every boot.
//     Workflow.Cron entries are schedules with the stable IDs
//     "cron:<workflow>:<index>".
//   - Runs walks an index of runs by enqueue time ({prefix}:runs) and
//     filters in Go. A finished run's key expires after WithRetention
//     (24 hours by default; Redis measures it, so it is wall time), and
//     the index drops it when it is next read.
//
// # Time
//
// Due times, sleeps, backoff, step timeouts and cron fires are on
// WithClock (the default is the real clock); leases and polling are on
// WithLeaseClock, because a lease is about a worker being alive. Both are
// read in Go and passed to the scripts, so clients must keep their clocks
// close: a client whose clock runs ahead claims due work early. Rate
// windows are measured on WithClock; Redis expires an idle window's key
// after twice its Per plus a minute of wall time.
//
// # Deployment notes
//
//   - Every key of a prefix carries the hash tag {prefix}, so on a Redis
//     Cluster one prefix lives in one slot: correct, but one prefix does
//     not spread over several shards. Proven on a single Redis node only.
//   - Use a Redis that does not evict keys (maxmemory-policy noeviction):
//     an evicted run or lease key is lost work, not a cache miss.
//   - Durability is Redis's: with AOF every second a crash of Redis
//     itself can lose up to a second of enqueues and state changes.
//   - Run ends wait in {prefix}:ended:<workflow> until a client with an
//     OnRunFinished observer delivers them, or until their run expires
//     (WithRetention); with retention off and no observer anywhere, that
//     index grows with every run, as the run index does.
//   - Every started client reports in to {prefix}:nodes every third of
//     WithLease; Nodes lists those seen within three leases (a crashed
//     node drops out after that). WithNodeID names the node.
//   - A run past Workflow.RunTimeout (measured from admission, or from
//     RunAt for a delayed run) is failed with workflow.ErrRunTimeout by
//     its holder, or made ready by the dispatcher when no client holds it
//     so a worker fails it; a run held back by a pause times out on
//     resume.
//   - UniqueStore (Client.UniqueStore or NewUniqueStore) keeps the locks
//     of the middleware package's Unique and UniqueWith in Redis, shared
//     by every process, with the holder's run for UniqueConfig.States and
//     ReturnExisting; a lock's ttl is a Redis expiry (wall time).
//   - JournalStore (workflow.JournalStore, step memoisation: a hash per
//     run, optional expiry), BucketStore (workflow.BucketStore, leaky
//     buckets: the drain and the take are one script, on the caller's
//     clock) and DebounceStore (middleware.DebounceStore: Swap is one
//     script) keep state other processes share; each has a constructor
//     (NewJournalStore, NewBucketStore, NewDebounceStore) and a Client
//     method over the client's prefix.
//   - WithEnqueueOnly makes a client for web nodes that enqueue and never
//     run work; Register is optional there.
package redis
