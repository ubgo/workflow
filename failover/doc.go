// Package failover joins two workflow clients into one: enqueues go to a
// primary backend, and when the primary is unavailable they go to a
// secondary, so a job is still accepted while (say) the Hatchet engine is
// down and a local SQL store takes the work.
//
//	c := failover.New(hatchetClient, sqlClient)
//	c.Register(wf) // registers on both
//	c.Start(ctx)   // starts both
//	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "email", IdempotencyKey: "order-42"})
//	st, err := c.Status(ctx, ref) // asks whichever backend holds the run
//
// # What fails over
//
// Only Enqueue fails over, and only when the Classifier says the primary's
// error means "backend unavailable". DefaultClassifier never fails over on
// a cancelled or expired caller context, nor on an error the secondary
// would return just the same (ErrInvalid, ErrUnsupported, ErrNotFound,
// ErrNotStarted, ErrAlreadyStarted, ErrDuplicate, ErrCancelled, a
// Permanent error); an error a driver marked with workflow.Unavailable,
// and everything else, counts as unavailable. The Unavailable mark wins
// over those sentinels: a driver's own timeout talking to its engine wraps
// context.DeadlineExceeded, and marked Unavailable it fails over, while the
// caller's own cancelled or expired context is checked before any
// Classifier runs and never fails over. StrictClassifier fails over
// only on errors marked with workflow.Unavailable, for deployments whose
// drivers all mark their outages; a deployment that can tell another way
// (a gRPC Unavailable code, a refused connection) passes its own
// Classifier through WithClassifier.
//
// # Running a job twice
//
// A failure can be ambiguous: the primary may have accepted the job and
// then failed to answer (a timeout after the write). Failing over then
// runs the job on both backends. So a Job without an IdempotencyKey is
// refused with ErrKeyRequired (which also matches workflow.ErrInvalid),
// unless the client was built with AllowWithoutKey. A key dedups only
// within one backend: the primary and the secondary do not see each
// other's keys, so the key protects a caller's retries against the same
// backend, and the handlers must still be idempotent across backends. A
// workflow with IdempotencyOff ignores keys altogether.
//
// # Run references
//
// Every RunRef this client returns, and every RunEvent and StepInfo it
// passes on, names its backend: RunID and ScheduledID carry the prefix
// "primary:" or "secondary:" in front of the driver's own ID, which is
// kept unchanged after it, so Status, Cancel and the run-finished hook
// reach the backend holding the run. A ref without a prefix (one a driver
// handed out directly) is tried on the primary, then on the secondary when
// the primary does not know it or is unavailable. A driver's middleware
// sees the driver's own RunID in StepInfo; the handler sees the prefixed
// one.
//
// # Capabilities
//
// Capabilities is the intersection of both backends', so Start refuses a
// workflow the secondary could not run rather than discovering it during
// an outage. The exceptions: the schedule family follows where schedules
// live (see below); CapRunFinished needs both backends to implement
// workflow.RunNotifier; and CapClosure and CapWorkerScale are the
// primary's alone. EnqueueClosure goes to the primary with no failover,
// because a closure has no idempotency key and an ambiguous failure
// retried on the secondary could run it twice; QueueLoads and SetWorkers
// scale the primary's workers (balance the secondary with a loop of its
// own). CapRunStates and CapChain are the intersection like the rest. A
// chained job (Job.Chain) is enqueued by the backend whose run finished
// it, never through this client, so it does not fail over: a chain stays
// on the backend that took its first job.
//
// # Managing runs
//
// The client implements every optional run interface, each needing its
// capability on both backends and refused with an UnsupportedError
// otherwise, never answered from one backend alone. Runs merges both
// backends' feeds newest first with prefixed refs and a cursor that
// records each feed's position; Retry and Delete go to the backend the
// ref names; CancelAll, Pause and Resume go to both (Paused lists the
// union); Drain drains both.
//
// # Schedules
//
// Workflow.Cron and Schedule live on the primary only, because a schedule
// on both backends fires on both: every tick would run twice. The cost is
// that no schedule fires while the primary is down. WithSchedulesOnBoth
// puts them on both for apps whose scheduled handlers are idempotent per
// tick. The schedule capabilities (CapCron, CapScheduleWhen,
// CapScheduleWindow, CapSchedulePause, CapScheduleList, CapTickEvents)
// are the primary's alone by default and need both under
// WithSchedulesOnBoth; the secondary's copy of a workflow then carries no
// Cron, CronWindows or ScheduleWhen. Ticks, listed schedules and the
// LabelSchedule of fired runs carry the backend prefix; PauseSchedule
// takes those IDs or the one Schedule returned.
//
// # Signals and run ends
//
// Signal goes to both backends, since a waiting run may sit on either;
// when one of them fails the error says which, and the other has already
// taken the event, so a retried Signal may reach that one twice.
// OnRunFinished registers the observer on both backends and needs both to
// implement workflow.RunNotifier; CapRunFinished holds only when both
// guarantee delivery.
//
// # Cool-off
//
// WithCoolOff stops trying the primary for a while after N consecutive
// failovers, so an outage does not cost every enqueue a timeout first. It
// reads time from WithClock (clock.Real by default).
//
// # Observing
//
// WithObserver reports each failover, each enqueue sent straight to the
// secondary during a cool-off, and the start and end of each cool-off, so
// an app can log, count or alert on them; by default nothing is reported.
package failover
