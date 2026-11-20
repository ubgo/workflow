package workflow

// The capabilities. Each is a one-capability Caps (see capset.go for the
// set itself). A new capability takes the next free number, which is never
// reused or renumbered: the number is the capability's identity in the
// legacy numeric JSON form (Caps.UnmarshalJSON), and TestCapsDistinct
// fails when two capabilities share one. A new bit is kept for a spec
// field that Requires, RequiresJob or RequiresSchedule must check, or for
// data a driver would otherwise leave silently empty. An optional
// interface whose call can answer for itself (RunRescheduler,
// StepCanceller, HealthChecker, LifecycleNotifier, ReportProgress) gets
// no bit: it returns an UnsupportedError when the driver cannot do it.
//
// They are variables only because Go has no constants of a struct type;
// never assign to one.
var (
	// CapDurable: runs, waits and sleeps survive a process restart.
	CapDurable = defineCap(0, "Durable")
	// CapDelayedStart: Job.RunAt is honoured.
	CapDelayedStart = defineCap(1, "DelayedStart")
	// CapWaitForEvent: wait steps suspend without holding a worker until
	// a Signal or their deadline. Whether the wait survives a restart is
	// CapDurable's claim.
	CapWaitForEvent = defineCap(2, "WaitForEvent")
	// CapDurableSleep: sleep steps suspend without holding a worker.
	// Whether the sleep survives a restart is CapDurable's claim.
	CapDurableSleep = defineCap(3, "DurableSleep")
	// CapConcurrencyKey: Workflow.Concurrency is enforced per key.
	CapConcurrencyKey = defineCap(4, "ConcurrencyKey")
	// CapRateLimit: Workflow.RateLimits are enforced.
	CapRateLimit = defineCap(5, "RateLimit")
	// CapIdempotencyKey: Job.IdempotencyKey dedups enqueues.
	CapIdempotencyKey = defineCap(6, "IdempotencyKey")
	// CapCron: Workflow.Cron and Client.Schedule fire.
	CapCron = defineCap(7, "Cron")
	// CapCancel: Client.Cancel stops a run.
	CapCancel = defineCap(8, "Cancel")
	// CapRunFinished: the driver implements RunNotifier with the
	// guaranteed delivery described there (every terminal run, at least
	// once). A driver that implements RunNotifier without this capability
	// delivers best-effort only.
	CapRunFinished = defineCap(9, "RunFinished")
	// CapRetryJitter: Backoff.Jitter spreads retry delays.
	CapRetryJitter = defineCap(10, "RetryJitter")
	// CapRetryDelay: RetryPolicy.DelayFor sets a retry's delay from the
	// error (RetryAfter).
	CapRetryDelay = defineCap(11, "RetryDelay")
	// CapMisfireSkip: Misfire{Policy: MisfireSkip} is honoured.
	CapMisfireSkip = defineCap(12, "MisfireSkip")
	// CapMisfireFireOnce: Misfire{Policy: MisfireFireOnce}, with Within,
	// is honoured.
	CapMisfireFireOnce = defineCap(13, "MisfireFireOnce")
	// CapMisfireFireAll: Misfire{Policy: MisfireFireAll}, with Within and
	// Max, is honoured.
	CapMisfireFireAll = defineCap(14, "MisfireFireAll")
	// CapCronZone: Workflow.CronLocation and Schedule.Location may be a
	// zone other than UTC, evaluated in that zone across DST.
	CapCronZone = defineCap(15, "CronZone")
	// CapQueue: Workflow.Queue and Job.Queue route runs to named queues,
	// and the driver's per-queue worker limits (QueueConfig) are honoured.
	CapQueue = defineCap(16, "Queue")
	// CapQueueOverride: Job.Queue may differ from the workflow's queue.
	CapQueueOverride = defineCap(17, "QueueOverride")
	// CapJobRetry: Job.Retry replaces the step policies for one run.
	CapJobRetry = defineCap(18, "JobRetry")
	// CapRetryUntil: RetryPolicy.Until is honoured.
	CapRetryUntil = defineCap(19, "RetryUntil")
	// CapRetryMaxErrors: RetryPolicy.MaxErrors is honoured.
	CapRetryMaxErrors = defineCap(20, "RetryMaxErrors")
	// CapDiscard: a step returning Discard(err) ends its run cancelled,
	// not failed. Without it Discard behaves as Permanent.
	CapDiscard = defineCap(21, "Discard")
	// CapRunQuery: the client implements RunQuerier.
	CapRunQuery = defineCap(22, "RunQuery")
	// CapRetryRun: the client implements RunRetrier, re-running a failed
	// run from the step that failed.
	CapRetryRun = defineCap(23, "RetryRun")
	// CapDeleteRun: the client implements RunDeleter.
	CapDeleteRun = defineCap(24, "DeleteRun")
	// CapCancelAll: the client implements BulkCanceller.
	CapCancelAll = defineCap(25, "CancelAll")
	// CapPauseWorkflow: the client implements Pauser for PauseTarget.Workflow
	// and PauseTarget.All.
	CapPauseWorkflow = defineCap(26, "PauseWorkflow")
	// CapPauseQueue: the client implements Pauser for PauseTarget.Queue.
	CapPauseQueue = defineCap(27, "PauseQueue")
	// CapSchedulePause: the client implements SchedulePauser.
	CapSchedulePause = defineCap(28, "SchedulePause")
	// CapScheduleWhen: Workflow.ScheduleWhen is asked before every tick.
	CapScheduleWhen = defineCap(29, "ScheduleWhen")
	// CapScheduleWindow: Schedule.Windows and Workflow.CronWindows are
	// honoured.
	CapScheduleWindow = defineCap(30, "ScheduleWindow")
	// CapTickEvents: the client implements TickNotifier.
	CapTickEvents = defineCap(31, "TickEvents")
	// CapScheduleList: the client implements ScheduleLister.
	CapScheduleList = defineCap(32, "ScheduleList")
	// CapDrain: the client implements Drainer.
	CapDrain = defineCap(33, "Drain")
	// CapRunStates: the driver reports StatusRetrying while a step waits
	// on its retry backoff and StatusDiscarded for a run a step ended with
	// Discard, and fills RunInfo.NextAttempt and NextAttemptAt (with
	// CapRunQuery). Without it those moments read StatusQueued or
	// StatusRunning, and a discarded run StatusCancelled (CapDiscard) or
	// StatusFailed; see EndStatus.
	CapRunStates = defineCap(34, "RunStates")
	// CapChain: Job.Chain is honoured (each job enqueued when the one
	// before it succeeds) and a running step may edit its run's chain with
	// PrependToChain and AppendToChain. The rest of a chain is data stored
	// with the run, so on a durable driver it survives a crash.
	CapChain = defineCap(35, "Chain")
	// CapWorkerScale: the client implements WorkerScaler, so a Balancer
	// can change its per-queue worker counts while it runs.
	CapWorkerScale = defineCap(36, "WorkerScale")
	// CapClosure: the client implements ClosureEnqueuer, running a Go
	// function as a job in this process. A closure cannot be stored or
	// sent, so only an in-process driver declares it.
	CapClosure = defineCap(37, "Closure")
	// CapLabelPrefix: RunQuery.LabelPrefixes is honoured by Runs and
	// CancelAll (a label value matched by prefix).
	CapLabelPrefix = defineCap(38, "LabelPrefix")
	// CapOverlapSkip: OverlapSkip on Workflow.CronOverlap and
	// Schedule.Overlap is honoured, decided from the previous run's
	// stored status (OverlapPolicy.Blocks).
	CapOverlapSkip = defineCap(39, "OverlapSkip")
	// CapScheduleTrigger: the client implements ScheduleTriggerer.
	CapScheduleTrigger = defineCap(40, "ScheduleTrigger")
	// CapScheduleHistory: ScheduleInfo.Last (last fire, last run and its
	// status) is filled, and stored with the schedule on a durable driver.
	CapScheduleHistory = defineCap(41, "ScheduleHistory")
	// CapEnqueueOnly: the driver has an enqueue-only mode (an option of
	// the driver's constructor) whose client enqueues, and answers
	// Status, Cancel and Signal, without Start and without running any
	// worker or scheduler, so web nodes enqueue and worker nodes run. On
	// an enqueue-only client Register is optional (a registered
	// workflow's checks apply; an unregistered one gets the job-level
	// checks only) and Start returns nil without starting anything. An
	// in-process driver cannot declare it: nothing else would ever run
	// what it enqueued.
	CapEnqueueOnly = defineCap(42, "EnqueueOnly")
	// CapErrorHistory: RunInfo.Errors holds the run's failed attempts
	// (AttemptError), oldest first, bounded by the driver's
	// error-history option (DefaultErrorHistory when unset).
	CapErrorHistory = defineCap(43, "ErrorHistory")
	// CapShutdownRequeue: a step attempt cut short by a clean stop (the
	// Start context cancelled) is put back without spending an attempt,
	// so the next worker runs it with the same attempt number and it
	// does not count toward the retry limit or appear in the error
	// history. Only a durable driver can declare it.
	CapShutdownRequeue = defineCap(44, "ShutdownRequeue")
	// CapForceRun: the client implements RunForcer.
	CapForceRun = defineCap(45, "ForceRun")
	// CapSnooze: a step returning Snooze(d) is put back for d without
	// spending an attempt (see Snooze).
	CapSnooze = defineCap(46, "Snooze")
	// CapHeartbeat: Step.HeartbeatTimeout is honoured (an attempt that
	// goes that long without Heartbeat fails with ErrHeartbeatTimeout),
	// and Heartbeat from a step reaches the driver (a durable driver
	// extends the attempt's lease).
	CapHeartbeat = defineCap(47, "Heartbeat")
	// CapTenant: Job.Tenant is stored with the run and reported on
	// StepInfo.Tenant, RunInfo.Tenant and LifecycleEvent.Tenant, and
	// RunQuery.Tenant filters by it.
	CapTenant = defineCap(48, "Tenant")
	// CapRunTimeout: Workflow.RunTimeout ends a run that runs too long,
	// Workflow.OnTimeout runs when a run fails because of a timeout, and
	// every timeout error matches ErrTimeout.
	CapRunTimeout = defineCap(49, "RunTimeout")
	// CapNodeIdentity: StepInfo.Node, AttemptError.Node and
	// RunInfo.AttemptedBy name the node that ran each attempt, and the
	// client implements NodeLister.
	CapNodeIdentity = defineCap(50, "NodeIdentity")
	// CapKeyFIFO: Concurrency.FIFO is honoured: the queued runs of one
	// concurrency key start in enqueue order (Job.Priority aside).
	CapKeyFIFO = defineCap(51, "KeyFIFO")
	// CapDueQuery: RunQuery.DueFrom and DueTo are honoured and
	// RunInfo.RunAt is filled.
	CapDueQuery = defineCap(52, "DueQuery")
	// CapResult: the client implements ResultReader and fills
	// RunInfo.Output for a succeeded run.
	CapResult = defineCap(53, "Result")
	// CapCronSeconds: Schedule.Seconds and Workflow.CronSeconds (a
	// six-field expression, seconds first) are honoured.
	CapCronSeconds = defineCap(54, "CronSeconds")
	// CapScheduleActive: Schedule.Active and Workflow.CronActive (a start
	// and end instant) are honoured.
	CapScheduleActive = defineCap(55, "ScheduleActive")
	// CapScheduleKeep: Schedule.KeepRuns and Workflow.CronKeepRuns (keep
	// only the newest N finished runs of a schedule) are honoured.
	CapScheduleKeep = defineCap(56, "ScheduleKeep")
	// CapTickTime: every run a schedule fires carries the tick it stands
	// for in LabelTickAt (TickTimeOf reads it), not only the wall time it
	// started.
	CapTickTime = defineCap(57, "TickTime")
	// CapQueueLimitGlobal: QueueConfig.Limit caps the in-flight step
	// attempts of a queue across every client sharing the store, not only
	// this client's workers (QueueConfig.Workers stays per client).
	CapQueueLimitGlobal = defineCap(58, "QueueLimitGlobal")
	// CapDiscardExhausted: RetryPolicy.DiscardOnExhaust ends a run whose
	// retries ran out StatusDiscarded instead of StatusFailed.
	CapDiscardExhausted = defineCap(59, "DiscardExhausted")
	// CapDrainReport: Drain reports the runs it cut off (DrainReport).
	CapDrainReport = defineCap(60, "DrainReport")
	// CapWorkflowVersion: every run records the Workflow.Version of the
	// client that enqueued it, and a worker admits, runs and advances
	// only runs whose version its definition runs (MinVersion to
	// Version), so during a rolling deploy a run started by new code
	// never runs on old code (LO-11).
	CapWorkflowVersion = defineCap(61, "WorkflowVersion")
	// CapSlotCost: Step.Slots is honoured: one attempt of the step holds
	// that many of the client's worker slots (WithWorkers) while it runs,
	// so a heavy step leaves room for fewer others.
	CapSlotCost = defineCap(62, "SlotCost")
	// CapPayloadSearch: RunQuery.Payload is honoured: runs are matched on
	// fields of their JSON payload (RS-08).
	CapPayloadSearch = defineCap(63, "PayloadSearch")
)

// Requires computes the capabilities a driver needs to run wf as written.
// Retry needs come from each Run step's effective policy, so a workflow
// default every step overrides asks for nothing. Durability is not
// inferred: a workflow reads the same on any driver, and the app chooses a
// durable driver when it needs one.
func Requires(wf Workflow) Caps {
	var c Caps
	if len(wf.Cron) > 0 {
		c = c.With(CapCron, misfireCaps(wf.Misfire), overlapCaps(wf.CronOverlap))
		if !IsUTC(wf.CronLocation) {
			c = c.With(CapCronZone)
		}
		if wf.CronSeconds {
			c = c.With(CapCronSeconds)
		}
		if !wf.CronActive.IsZero() {
			c = c.With(CapScheduleActive)
		}
		if wf.CronKeepRuns > 0 {
			c = c.With(CapScheduleKeep)
		}
	}
	if wf.Concurrency != nil {
		c = c.With(CapConcurrencyKey)
		if wf.Concurrency.FIFO {
			c = c.With(CapKeyFIFO)
		}
	}
	if wf.RunTimeout > 0 || wf.OnTimeout != nil {
		c = c.With(CapRunTimeout)
	}
	if wf.Version != 0 || wf.MinVersion != 0 {
		c = c.With(CapWorkflowVersion)
	}
	if len(wf.RateLimits) > 0 {
		c = c.With(CapRateLimit)
	}
	if wf.Idempotency == IdempotencyRequired {
		c = c.With(CapIdempotencyKey)
	}
	if wf.Queue != "" && wf.Queue != DefaultQueue {
		c = c.With(CapQueue)
	}
	if wf.ScheduleWhen != nil {
		c = c.With(CapScheduleWhen)
	}
	if !wf.CronWindows.IsZero() {
		c = c.With(CapScheduleWindow)
	}
	for _, s := range wf.Steps {
		switch s.Kind() {
		case StepRun:
			c = c.With(retryCaps(EffectiveRetries(s, wf)))
			if s.HeartbeatTimeout > 0 {
				c = c.With(CapHeartbeat)
			}
			if s.Slots > 1 {
				c = c.With(CapSlotCost)
			}
		case StepWait:
			c = c.With(CapWaitForEvent)
		case StepSleep:
			c = c.With(CapDurableSleep)
		}
	}
	return c
}

// retryCaps is what a retry policy needs beyond plain exponential backoff.
func retryCaps(p RetryPolicy) Caps {
	var c Caps
	if p.Backoff.Jitter > 0 {
		c = c.With(CapRetryJitter)
	}
	if p.DelayFor != nil {
		c = c.With(CapRetryDelay)
	}
	if p.Until > 0 {
		c = c.With(CapRetryUntil)
	}
	if p.MaxErrors > 0 {
		c = c.With(CapRetryMaxErrors)
	}
	if p.DiscardOnExhaust {
		c = c.With(CapDiscardExhausted, CapRunStates)
	}
	return c
}

// misfireCaps is the capability a misfire policy needs; the driver
// default needs none.
func misfireCaps(m Misfire) Caps {
	switch m.Policy {
	case MisfireSkip:
		return CapMisfireSkip
	case MisfireFireOnce:
		return CapMisfireFireOnce
	case MisfireFireAll:
		return CapMisfireFireAll
	}
	return Caps{}
}

// RequiresSchedule computes the capabilities a driver needs to honour s as
// written: CapCron, CapCronZone for a zone other than UTC, the capability
// of its misfire policy, CapScheduleWindow for windows, CapOverlapSkip
// for OverlapSkip, CapCronSeconds for Seconds, CapScheduleActive for an
// Active period, CapScheduleKeep for KeepRuns and CapTenant for Tenant.
func RequiresSchedule(s Schedule) Caps {
	c := CapsOf(CapCron, misfireCaps(s.Misfire), overlapCaps(s.Overlap))
	if !IsUTC(s.Location) {
		c = c.With(CapCronZone)
	}
	if !s.Windows.IsZero() {
		c = c.With(CapScheduleWindow)
	}
	if s.Seconds {
		c = c.With(CapCronSeconds)
	}
	if !s.Active.IsZero() {
		c = c.With(CapScheduleActive)
	}
	if s.KeepRuns > 0 {
		c = c.With(CapScheduleKeep)
	}
	if s.Tenant != "" {
		c = c.With(CapTenant)
	}
	return c
}

// CheckScheduleCaps returns an UnsupportedError when have lacks what s
// needs, and nil otherwise. Its Field names the cause ("Schedule",
// "Schedule.Location", "Schedule.Windows", "Schedule.Overlap",
// "Schedule.Seconds", "Schedule.Active", "Schedule.KeepRuns" or
// "Schedule.Misfire") and its Reason the workflow.
// Drivers call it from Schedule, after ValidateSchedule.
func CheckScheduleCaps(have Caps, s Schedule) error {
	missing := have.Missing(RequiresSchedule(s))
	if missing.IsZero() {
		return nil
	}
	field := "Schedule"
	switch {
	case missing.Has(CapCron):
	case missing.HasAny(CapCronZone):
		field = "Schedule.Location"
	case missing.HasAny(CapScheduleWindow):
		field = "Schedule.Windows"
	case missing.HasAny(CapOverlapSkip):
		field = "Schedule.Overlap"
	case missing.HasAny(CapCronSeconds):
		field = "Schedule.Seconds"
	case missing.HasAny(CapScheduleActive):
		field = "Schedule.Active"
	case missing.HasAny(CapScheduleKeep):
		field = "Schedule.KeepRuns"
	case missing.HasAny(CapTenant):
		field = "Schedule.Tenant"
	default:
		field = "Schedule.Misfire"
	}
	return &UnsupportedError{Field: field, Missing: missing, Reason: "schedule for workflow " + s.Workflow}
}

// RequiresJob computes the capabilities a driver needs to honour job as
// written: a delayed start, an idempotency key, a queue, a tenant, a
// per-dispatch retry policy (with what that policy needs) and a chain (CapChain plus
// what every chained job needs, since the same driver enqueues them). A driver checks it in
// Enqueue and refuses with an UnsupportedError rather than ignoring the
// field.
//
// A key counts even when the workflow's policy is IdempotencyOff. The
// policy is the workflow's to choose, but a key on a job is the caller's
// request, and a driver without the capability must say so instead of
// silently ignoring it: a key is never dropped without an error. It also
// keeps the check a pure function of the job, so a driver can refuse
// before looking the workflow up.
func RequiresJob(job Job) Caps {
	var c Caps
	if !job.RunAt.IsZero() {
		c = c.With(CapDelayedStart)
	}
	if job.IdempotencyKey != "" {
		c = c.With(CapIdempotencyKey)
	}
	if job.Queue != "" {
		c = c.With(CapQueue)
	}
	if job.Retry != nil {
		c = c.With(CapJobRetry, retryCaps(*job.Retry))
	}
	if job.Tenant != "" {
		c = c.With(CapTenant)
	}
	if len(job.Chain) > 0 {
		c = c.With(CapChain)
		for _, next := range job.Chain {
			c = c.With(RequiresJob(next))
		}
	}
	return c
}

// CheckJob returns an UnsupportedError when have lacks what job needs
// (RequiresJob), naming the first field responsible, and nil otherwise.
// Drivers call it from Enqueue after ValidateJob.
func CheckJob(have Caps, job Job) error {
	missing := have.Missing(RequiresJob(job))
	if missing.IsZero() {
		return nil
	}
	field := "Job"
	switch {
	case missing.HasAny(CapDelayedStart):
		field = "Job.RunAt"
	case missing.HasAny(CapIdempotencyKey):
		field = "Job.IdempotencyKey"
	case missing.HasAny(CapQueue):
		field = "Job.Queue"
	case missing.HasAny(CapTenant):
		field = "Job.Tenant"
	case missing.HasAny(CapChain):
		field = "Job.Chain"
	case missing.HasAny(CapsOf(CapJobRetry, CapRetryJitter, CapRetryUntil, CapRetryMaxErrors, CapDiscardExhausted)):
		field = "Job.Retry"
	}
	return &UnsupportedError{Field: field, Missing: missing, Reason: "job for workflow " + job.Workflow}
}

// CheckJobQueue refuses a Job.Queue that differs from wf's effective queue
// when have lacks CapQueueOverride: an engine that binds a workflow to the
// workers of its queue cannot send one run elsewhere. Drivers call it from
// Enqueue once they have looked the workflow up.
func CheckJobQueue(have Caps, wf Workflow, job Job) error {
	if job.Queue == "" || have.Has(CapQueueOverride) || job.Queue == EffectiveQueue(wf, Job{}) {
		return nil
	}
	return &UnsupportedError{Field: "Job.Queue", Missing: CapQueueOverride,
		Reason: "workflow " + wf.Name + " runs on queue " + EffectiveQueue(wf, Job{}) + ", not " + job.Queue}
}

// CheckCaps returns an UnsupportedError naming wf when have lacks what wf
// needs, and nil otherwise. Drivers call it from Start.
func CheckCaps(have Caps, wf Workflow) error {
	if missing := have.Missing(Requires(wf)); !missing.IsZero() {
		return &UnsupportedError{Field: "Workflow", Missing: missing, Reason: "workflow " + wf.Name}
	}
	return nil
}
