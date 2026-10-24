package workflow

import "strings"

// Caps is a set of capabilities a driver really has. Drivers declare it
// through Client.Capabilities; Start compares it with Requires for every
// registered workflow and refuses what cannot be honoured, so a missing
// feature is an error at start-up rather than a silent no-op at run time.
//
// Caps is 64 bits and 58 are used, so a new bit is kept for a spec field
// that Requires, RequiresJob or RequiresSchedule must check, or for data a
// driver would otherwise leave silently empty. An optional interface whose
// call can answer for itself (RunRescheduler, StepCanceller,
// HealthChecker, LifecycleNotifier, ReportProgress) gets no bit: it
// returns an UnsupportedError when the driver cannot do it.
type Caps uint64

// The capabilities.
const (
	// CapDurable: runs, waits and sleeps survive a process restart.
	CapDurable Caps = 1 << iota
	// CapDelayedStart: Job.RunAt is honoured.
	CapDelayedStart
	// CapWaitForEvent: wait steps suspend without holding a worker until
	// a Signal or their deadline. Whether the wait survives a restart is
	// CapDurable's claim.
	CapWaitForEvent
	// CapDurableSleep: sleep steps suspend without holding a worker.
	// Whether the sleep survives a restart is CapDurable's claim.
	CapDurableSleep
	// CapConcurrencyKey: Workflow.Concurrency is enforced per key.
	CapConcurrencyKey
	// CapRateLimit: Workflow.RateLimits are enforced.
	CapRateLimit
	// CapIdempotencyKey: Job.IdempotencyKey dedups enqueues.
	CapIdempotencyKey
	// CapCron: Workflow.Cron and Client.Schedule fire.
	CapCron
	// CapCancel: Client.Cancel stops a run.
	CapCancel
	// CapRunFinished: the driver implements RunNotifier with the
	// guaranteed delivery described there (every terminal run, at least
	// once). A driver that implements RunNotifier without this capability
	// delivers best-effort only.
	CapRunFinished
	// CapRetryJitter: Backoff.Jitter spreads retry delays.
	CapRetryJitter
	// CapRetryDelay: RetryPolicy.DelayFor sets a retry's delay from the
	// error (RetryAfter).
	CapRetryDelay
	// CapMisfireSkip: Misfire{Policy: MisfireSkip} is honoured.
	CapMisfireSkip
	// CapMisfireFireOnce: Misfire{Policy: MisfireFireOnce}, with Within,
	// is honoured.
	CapMisfireFireOnce
	// CapMisfireFireAll: Misfire{Policy: MisfireFireAll}, with Within and
	// Max, is honoured.
	CapMisfireFireAll
	// CapCronZone: Workflow.CronLocation and Schedule.Location may be a
	// zone other than UTC, evaluated in that zone across DST.
	CapCronZone
	// CapQueue: Workflow.Queue and Job.Queue route runs to named queues,
	// and the driver's per-queue worker limits (QueueConfig) are honoured.
	CapQueue
	// CapQueueOverride: Job.Queue may differ from the workflow's queue.
	CapQueueOverride
	// CapJobRetry: Job.Retry replaces the step policies for one run.
	CapJobRetry
	// CapRetryUntil: RetryPolicy.Until is honoured.
	CapRetryUntil
	// CapRetryMaxErrors: RetryPolicy.MaxErrors is honoured.
	CapRetryMaxErrors
	// CapDiscard: a step returning Discard(err) ends its run cancelled,
	// not failed. Without it Discard behaves as Permanent.
	CapDiscard
	// CapRunQuery: the client implements RunQuerier.
	CapRunQuery
	// CapRetryRun: the client implements RunRetrier, re-running a failed
	// run from the step that failed.
	CapRetryRun
	// CapDeleteRun: the client implements RunDeleter.
	CapDeleteRun
	// CapCancelAll: the client implements BulkCanceller.
	CapCancelAll
	// CapPauseWorkflow: the client implements Pauser for PauseTarget.Workflow
	// and PauseTarget.All.
	CapPauseWorkflow
	// CapPauseQueue: the client implements Pauser for PauseTarget.Queue.
	CapPauseQueue
	// CapSchedulePause: the client implements SchedulePauser.
	CapSchedulePause
	// CapScheduleWhen: Workflow.ScheduleWhen is asked before every tick.
	CapScheduleWhen
	// CapScheduleWindow: Schedule.Windows and Workflow.CronWindows are
	// honoured.
	CapScheduleWindow
	// CapTickEvents: the client implements TickNotifier.
	CapTickEvents
	// CapScheduleList: the client implements ScheduleLister.
	CapScheduleList
	// CapDrain: the client implements Drainer.
	CapDrain
	// CapRunStates: the driver reports StatusRetrying while a step waits
	// on its retry backoff and StatusDiscarded for a run a step ended with
	// Discard, and fills RunInfo.NextAttempt and NextAttemptAt (with
	// CapRunQuery). Without it those moments read StatusQueued or
	// StatusRunning, and a discarded run StatusCancelled (CapDiscard) or
	// StatusFailed; see EndStatus.
	CapRunStates
	// CapChain: Job.Chain is honoured (each job enqueued when the one
	// before it succeeds) and a running step may edit its run's chain with
	// PrependToChain and AppendToChain. The rest of a chain is data stored
	// with the run, so on a durable driver it survives a crash.
	CapChain
	// CapWorkerScale: the client implements WorkerScaler, so a Balancer
	// can change its per-queue worker counts while it runs.
	CapWorkerScale
	// CapClosure: the client implements ClosureEnqueuer, running a Go
	// function as a job in this process. A closure cannot be stored or
	// sent, so only an in-process driver declares it.
	CapClosure
	// CapLabelPrefix: RunQuery.LabelPrefixes is honoured by Runs and
	// CancelAll (a label value matched by prefix).
	CapLabelPrefix
	// CapOverlapSkip: OverlapSkip on Workflow.CronOverlap and
	// Schedule.Overlap is honoured, decided from the previous run's
	// stored status (OverlapPolicy.Blocks).
	CapOverlapSkip
	// CapScheduleTrigger: the client implements ScheduleTriggerer.
	CapScheduleTrigger
	// CapScheduleHistory: ScheduleInfo.Last (last fire, last run and its
	// status) is filled, and stored with the schedule on a durable driver.
	CapScheduleHistory
	// CapEnqueueOnly: the driver has an enqueue-only mode (an option of
	// the driver's constructor) whose client enqueues, and answers
	// Status, Cancel and Signal, without Start and without running any
	// worker or scheduler, so web nodes enqueue and worker nodes run. On
	// an enqueue-only client Register is optional (a registered
	// workflow's checks apply; an unregistered one gets the job-level
	// checks only) and Start returns nil without starting anything. An
	// in-process driver cannot declare it: nothing else would ever run
	// what it enqueued.
	CapEnqueueOnly
	// CapErrorHistory: RunInfo.Errors holds the run's failed attempts
	// (AttemptError), oldest first, bounded by the driver's
	// error-history option (DefaultErrorHistory when unset).
	CapErrorHistory
	// CapShutdownRequeue: a step attempt cut short by a clean stop (the
	// Start context cancelled) is put back without spending an attempt,
	// so the next worker runs it with the same attempt number and it
	// does not count toward the retry limit or appear in the error
	// history. Only a durable driver can declare it.
	CapShutdownRequeue
	// CapForceRun: the client implements RunForcer.
	CapForceRun
	// CapSnooze: a step returning Snooze(d) is put back for d without
	// spending an attempt (see Snooze).
	CapSnooze
	// CapHeartbeat: Step.HeartbeatTimeout is honoured (an attempt that
	// goes that long without Heartbeat fails with ErrHeartbeatTimeout),
	// and Heartbeat from a step reaches the driver (a durable driver
	// extends the attempt's lease).
	CapHeartbeat
	// CapTenant: Job.Tenant is stored with the run and reported on
	// StepInfo.Tenant, RunInfo.Tenant and LifecycleEvent.Tenant, and
	// RunQuery.Tenant filters by it.
	CapTenant
	// CapRunTimeout: Workflow.RunTimeout ends a run that runs too long,
	// Workflow.OnTimeout runs when a run fails because of a timeout, and
	// every timeout error matches ErrTimeout.
	CapRunTimeout
	// CapNodeIdentity: StepInfo.Node, AttemptError.Node and
	// RunInfo.AttemptedBy name the node that ran each attempt, and the
	// client implements NodeLister.
	CapNodeIdentity
	// CapKeyFIFO: Concurrency.FIFO is honoured: the queued runs of one
	// concurrency key start in enqueue order (Job.Priority aside).
	CapKeyFIFO
	// CapDueQuery: RunQuery.DueFrom and DueTo are honoured and
	// RunInfo.RunAt is filled.
	CapDueQuery
	// CapResult: the client implements ResultReader and fills
	// RunInfo.Output for a succeeded run.
	CapResult
	// CapCronSeconds: Schedule.Seconds and Workflow.CronSeconds (a
	// six-field expression, seconds first) are honoured.
	CapCronSeconds
	// CapScheduleActive: Schedule.Active and Workflow.CronActive (a start
	// and end instant) are honoured.
	CapScheduleActive
	// CapScheduleKeep: Schedule.KeepRuns and Workflow.CronKeepRuns (keep
	// only the newest N finished runs of a schedule) are honoured.
	CapScheduleKeep
	// CapTickTime: every run a schedule fires carries the tick it stands
	// for in LabelTickAt (TickTimeOf reads it), not only the wall time it
	// started.
	CapTickTime
	// CapQueueLimitGlobal: QueueConfig.Limit caps the in-flight step
	// attempts of a queue across every client sharing the store, not only
	// this client's workers (QueueConfig.Workers stays per client).
	CapQueueLimitGlobal
	// CapDiscardExhausted: RetryPolicy.DiscardOnExhaust ends a run whose
	// retries ran out StatusDiscarded instead of StatusFailed.
	CapDiscardExhausted
	// CapDrainReport: Drain reports the runs it cut off (DrainReport).
	CapDrainReport

	capEnd // one past the last capability; keep last
)

var capNames = []string{
	"Durable", "DelayedStart", "WaitForEvent", "DurableSleep",
	"ConcurrencyKey", "RateLimit", "IdempotencyKey", "Cron", "Cancel",
	"RunFinished", "RetryJitter", "RetryDelay", "MisfireSkip",
	"MisfireFireOnce", "MisfireFireAll", "CronZone",
	"Queue", "QueueOverride", "JobRetry", "RetryUntil", "RetryMaxErrors",
	"Discard", "RunQuery", "RetryRun", "DeleteRun", "CancelAll",
	"PauseWorkflow", "PauseQueue", "SchedulePause", "ScheduleWhen",
	"ScheduleWindow", "TickEvents", "ScheduleList", "Drain",
	"RunStates", "Chain", "WorkerScale", "Closure", "LabelPrefix",
	"OverlapSkip", "ScheduleTrigger", "ScheduleHistory", "EnqueueOnly",
	"ErrorHistory", "ShutdownRequeue", "ForceRun",
	"Snooze", "Heartbeat", "Tenant", "RunTimeout", "NodeIdentity", "KeyFIFO",
	"DueQuery", "Result", "CronSeconds", "ScheduleActive", "ScheduleKeep",
	"TickTime", "QueueLimitGlobal", "DiscardExhausted", "DrainReport",
}

// Has reports whether c holds every capability in want.
func (c Caps) Has(want Caps) bool { return c&want == want }

// Missing returns the capabilities in required that c lacks; zero means
// c covers them all.
func (c Caps) Missing(required Caps) Caps { return required &^ c }

// String lists the capabilities joined by "|", such as
// "Durable|Cron"; the empty set is "none".
func (c Caps) String() string {
	if c == 0 {
		return "none"
	}
	var names []string
	for i, bit := 0, Caps(1); bit < capEnd; i, bit = i+1, bit<<1 {
		if c&bit != 0 {
			names = append(names, capNames[i])
		}
	}
	if rest := c &^ (capEnd - 1); rest != 0 {
		names = append(names, "unknown")
	}
	return strings.Join(names, "|")
}

// Requires computes the capabilities a driver needs to run wf as written.
// Retry needs come from each Run step's effective policy, so a workflow
// default every step overrides asks for nothing. Durability is not
// inferred: a workflow reads the same on any driver, and the app chooses a
// durable driver when it needs one.
func Requires(wf Workflow) Caps {
	var c Caps
	if len(wf.Cron) > 0 {
		c |= CapCron | misfireCaps(wf.Misfire) | overlapCaps(wf.CronOverlap)
		if !IsUTC(wf.CronLocation) {
			c |= CapCronZone
		}
		if wf.CronSeconds {
			c |= CapCronSeconds
		}
		if !wf.CronActive.IsZero() {
			c |= CapScheduleActive
		}
		if wf.CronKeepRuns > 0 {
			c |= CapScheduleKeep
		}
	}
	if wf.Concurrency != nil {
		c |= CapConcurrencyKey
		if wf.Concurrency.FIFO {
			c |= CapKeyFIFO
		}
	}
	if wf.RunTimeout > 0 || wf.OnTimeout != nil {
		c |= CapRunTimeout
	}
	if len(wf.RateLimits) > 0 {
		c |= CapRateLimit
	}
	if wf.Idempotency == IdempotencyRequired {
		c |= CapIdempotencyKey
	}
	if wf.Queue != "" && wf.Queue != DefaultQueue {
		c |= CapQueue
	}
	if wf.ScheduleWhen != nil {
		c |= CapScheduleWhen
	}
	if !wf.CronWindows.IsZero() {
		c |= CapScheduleWindow
	}
	for _, s := range wf.Steps {
		switch s.Kind() {
		case StepRun:
			c |= retryCaps(EffectiveRetries(s, wf))
			if s.HeartbeatTimeout > 0 {
				c |= CapHeartbeat
			}
		case StepWait:
			c |= CapWaitForEvent
		case StepSleep:
			c |= CapDurableSleep
		}
	}
	return c
}

// retryCaps is what a retry policy needs beyond plain exponential backoff.
func retryCaps(p RetryPolicy) Caps {
	var c Caps
	if p.Backoff.Jitter > 0 {
		c |= CapRetryJitter
	}
	if p.DelayFor != nil {
		c |= CapRetryDelay
	}
	if p.Until > 0 {
		c |= CapRetryUntil
	}
	if p.MaxErrors > 0 {
		c |= CapRetryMaxErrors
	}
	if p.DiscardOnExhaust {
		c |= CapDiscardExhausted | CapRunStates
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
	return 0
}

// RequiresSchedule computes the capabilities a driver needs to honour s as
// written: CapCron, CapCronZone for a zone other than UTC, the capability
// of its misfire policy, CapScheduleWindow for windows, CapOverlapSkip
// for OverlapSkip, CapCronSeconds for Seconds, CapScheduleActive for an
// Active period, CapScheduleKeep for KeepRuns and CapTenant for Tenant.
func RequiresSchedule(s Schedule) Caps {
	c := CapCron | misfireCaps(s.Misfire) | overlapCaps(s.Overlap)
	if !IsUTC(s.Location) {
		c |= CapCronZone
	}
	if !s.Windows.IsZero() {
		c |= CapScheduleWindow
	}
	if s.Seconds {
		c |= CapCronSeconds
	}
	if !s.Active.IsZero() {
		c |= CapScheduleActive
	}
	if s.KeepRuns > 0 {
		c |= CapScheduleKeep
	}
	if s.Tenant != "" {
		c |= CapTenant
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
	if missing == 0 {
		return nil
	}
	field := "Schedule"
	switch {
	case missing.Has(CapCron):
	case missing&CapCronZone != 0:
		field = "Schedule.Location"
	case missing&CapScheduleWindow != 0:
		field = "Schedule.Windows"
	case missing&CapOverlapSkip != 0:
		field = "Schedule.Overlap"
	case missing&CapCronSeconds != 0:
		field = "Schedule.Seconds"
	case missing&CapScheduleActive != 0:
		field = "Schedule.Active"
	case missing&CapScheduleKeep != 0:
		field = "Schedule.KeepRuns"
	case missing&CapTenant != 0:
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
		c |= CapDelayedStart
	}
	if job.IdempotencyKey != "" {
		c |= CapIdempotencyKey
	}
	if job.Queue != "" {
		c |= CapQueue
	}
	if job.Retry != nil {
		c |= CapJobRetry | retryCaps(*job.Retry)
	}
	if job.Tenant != "" {
		c |= CapTenant
	}
	if len(job.Chain) > 0 {
		c |= CapChain
		for _, next := range job.Chain {
			c |= RequiresJob(next)
		}
	}
	return c
}

// CheckJob returns an UnsupportedError when have lacks what job needs
// (RequiresJob), naming the first field responsible, and nil otherwise.
// Drivers call it from Enqueue after ValidateJob.
func CheckJob(have Caps, job Job) error {
	missing := have.Missing(RequiresJob(job))
	if missing == 0 {
		return nil
	}
	field := "Job"
	switch {
	case missing&CapDelayedStart != 0:
		field = "Job.RunAt"
	case missing&CapIdempotencyKey != 0:
		field = "Job.IdempotencyKey"
	case missing&CapQueue != 0:
		field = "Job.Queue"
	case missing&CapTenant != 0:
		field = "Job.Tenant"
	case missing&CapChain != 0:
		field = "Job.Chain"
	case missing&(CapJobRetry|CapRetryJitter|CapRetryUntil|CapRetryMaxErrors|CapDiscardExhausted) != 0:
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
	if missing := have.Missing(Requires(wf)); missing != 0 {
		return &UnsupportedError{Field: "Workflow", Missing: missing, Reason: "workflow " + wf.Name}
	}
	return nil
}
