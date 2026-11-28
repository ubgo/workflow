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
	//ds:def id=capdurable-yhjnwexa owner=@khanakia stability=stable
	CapDurable = defineCap(0, "Durable")
	// CapDelayedStart: Job.RunAt is honoured.
	//ds:def id=capdelayedstart-qbxsx6hb owner=@khanakia stability=stable
	CapDelayedStart = defineCap(1, "DelayedStart")
	// CapWaitForEvent: wait steps suspend without holding a worker until
	// a Signal or their deadline. Whether the wait survives a restart is
	// CapDurable's claim.
	//ds:def id=capwaitforevent-2km3dkzy owner=@khanakia stability=stable
	CapWaitForEvent = defineCap(2, "WaitForEvent")
	// CapDurableSleep: sleep steps suspend without holding a worker.
	// Whether the sleep survives a restart is CapDurable's claim.
	//ds:def id=capdurablesleep-k5tkkje8 owner=@khanakia stability=stable
	CapDurableSleep = defineCap(3, "DurableSleep")
	// CapConcurrencyKey: Workflow.Concurrency is enforced per key.
	//ds:def id=capconcurrencykey-5amtud9g owner=@khanakia stability=stable
	CapConcurrencyKey = defineCap(4, "ConcurrencyKey")
	// CapRateLimit: Workflow.RateLimits are enforced.
	//ds:def id=capratelimit-7jktf585 owner=@khanakia stability=stable
	CapRateLimit = defineCap(5, "RateLimit")
	// CapIdempotencyKey: Job.IdempotencyKey dedups enqueues.
	//ds:def id=capidempotencykey-5wcqqwy8 owner=@khanakia stability=stable
	CapIdempotencyKey = defineCap(6, "IdempotencyKey")
	// CapCron: Workflow.Cron and Client.Schedule fire.
	//ds:def id=capcron-twc2ytyn owner=@khanakia stability=stable
	CapCron = defineCap(7, "Cron")
	// CapCancel: Client.Cancel stops a run.
	//ds:def id=capcancel-73j3f5s3 owner=@khanakia stability=stable
	CapCancel = defineCap(8, "Cancel")
	// CapRunFinished: the driver implements RunNotifier with the
	// guaranteed delivery described there (every terminal run, at least
	// once). A driver that implements RunNotifier without this capability
	// delivers best-effort only.
	//ds:def id=caprunfinished-7nrsun98 owner=@khanakia stability=stable
	CapRunFinished = defineCap(9, "RunFinished")
	// CapRetryJitter: Backoff.Jitter spreads retry delays.
	//ds:def id=capretryjitter-4dbyewuw owner=@khanakia stability=stable
	CapRetryJitter = defineCap(10, "RetryJitter")
	// CapRetryDelay: RetryPolicy.DelayFor sets a retry's delay from the
	// error (RetryAfter).
	//ds:def id=capretrydelay-pp5car73 owner=@khanakia stability=stable
	CapRetryDelay = defineCap(11, "RetryDelay")
	// CapMisfireSkip: Misfire{Policy: MisfireSkip} is honoured.
	//ds:def id=capmisfireskip-g62y4cwh owner=@khanakia stability=stable
	CapMisfireSkip = defineCap(12, "MisfireSkip")
	// CapMisfireFireOnce: Misfire{Policy: MisfireFireOnce}, with Within,
	// is honoured.
	//ds:def id=capmisfirefireonce-p94vdm89 owner=@khanakia stability=stable
	CapMisfireFireOnce = defineCap(13, "MisfireFireOnce")
	// CapMisfireFireAll: Misfire{Policy: MisfireFireAll}, with Within and
	// Max, is honoured.
	//ds:def id=capmisfirefireall-9zf4spja owner=@khanakia stability=stable
	CapMisfireFireAll = defineCap(14, "MisfireFireAll")
	// CapCronZone: Workflow.CronLocation and Schedule.Location may be a
	// zone other than UTC, evaluated in that zone across DST.
	//ds:def id=capcronzone-4tkwuz9q owner=@khanakia stability=stable
	CapCronZone = defineCap(15, "CronZone")
	// CapQueue: Workflow.Queue and Job.Queue route runs to named queues,
	// and the driver's per-queue worker limits (QueueConfig) are honoured.
	//ds:def id=capqueue-47q2gcgz owner=@khanakia stability=stable
	CapQueue = defineCap(16, "Queue")
	// CapQueueOverride: Job.Queue may differ from the workflow's queue.
	//ds:def id=capqueueoverride-7tshsxqq owner=@khanakia stability=stable
	CapQueueOverride = defineCap(17, "QueueOverride")
	// CapJobRetry: Job.Retry replaces the step policies for one run.
	//ds:def id=capjobretry-p3zcdugs owner=@khanakia stability=stable
	CapJobRetry = defineCap(18, "JobRetry")
	// CapRetryUntil: RetryPolicy.Until is honoured.
	//ds:def id=capretryuntil-btnd8y5w owner=@khanakia stability=stable
	CapRetryUntil = defineCap(19, "RetryUntil")
	// CapRetryMaxErrors: RetryPolicy.MaxErrors is honoured.
	//ds:def id=capretrymaxerrors-pcdbetra owner=@khanakia stability=stable
	CapRetryMaxErrors = defineCap(20, "RetryMaxErrors")
	// CapDiscard: a step returning Discard(err) ends its run cancelled,
	// not failed. Without it Discard behaves as Permanent.
	//ds:def id=capdiscard-9fex6gfu owner=@khanakia stability=stable
	CapDiscard = defineCap(21, "Discard")
	// CapRunQuery: the client implements RunQuerier.
	//ds:def id=caprunquery-7pyv7hv3 owner=@khanakia stability=stable
	CapRunQuery = defineCap(22, "RunQuery")
	// CapRetryRun: the client implements RunRetrier, re-running a failed
	// run from the step that failed.
	//ds:def id=capretryrun-qf2ekfmf owner=@khanakia stability=stable
	CapRetryRun = defineCap(23, "RetryRun")
	// CapDeleteRun: the client implements RunDeleter.
	//ds:def id=capdeleterun-t4p72gkj owner=@khanakia stability=stable
	CapDeleteRun = defineCap(24, "DeleteRun")
	// CapCancelAll: the client implements BulkCanceller.
	//ds:def id=capcancelall-7enkwcsq owner=@khanakia stability=stable
	CapCancelAll = defineCap(25, "CancelAll")
	// CapPauseWorkflow: the client implements Pauser for PauseTarget.Workflow
	// and PauseTarget.All.
	//ds:def id=cappauseworkflow-xe2cngzv owner=@khanakia stability=stable
	CapPauseWorkflow = defineCap(26, "PauseWorkflow")
	// CapPauseQueue: the client implements Pauser for PauseTarget.Queue.
	//ds:def id=cappausequeue-5b6wusw5 owner=@khanakia stability=stable
	CapPauseQueue = defineCap(27, "PauseQueue")
	// CapSchedulePause: the client implements SchedulePauser.
	//ds:def id=capschedulepause-h24u69mw owner=@khanakia stability=stable
	CapSchedulePause = defineCap(28, "SchedulePause")
	// CapScheduleWhen: Workflow.ScheduleWhen is asked before every tick.
	//ds:def id=capschedulewhen-z3ehkydm owner=@khanakia stability=stable
	CapScheduleWhen = defineCap(29, "ScheduleWhen")
	// CapScheduleWindow: Schedule.Windows and Workflow.CronWindows are
	// honoured.
	//ds:def id=capschedulewindow-rn8htvhs owner=@khanakia stability=stable
	CapScheduleWindow = defineCap(30, "ScheduleWindow")
	// CapTickEvents: the client implements TickNotifier.
	//ds:def id=captickevents-gkgp2seh owner=@khanakia stability=stable
	CapTickEvents = defineCap(31, "TickEvents")
	// CapScheduleList: the client implements ScheduleLister.
	//ds:def id=capschedulelist-3hbmmd6n owner=@khanakia stability=stable
	CapScheduleList = defineCap(32, "ScheduleList")
	// CapDrain: the client implements Drainer.
	//ds:def id=capdrain-durs9rac owner=@khanakia stability=stable
	CapDrain = defineCap(33, "Drain")
	// CapRunStates: the driver reports StatusRetrying while a step waits
	// on its retry backoff and StatusDiscarded for a run a step ended with
	// Discard, and fills RunInfo.NextAttempt and NextAttemptAt (with
	// CapRunQuery). Without it those moments read StatusQueued or
	// StatusRunning, and a discarded run StatusCancelled (CapDiscard) or
	// StatusFailed; see EndStatus.
	//ds:def id=caprunstates-qugshrv4 owner=@khanakia stability=stable
	CapRunStates = defineCap(34, "RunStates")
	// CapChain: Job.Chain is honoured (each job enqueued when the one
	// before it succeeds) and a running step may edit its run's chain with
	// PrependToChain and AppendToChain. The rest of a chain is data stored
	// with the run, so on a durable driver it survives a crash.
	//ds:def id=capchain-2p7k762t owner=@khanakia stability=stable
	CapChain = defineCap(35, "Chain")
	// CapWorkerScale: the client implements WorkerScaler, so a Balancer
	// can change its per-queue worker counts while it runs.
	//ds:def id=capworkerscale-7kxc7pt2 owner=@khanakia stability=stable
	CapWorkerScale = defineCap(36, "WorkerScale")
	// CapClosure: the client implements ClosureEnqueuer, running a Go
	// function as a job in this process. A closure cannot be stored or
	// sent, so only an in-process driver declares it.
	//ds:def id=capclosure-tr6tdrfy owner=@khanakia stability=stable
	CapClosure = defineCap(37, "Closure")
	// CapLabelPrefix: RunQuery.LabelPrefixes is honoured by Runs and
	// CancelAll (a label value matched by prefix).
	//ds:def id=caplabelprefix-3paef4rr owner=@khanakia stability=stable
	CapLabelPrefix = defineCap(38, "LabelPrefix")
	// CapOverlapSkip: OverlapSkip on Workflow.CronOverlap and
	// Schedule.Overlap is honoured, decided from the previous run's
	// stored status (OverlapPolicy.Blocks).
	//ds:def id=capoverlapskip-m32v6e7r owner=@khanakia stability=stable
	CapOverlapSkip = defineCap(39, "OverlapSkip")
	// CapScheduleTrigger: the client implements ScheduleTriggerer.
	//ds:def id=capscheduletrigger-v7y76gam owner=@khanakia stability=stable
	CapScheduleTrigger = defineCap(40, "ScheduleTrigger")
	// CapScheduleHistory: ScheduleInfo.Last (last fire, last run and its
	// status) is filled, and stored with the schedule on a durable driver.
	//ds:def id=capschedulehistory-5y6e8cr6 owner=@khanakia stability=stable
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
	//ds:def id=capenqueueonly-9mhemhe8 owner=@khanakia stability=stable
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
	//ds:def id=capshutdownrequeue-q5p8zrkz owner=@khanakia stability=stable
	CapShutdownRequeue = defineCap(44, "ShutdownRequeue")
	// CapForceRun: the client implements RunForcer.
	//ds:def id=capforcerun-ybb9dzyt owner=@khanakia stability=stable
	CapForceRun = defineCap(45, "ForceRun")
	// CapSnooze: a step returning Snooze(d) is put back for d without
	// spending an attempt (see Snooze).
	//ds:def id=capsnooze-pffu58xc owner=@khanakia stability=stable
	CapSnooze = defineCap(46, "Snooze")
	// CapHeartbeat: Step.HeartbeatTimeout is honoured (an attempt that
	// goes that long without Heartbeat fails with ErrHeartbeatTimeout),
	// and Heartbeat from a step reaches the driver (a durable driver
	// extends the attempt's lease).
	//ds:def id=capheartbeat-92ykxh7q owner=@khanakia stability=stable
	CapHeartbeat = defineCap(47, "Heartbeat")
	// CapTenant: Job.Tenant is stored with the run and reported on
	// StepInfo.Tenant, RunInfo.Tenant and LifecycleEvent.Tenant, and
	// RunQuery.Tenant filters by it.
	//ds:def id=captenant-wgqeku2w owner=@khanakia stability=stable
	CapTenant = defineCap(48, "Tenant")
	// CapRunTimeout: Workflow.RunTimeout ends a run that runs too long,
	// Workflow.OnTimeout runs when a run fails because of a timeout, and
	// every timeout error matches ErrTimeout.
	//ds:def id=capruntimeout-nth9c52n owner=@khanakia stability=stable
	CapRunTimeout = defineCap(49, "RunTimeout")
	// CapNodeIdentity: StepInfo.Node, AttemptError.Node and
	// RunInfo.AttemptedBy name the node that ran each attempt, and the
	// client implements NodeLister.
	//ds:def id=capnodeidentity-wdmsuc62 owner=@khanakia stability=stable
	CapNodeIdentity = defineCap(50, "NodeIdentity")
	// CapKeyFIFO: Concurrency.FIFO is honoured: the queued runs of one
	// concurrency key start in enqueue order (Job.Priority aside).
	//ds:def id=capkeyfifo-h9enk5d4 owner=@khanakia stability=stable
	CapKeyFIFO = defineCap(51, "KeyFIFO")
	// CapDueQuery: RunQuery.DueFrom and DueTo are honoured and
	// RunInfo.RunAt is filled.
	CapDueQuery = defineCap(52, "DueQuery")
	// CapResult: the client implements ResultReader and fills
	// RunInfo.Output for a succeeded run.
	//ds:def id=capresult-9dusbjaz owner=@khanakia stability=stable
	CapResult = defineCap(53, "Result")
	// CapCronSeconds: Schedule.Seconds and Workflow.CronSeconds (a
	// six-field expression, seconds first) are honoured.
	//ds:def id=capcronseconds-7ks5ygha owner=@khanakia stability=stable
	CapCronSeconds = defineCap(54, "CronSeconds")
	// CapScheduleActive: Schedule.Active and Workflow.CronActive (a start
	// and end instant) are honoured.
	//ds:def id=capscheduleactive-7jnzkwny owner=@khanakia stability=stable
	CapScheduleActive = defineCap(55, "ScheduleActive")
	// CapScheduleKeep: Schedule.KeepRuns and Workflow.CronKeepRuns (keep
	// only the newest N finished runs of a schedule) are honoured.
	//ds:def id=capschedulekeep-rx5bjfyu owner=@khanakia stability=stable
	CapScheduleKeep = defineCap(56, "ScheduleKeep")
	// CapTickTime: every run a schedule fires carries the tick it stands
	// for in LabelTickAt (TickTimeOf reads it), not only the wall time it
	// started.
	//ds:def id=capticktime-6gn5wpmj owner=@khanakia stability=stable
	CapTickTime = defineCap(57, "TickTime")
	// CapQueueLimitGlobal: QueueConfig.Limit caps the in-flight step
	// attempts of a queue across every client sharing the store, not only
	// this client's workers (QueueConfig.Workers stays per client).
	//ds:def id=capqueuelimitglobal-xqqyp3bq owner=@khanakia stability=stable
	CapQueueLimitGlobal = defineCap(58, "QueueLimitGlobal")
	// CapDiscardExhausted: RetryPolicy.DiscardOnExhaust ends a run whose
	// retries ran out StatusDiscarded instead of StatusFailed.
	//ds:def id=capdiscardexhausted-vzuqguga owner=@khanakia stability=stable
	CapDiscardExhausted = defineCap(59, "DiscardExhausted")
	// CapDrainReport: Drain reports the runs it cut off (DrainReport).
	//ds:def id=capdrainreport-fb2tr3jp owner=@khanakia stability=stable
	CapDrainReport = defineCap(60, "DrainReport")
	// CapWorkflowVersion: every run records the Workflow.Version of the
	// client that enqueued it, and a worker admits, runs and advances
	// only runs whose version its definition runs (MinVersion to
	// Version), so during a rolling deploy a run started by new code
	// never runs on old code (LO-11).
	//ds:def id=capworkflowversion-6gd7k448 owner=@khanakia stability=stable
	CapWorkflowVersion = defineCap(61, "WorkflowVersion")
	// CapSlotCost: Step.Slots is honoured: one attempt of the step holds
	// that many of the client's worker slots (WithWorkers) while it runs,
	// so a heavy step leaves room for fewer others.
	//ds:def id=capslotcost-9fatdg72 owner=@khanakia stability=stable
	CapSlotCost = defineCap(62, "SlotCost")
	// CapPayloadSearch: RunQuery.Payload is honoured: runs are matched on
	// fields of their JSON payload (RS-08).
	//ds:def id=cappayloadsearch-ebrgjdkj owner=@khanakia stability=stable
	CapPayloadSearch = defineCap(63, "PayloadSearch")
	// CapWaitOwnRun: WaitSpec.OwnRun is honoured: a wait step takes the
	// signals addressed to its own run (RunKey, SignalRun, SignalParent),
	// its key resolved from the run ID when the wait begins (WP-22).
	//ds:def id=capwaitownrun-2fd6b64v owner=@khanakia stability=stable
	CapWaitOwnRun = defineCap(64, "WaitOwnRun")
	// CapRoundRobin: under ConcurrencyRoundRobin the keys of a workflow
	// take turns for its workers (FC-05): a key that enqueued a backlog
	// first does not hold back keys that enqueue later. A driver without
	// it still caps each bucket at MaxRuns but serves the runs in
	// priority and arrival order.
	//ds:def id=caproundrobin-yw4jqugm owner=@khanakia stability=stable
	CapRoundRobin = defineCap(65, "RoundRobin")
	// CapKeyWeights: Concurrency.Weight is honoured: while keys of a
	// ConcurrencyRoundRobin workflow have work waiting, each gets turns in
	// proportion to its weight (FC-06).
	//ds:def id=capkeyweights-vqknb9qc owner=@khanakia stability=stable
	CapKeyWeights = defineCap(66, "KeyWeights")
	// CapConcurrencyLimits: Workflow.ConcurrencyLimits are enforced: a run
	// is admitted only when every one of its concurrency keys has room,
	// taking them all at once, and a waiting run is never overtaken on any
	// of its keys by a run behind it (see ConcurrencyLimit). A driver that
	// enforces one key only refuses such a workflow instead of honouring
	// part of it.
	//ds:def id=capconcurrencylimits-585ybb9u owner=@khanakia stability=stable
	CapConcurrencyLimits = defineCap(67, "ConcurrencyLimits")
	// CapHoldWhileRunning: Concurrency.Hold and ConcurrencyLimit.Hold set
	// to HoldWhileRunning are honoured: a run with nothing executing and
	// nothing waiting for a worker gives back those slots and takes them
	// all again, at its original place in admission order, before its
	// next step runs, never exceeding MaxRuns (see ConcurrencyHold).
	//ds:def id=capholdwhilerunning-h6t3nb8d owner=@khanakia stability=stable
	CapHoldWhileRunning = defineCap(68, "HoldWhileRunning")
	// CapScheduleKey: Schedule.IdempotencyKey is honoured: every run the
	// schedule fires, by tick or by TriggerSchedule, carries the key the
	// template expands to, with Job.IdempotencyKey's semantics, and
	// Schedule.OnHeld decides what a trigger does while the key is held.
	CapScheduleKey = defineCap(69, "ScheduleKey")
	// CapCheckpoint: a running step can save a checkpoint
	// (SaveCheckpoint) and read it back on a later attempt of the same
	// run and step (LoadCheckpoint); the driver drops it when the run
	// ends and refuses a save from an attempt that no longer owns its
	// step. Whether it survives a restart is CapDurable's claim.
	CapCheckpoint = defineCap(70, "Checkpoint")
	// CapCoalesce: Workflow.Coalesce is honoured: a key held by a run
	// that already left the queue admits one more run, which waits until
	// that run ends; further enqueues for the key fold into it.
	CapCoalesce = defineCap(71, "Coalesce")
)

// Requires computes the capabilities a driver needs to run wf as written.
// Retry needs come from each Run step's effective policy, so a workflow
// default every step overrides asks for nothing. Durability is not
// inferred: a workflow reads the same on any driver, and the app chooses a
// durable driver when it needs one.
//
//ds:def id=requires-6tcjbcfj owner=@khanakia stability=stable
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
		if wf.Concurrency.Weight != nil {
			c = c.With(CapKeyWeights)
		}
	}
	if len(wf.ConcurrencyLimits) > 0 {
		c = c.With(CapConcurrencyLimits)
	}
	if HoldsWhileRunning(wf) {
		c = c.With(CapHoldWhileRunning)
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
	if wf.Coalesce != nil {
		c = c.With(CapCoalesce, CapIdempotencyKey)
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
			if s.Wait.OwnRun {
				c = c.With(CapWaitOwnRun)
			}
		case StepSleep:
			c = c.With(CapDurableSleep)
		}
	}
	return c
}

// retryCaps is what a retry policy needs beyond plain exponential backoff.
//
//ds:def id=retrycaps-vqww8mgj owner=@khanakia stability=stable
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
//
//ds:def id=misfirecaps-e5ug8m9f owner=@khanakia stability=stable
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
// Active period, CapScheduleKeep for KeepRuns, CapTenant for Tenant and
// CapScheduleKey with CapIdempotencyKey for IdempotencyKey.
//
//ds:def id=requiresschedule-wk3hrhfy owner=@khanakia stability=stable
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
	if s.IdempotencyKey != "" {
		c = c.With(CapScheduleKey, CapIdempotencyKey)
	}
	return c
}

// CheckScheduleCaps returns an UnsupportedError when have lacks what s
// needs, and nil otherwise. Its Field names the cause ("Schedule",
// "Schedule.Location", "Schedule.Windows", "Schedule.Overlap",
// "Schedule.Seconds", "Schedule.Active", "Schedule.KeepRuns",
// "Schedule.Tenant", "Schedule.IdempotencyKey" or "Schedule.Misfire") and its Reason the workflow.
// Drivers call it from Schedule, after ValidateSchedule.
//
//ds:def id=checkschedulecaps-45emsz38 owner=@khanakia stability=stable
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
	case missing.HasAny(CapsOf(CapScheduleKey, CapIdempotencyKey)):
		field = "Schedule.IdempotencyKey"
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
//
//ds:def id=requiresjob-mrfs7vqd owner=@khanakia stability=stable
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
//
//ds:def id=checkjob-2rsfu9ma owner=@khanakia stability=stable
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
//
//ds:def id=checkjobqueue-nkkxjr77 owner=@khanakia stability=stable
func CheckJobQueue(have Caps, wf Workflow, job Job) error {
	if job.Queue == "" || have.Has(CapQueueOverride) || job.Queue == EffectiveQueue(wf, Job{}) {
		return nil
	}
	return &UnsupportedError{Field: "Job.Queue", Missing: CapQueueOverride,
		Reason: "workflow " + wf.Name + " runs on queue " + EffectiveQueue(wf, Job{}) + ", not " + job.Queue}
}

// CheckCaps returns an UnsupportedError naming wf when have lacks what wf
// needs, and nil otherwise. Drivers call it from Start.
//
//ds:def id=checkcaps-eg44ezuh owner=@khanakia stability=stable
func CheckCaps(have Caps, wf Workflow) error {
	if missing := have.Missing(Requires(wf)); !missing.IsZero() {
		return &UnsupportedError{Field: "Workflow", Missing: missing, Reason: "workflow " + wf.Name}
	}
	return nil
}
