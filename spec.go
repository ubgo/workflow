package workflow

import (
	"context"
	"time"
)

// RunID identifies one run. Drivers treat it as opaque: the memory driver
// uses a counter, an engine uses its own ID format.
type RunID string

// String returns the ID as a string.
func (id RunID) String() string { return string(id) }

// IsZero reports whether the ID is unset.
func (id RunID) IsZero() bool { return id == "" }

// ScheduledID identifies a delayed start that has not begun yet. Engines
// such as Hatchet return it for a RunAt enqueue and hand out the RunID only
// once the run fires, so the two cannot share one field.
type ScheduledID string

// String returns the ID as a string.
func (id ScheduledID) String() string { return string(id) }

// IsZero reports whether the ID is unset.
func (id ScheduledID) IsZero() bool { return id == "" }

// ScheduleID identifies a recurring trigger made by Client.Schedule. The
// app stores it and passes it back to Unschedule.
type ScheduleID string

// String returns the ID as a string.
func (id ScheduleID) String() string { return string(id) }

// IsZero reports whether the ID is unset.
func (id ScheduleID) IsZero() bool { return id == "" }

// RunRef points at a run. ScheduledID is set while a delayed run has not
// started; RunID is set once it has. A driver that learns the RunID of a
// delayed run resolves it itself, so callers keep passing the RunRef they
// were given.
type RunRef struct {
	RunID       RunID
	ScheduledID ScheduledID
}

// IsZero reports whether the ref points at nothing.
func (r RunRef) IsZero() bool { return r.RunID.IsZero() && r.ScheduledID.IsZero() }

// Workflow is a named graph of steps plus the policies that apply to every
// run of it. A one-step workflow is a plain background job.
type Workflow struct {
	// Name is the key Job.Workflow and Schedule.Workflow refer to.
	Name string

	// Steps form a DAG through Step.After. A step with no After starts
	// first; a plain list with no After runs its steps in parallel, so
	// chain them with After for sequence.
	Steps []Step

	// Cron lists 5-field expressions declared with the workflow and
	// evaluated in CronLocation. They are replaced, never duplicated, when
	// the workflow registers again. Needs CapCron.
	Cron []string

	// CronLocation is the time zone Cron is evaluated in. Nil means UTC
	// (the behaviour before the field existed); time.Local is refused, so
	// no declared cron runs in the host's implicit zone. Any zone but UTC
	// needs CapCronZone: a UTC-only engine refuses it rather than shifting
	// the expression, which would break across DST.
	CronLocation *time.Location

	// Misfire says what happens to Cron ticks the driver reached late or
	// not at all (nothing was running). The zero value is the driver's
	// default, documented in its package. Setting it without Cron is
	// invalid. See Misfire for the capability each policy needs.
	Misfire Misfire

	// Concurrency caps simultaneous runs per key. Nil means no cap.
	// Needs CapConcurrencyKey.
	Concurrency *Concurrency

	// RateLimits cap how often runs may start. Needs CapRateLimit.
	RateLimits []RateLimit

	// Idempotency says how Job.IdempotencyKey is treated. Empty means
	// IdempotencyOptional.
	Idempotency IdempotencyPolicy

	// Retry is the default for steps whose Retry is nil. Its zero value
	// means no retries.
	Retry RetryPolicy

	// Timeout is the default per-step timeout. Zero means none.
	Timeout time.Duration

	// OnFailure runs once when a run fails for good, with the failing
	// step's input. Nil means nothing runs.
	OnFailure Handler

	// Queue is the default queue of this workflow's runs; Job.Queue
	// overrides it per run. Empty means DefaultQueue. A queue other than
	// DefaultQueue needs CapQueue. See QueueConfig for how workers serve
	// queues.
	Queue string

	// ScheduleWhen is asked before each tick of this workflow's schedules
	// fires: the declared Cron and every Client.Schedule of the workflow
	// alike (switch on tick.Name for a rule per schedule). Returning false
	// skips the tick, which tick observers see as TickSkipped with Reason
	// TickReasonWhen. Nil fires every tick. It lives on the workflow, not
	// on Schedule, because a condition is code that every node registers,
	// while a durable driver stores a Schedule as data and may fire it on
	// any node. It is called on the firing node with the driver's context
	// and must return quickly. Needs CapScheduleWhen.
	ScheduleWhen func(ctx context.Context, tick Tick) bool

	// CronWindows limits the declared Cron ticks to time-of-day windows on
	// CronLocation's wall clock. Zero means no limit. Needs
	// CapScheduleWindow.
	CronWindows Windows

	// CronOverlap is the overlap policy of every Cron entry: OverlapSkip
	// skips a tick while that entry's previous run is unfinished. The
	// zero value is OverlapAllow. Each entry is its own schedule, so one
	// entry's run does not hold back another's tick. It needs Cron.
	CronOverlap OverlapPolicy

	// CronSeconds says every Cron entry has six fields, seconds first
	// (cron.ParseSeconds). It is never guessed from the field count,
	// because six fields could as well mean a trailing year. Needs Cron
	// and CapCronSeconds.
	CronSeconds bool

	// CronActive limits the declared Cron to a period: ticks before
	// Active.From or from Active.Until on do not fire (see Active). Zero
	// means always. Needs Cron and CapScheduleActive.
	CronActive Active

	// CronKeepRuns keeps only the newest N finished runs fired by each
	// Cron entry; older finished runs are forgotten as RunDeleter would.
	// Zero keeps them all (the driver's retention still applies). Needs
	// Cron and CapScheduleKeep.
	CronKeepRuns int

	// RunTimeout caps a run's whole life, from its admission to its end
	// (waits and sleeps included). A run past it is ended: running
	// attempts see their ctx cancelled with cause ErrRunTimeout, and the
	// run fails with an error matching ErrRunTimeout and ErrTimeout, which
	// is never retried. Zero means none. Needs CapRunTimeout.
	RunTimeout time.Duration

	// OnTimeout runs once, in place of OnFailure, when a run fails for
	// good because of a timeout (IsTimeout: a step attempt past its
	// Timeout on its last attempt, a wait past its deadline, a heartbeat
	// timeout or RunTimeout), with the failing step's input (the run's
	// payload for RunTimeout outside any step). Nil means OnFailure runs
	// for timeouts too. Needs CapRunTimeout.
	OnTimeout Handler
}

// StepKind is what a step does. Exactly one kind is set per step.
type StepKind string

// The step kinds.
const (
	StepRun   StepKind = "run"   // calls Step.Run
	StepWait  StepKind = "wait"  // suspends until an Event matches Step.Wait
	StepSleep StepKind = "sleep" // suspends for Step.Sleep
)

// Step is one node of a workflow. Exactly one of Run, Wait and Sleep is
// set; Validate rejects anything else.
type Step struct {
	// Name is unique within the workflow; After refers to it.
	Name string

	// After names the parent steps that must succeed before this one
	// starts. Empty means the step starts with the run.
	After []string

	// Run is the code the step executes.
	Run Handler

	// Wait suspends the run until a matching Event or the deadline.
	// Needs CapWaitForEvent.
	Wait *WaitSpec

	// Sleep suspends the run for this long. Needs CapDurableSleep.
	Sleep time.Duration

	// Retry overrides the workflow default. Nil means the workflow's;
	// RetriesNone() means never retry. See EffectiveRetries.
	Retry *RetryPolicy

	// Timeout caps one attempt. Zero means the workflow default.
	Timeout time.Duration

	// HeartbeatTimeout fails an attempt that goes this long without
	// calling Heartbeat (counted from the attempt's start, then from each
	// Heartbeat), with an error matching ErrHeartbeatTimeout and
	// ErrTimeout; the step's retry policy then applies. It catches a
	// handler that hangs well before a long Timeout would. Zero means
	// none. Run steps only. Needs CapHeartbeat.
	HeartbeatTimeout time.Duration
}

// Kind reports which kind the step is, or "" when none or several are
// set (an invalid step).
func (s Step) Kind() StepKind {
	var kinds []StepKind
	if s.Run != nil {
		kinds = append(kinds, StepRun)
	}
	if s.Wait != nil {
		kinds = append(kinds, StepWait)
	}
	if s.Sleep != 0 {
		kinds = append(kinds, StepSleep)
	}
	if len(kinds) != 1 {
		return ""
	}
	return kinds[0]
}

// WaitSpec is a durable wait: the run releases its worker until Signal
// delivers an Event with the same name and key, or the wait expires. At
// most one of Deadline and Timeout is set; neither means wait forever.
type WaitSpec struct {
	// Event is the event name to wait for.
	Event string

	// Key is a fixed key, the same for every run of the workflow. Empty
	// matches an Event with an empty key. Use KeyFrom for a key that
	// differs per run.
	Key string

	// KeyFrom derives the key per run, so many runs of one workflow can
	// wait on the same event name for different things ("payment.succeeded
	// for THIS order"). The driver evaluates it once, when the wait step
	// begins, against the run's Job (its original Payload and Labels, not
	// the step's input), and Signal matches Event.Key against the result.
	// Func is evaluated in Go by any driver (ResolveKey does it); Expr is
	// an expression in the engine's language over the run input (Hatchet's
	// CEL), and a driver without one refuses it with an UnsupportedError.
	// At most one of Key and KeyFrom is set.
	KeyFrom ConcurrencyKey

	// Deadline is the absolute time the wait fails.
	Deadline time.Time

	// Timeout is how long the wait lasts from when it begins.
	Timeout time.Duration
}

// ResolveKey returns the key a run waits on: KeyFrom.Func applied to job
// when set, else the fixed Key. A KeyFrom.Expr is the engine's to
// evaluate, so here it returns an UnsupportedError naming the field; a
// driver that evaluates expressions handles that case before calling.
func (w WaitSpec) ResolveKey(job Job) (string, error) {
	switch {
	case w.KeyFrom.Func != nil:
		return w.KeyFrom.Func(job), nil
	case w.KeyFrom.Expr != "":
		return "", Unsupported("WaitSpec.KeyFrom.Expr", "this driver evaluates Go key functions only")
	}
	return w.Key, nil
}

// Event resumes runs waiting on a matching WaitSpec. Its Payload becomes
// the wait step's output.
type Event struct {
	Name    string
	Key     string
	Payload []byte
}

// Job is one request to run a workflow.
type Job struct {
	// Workflow is the registered workflow name.
	Workflow string

	// Payload is the input of the first steps.
	Payload []byte

	// RunAt delays the start. Zero means now. Needs CapDelayedStart.
	RunAt time.Time

	// IdempotencyKey dedups enqueues while a run holding the key has not
	// finished. Honoured per the workflow's Idempotency policy; needs
	// CapIdempotencyKey.
	IdempotencyKey string

	// Labels carry app metadata (source, trace) explicitly, so a
	// middleware can require one instead of trusting a context convention.
	Labels map[string]string

	// Tenant is the tenant the run belongs to, a first-class field rather
	// than a label convention: drivers store it, report it on
	// StepInfo.Tenant and RunInfo.Tenant, and filter by it
	// (RunQuery.Tenant). Empty means none. It must pass ValidateTenant.
	// Each chained job carries its own Tenant. Needs CapTenant.
	Tenant string

	// Priority orders runs waiting for a worker: higher goes first. Zero
	// is the default priority.
	Priority int

	// Queue overrides the workflow's queue for this run. Empty means the workflow's (EffectiveQueue). Needs
	// CapQueue, and CapQueueOverride when it differs from the workflow's
	// queue (CheckJobQueue).
	Queue string

	// Retry replaces every step's effective policy for this run (a retry
	// budget chosen per dispatch), so it wins over Step.Retry and Workflow.Retry;
	// see EffectiveRetriesFor. It is data only: NoRetryOn and DelayFor
	// must be nil (ValidateJob refuses them), because a durable driver
	// stores the job and cannot store a function. Nil means no override.
	// Needs CapJobRetry plus what the policy itself needs.
	Retry *RetryPolicy

	// Chain lists the jobs that run after this one, in order: when this
	// run succeeds the driver enqueues Chain[0] carrying Chain[1:] as its
	// own Chain (NextInChain), so each job starts only once the one before
	// it succeeded. A run that fails, is discarded or is cancelled ends
	// the chain. A running step may edit the rest of its run's chain with
	// PrependToChain and AppendToChain. The chain is data stored with the
	// run, so on a durable driver it survives a crash. Enqueue middleware
	// runs for this job only, not for the jobs the driver enqueues later.
	// At most MaxChainJobs jobs; each passes ValidateJob. Needs CapChain
	// plus what every chained job needs.
	Chain []Job
}

// Schedule is a recurring trigger made at run time (per tenant, per user),
// as opposed to Workflow.Cron, which is declared with the workflow.
type Schedule struct {
	// Name is a stable, human-readable name an engine may show. A
	// schedule with a Name is upserted by (Workflow, Name) on every
	// driver: calling Schedule again with the same name returns the same
	// ScheduleID and replaces that schedule instead of adding a second, so
	// an app can declare its schedules at every boot. Without a Name every
	// call adds a schedule.
	Name string

	// Workflow is the registered workflow to run.
	Workflow string

	// Cron is a 5-field cron expression.
	Cron string

	// Location is the time zone Cron is evaluated in. It is required, so
	// no schedule is ever evaluated in an implicit zone; use time.UTC for
	// UTC. time.Local is refused for the same reason: it differs from
	// machine to machine. A driver whose engine only runs UTC crons returns an
	// UnsupportedError for any other zone rather than shifting it.
	Location *time.Location

	// Input is the payload of each fired run.
	Input []byte

	// Labels are copied onto each fired run's Job.Labels. Keys under a
	// reserved prefix (ReservedLabelPrefixes) are refused.
	Labels map[string]string

	// Misfire says what happens to ticks the driver reached late or not
	// at all. The zero value is the driver's default.
	Misfire Misfire

	// Windows limits ticks to time-of-day windows on Location's wall
	// clock (only between, or never between, two times). A tick outside them is
	// skipped, which tick observers see as TickSkipped with Reason
	// TickReasonWindow. Zero means no limit. Needs CapScheduleWindow.
	Windows Windows

	// Overlap is the schedule's overlap policy: OverlapSkip skips a tick
	// (and refuses a manual trigger with ErrOverlap) while the run of
	// its previous fire is unfinished. The zero value is OverlapAllow.
	Overlap OverlapPolicy

	// Seconds says Cron has six fields, seconds first (cron.ParseSeconds).
	// Needs CapCronSeconds.
	Seconds bool

	// Active limits the schedule to a period (see Active). Zero means
	// always. Needs CapScheduleActive.
	Active Active

	// KeepRuns keeps only the newest N finished runs this schedule fired;
	// older finished runs are forgotten as RunDeleter would. Zero keeps
	// them all. Needs CapScheduleKeep.
	KeepRuns int

	// Tenant is the Job.Tenant of every run the schedule fires, by tick or
	// by TriggerSchedule, so a per-tenant schedule's runs are found by
	// RunQuery.Tenant like any other run of that tenant. Empty means none.
	// It must pass ValidateTenant and is stored with the schedule. Needs
	// CapTenant.
	Tenant string
}

// MisfirePolicy is what a scheduler does with cron ticks it missed: ticks
// that fell due while nothing was running, or that it reached late.
type MisfirePolicy string

// The misfire policies. A driver declares the ones it honours through
// CapMisfireSkip, CapMisfireFireOnce and CapMisfireFireAll and refuses the
// rest; the empty policy is the driver's own default and needs nothing.
const (
	// MisfireSkip drops every missed tick: a tick fires only when the
	// scheduler reaches it within MisfireGrace of its time (a 09:00 digest
	// must not go out at 14:00).
	MisfireSkip MisfirePolicy = "skip"
	// MisfireFireOnce fires the most recent missed tick once and drops the
	// older ones.
	MisfireFireOnce MisfirePolicy = "fire_once"
	// MisfireFireAll fires every missed tick, oldest first, at most
	// Misfire.Max of them per catch-up (a daily report per missed day).
	MisfireFireAll MisfirePolicy = "fire_all"
)

// MisfireGrace is how late MisfireSkip still fires a tick: a scheduler
// wakes a little after a tick's time even when nothing was missed, and a
// tick reached within this window is on time, not missed.
const MisfireGrace = time.Minute

// Misfire is a cron misfire policy. The zero value means the driver's
// default.
type Misfire struct {
	// Policy is what happens to missed ticks. Empty is the driver default.
	Policy MisfirePolicy

	// Within drops a missed tick reached more than this long after its
	// time, for MisfireFireOnce and MisfireFireAll (a starting deadline).
	// Zero means no limit. MisfireSkip uses MisfireGrace and refuses it.
	Within time.Duration

	// Max caps how many missed ticks MisfireFireAll fires per catch-up:
	// the newest Max, oldest first. Required (at least 1) for
	// MisfireFireAll and refused for the other policies, so a backfill is
	// never unbounded by accident.
	Max int
}

// IsZero reports whether m is the driver default.
func (m Misfire) IsZero() bool { return m == Misfire{} }

// IsUTC reports whether loc evaluates times as UTC: nil (which Workflow.
// CronLocation treats as UTC), time.UTC, or a zone named "UTC".
func IsUTC(loc *time.Location) bool {
	return loc == nil || loc == time.UTC || loc.String() == "UTC"
}

// ConcurrencyStrategy is what happens when a run arrives for a key that
// is already at Concurrency.MaxRuns.
type ConcurrencyStrategy string

// The concurrency strategies. They map onto Hatchet's limit strategies; a
// driver that cannot do one returns an UnsupportedError.
//
// Under DropNewest and CancelNewest, a run that arrives for a full bucket
// is still created: Enqueue returns its RunRef and a nil error, its Status
// is StatusCancelled, and none of its steps runs. An enqueue is therefore
// never an error because of concurrency, on any driver.
const (
	ConcurrencyQueueNewest      ConcurrencyStrategy = "queue_newest"       // the new run waits
	ConcurrencyDropNewest       ConcurrencyStrategy = "drop_newest"        // the new run is dropped; see below
	ConcurrencyCancelNewest     ConcurrencyStrategy = "cancel_newest"      // the new run is cancelled; see below
	ConcurrencyCancelInProgress ConcurrencyStrategy = "cancel_in_progress" // the running one is cancelled
	ConcurrencyRoundRobin       ConcurrencyStrategy = "round_robin"        // keys take turns
)

// Concurrency caps simultaneous runs per key: each distinct key value is
// its own bucket of MaxRuns.
type Concurrency struct {
	// Key derives the bucket. A zero Key puts every run in one bucket.
	Key ConcurrencyKey

	// MaxRuns is the cap per bucket; it must be at least 1.
	MaxRuns int

	// Strategy applies when a bucket is full. Empty means
	// ConcurrencyQueueNewest.
	Strategy ConcurrencyStrategy

	// FIFO promises that the runs queued on one key start in the order
	// they were enqueued (higher Job.Priority still goes first). With
	// MaxRuns 1 that is a per-key sequence: one at a time, in insert
	// order. Only with ConcurrencyQueueNewest (the default). Needs
	// CapKeyFIFO.
	FIFO bool
}

// ConcurrencyKey derives a key from a job: the concurrency bucket, the
// rate-limit bucket, or a per-run wait key (WaitSpec.KeyFrom). It has two
// forms: a Go function (in-process drivers) or an expression in the
// engine's own language (Hatchet's CEL). A driver that cannot evaluate the
// form it is given returns an UnsupportedError naming the field. Build it
// with KeyFunc or KeyExpr.
type ConcurrencyKey struct {
	Func func(Job) string
	Expr string
}

// KeyFunc returns a ConcurrencyKey computed by a Go function.
func KeyFunc(f func(Job) string) ConcurrencyKey { return ConcurrencyKey{Func: f} }

// KeyExpr returns a ConcurrencyKey written in the engine's expression
// language.
func KeyExpr(expr string) ConcurrencyKey { return ConcurrencyKey{Expr: expr} }

// IsZero reports whether the key is unset (one bucket for the workflow).
func (k ConcurrencyKey) IsZero() bool { return k.Func == nil && k.Expr == "" }

// RateLimit caps run starts per key value: at most Limit starts per window
// of length Per. The contract admits both fixed-window and sliding-window
// engines; for one key, every driver guarantees all three of:
//
//   - (a) no span of length Per contains more than 2*Limit starts (a fixed
//     window allows a burst at the end of one window and the start of the
//     next, never more);
//   - (b) N starts take at least (ceil(N/Limit) - 2) * Per from the first
//     to the last (they fill at least ceil(N/Limit) windows, and a fixed
//     window engine may start runs at the very end of the first and the
//     very start of the last: Limit 2, six starts at 0:59.9, 1:00 and
//     2:00 span 60.1s, not 120s);
//   - (c) within one window, at most Limit runs start.
//
// A driver may be stricter (a sliding window keeps any span of length Per
// to Limit starts) and documents which kind it is. A run held back by the
// limit stays queued; it is delayed, never dropped.
type RateLimit struct {
	// Key derives the limited bucket. Zero means one bucket for the
	// workflow.
	Key ConcurrencyKey

	// Limit is the number of starts allowed per window; at least 1.
	Limit int

	// Per is the window length; positive.
	Per time.Duration
}

// IdempotencyPolicy says how Job.IdempotencyKey is treated.
type IdempotencyPolicy string

// The idempotency policies.
const (
	IdempotencyOptional IdempotencyPolicy = "optional" // honoured when present (the default)
	IdempotencyRequired IdempotencyPolicy = "required" // an enqueue without a key fails
	IdempotencyOff      IdempotencyPolicy = "off"      // keys are ignored
)
