// Package workflow is one API for cron schedules, delayed jobs, durable
// background jobs and multi-step workflows, with drivers that range from an
// in-process memory driver to engines such as Hatchet.
//
// The package holds the contract only: the Client interface every driver
// implements, the specs an app writes (Workflow, Step, Job, Schedule), the
// capability set a driver declares (Caps), the sentinel errors, and the
// typed helpers that sit on top of Handler. It needs nothing beyond the
// standard library, and deps_test.go fails the build if that changes.
//
// Three rules hold everywhere:
//
//   - Honest capabilities. A driver declares what it can really do. Start
//     refuses a registered workflow that needs a capability the driver
//     lacks (see Requires and Caps.Missing), and a call the driver cannot
//     honour returns an error wrapping ErrUnsupported that names the field.
//     Nothing is silently dropped.
//   - Nothing runs until Start. Constructing a client opens no connections
//     and starts no goroutines.
//   - No hidden state and no wall clock in logic. Everything hangs off the
//     value the app constructs, and time comes from a clock.Clock.
//
// Seams a driver applies the same way, so their meaning never drifts: the
// retry delay (RetryPolicy.NextDelay: Backoff.Jitter, DelayFor and
// RetryAfter), the cron misfire policy (Misfire.Due), the zone check
// (IsUTC, RequiresSchedule, CheckScheduleCaps), the backend-outage marker
// (ErrUnavailable, Unavailable, IsUnavailable) and the reserved label
// prefixes (ReservedLabelPrefixes, ValidateLabels), the retry decision
// (RetryPolicy.Next with Attempts: Max, MaxErrors, Until), job checks
// (ValidateJob, CheckJob, CheckJobQueue, EffectiveQueue,
// EffectiveRetriesFor), the misfire plan with its dropped count
// (Misfire.Plan), time-of-day windows (Windows.Allows), run query filters
// (RunQuery.Match, ValidateRunQuery, ValidateCancelQuery), pause targets
// (CheckPause, PauseTarget.Holds), the labels of a fired run
// (FiredLabels), the status a step error ends a run with (EndStatus), and
// a chain's next job and edits (NextInChain, ChainEdits).
//
// Optional interfaces beside Client, each declared by a capability:
// RunNotifier, RunQuerier, RunRetrier, RunDeleter, BulkCanceller, Pauser,
// TickNotifier, ScheduleLister, SchedulePauser, Drainer, WorkerScaler and
// ClosureEnqueuer. A client wrapper embeds Forward so it never hides
// them. RunInline runs a workflow in the caller's goroutine with no client
// at all.
//
// Driver-independent pieces built on Client: Deferred, DeferEnqueue and
// DeferHTTP hold enqueues until a request succeeds; Balancer, ByDepth,
// ByWait and BalanceLoop move workers between queues on a WorkerScaler;
// EnqueueClosure runs a Go function on an in-process driver.
package workflow
