package workflow

// Status is where a run is.
//
// # The run state machine
//
// A run is created StatusScheduled (Job.RunAt in the future, CapDelayedStart)
// or StatusQueued, and ends in exactly one terminal status. The legal moves,
// what triggers each and the capabilities that gate it are the Transitions
// table below; this is the same thing in prose.
//
//	scheduled -> queued       the start time arrives (CapDelayedStart)
//	queued    -> running      a worker claims a ready step
//	running   -> queued       the attempt is handed back (graceful stop, dead
//	                          worker, lease lost) or the next step is ready
//	running   -> waiting      a Wait or Sleep step parks (CapWaitForEvent,
//	                          CapDurableSleep) or a step Snoozes (CapSnooze)
//	waiting   -> queued       the event arrives or the sleep ends
//	waiting   -> running      the resumed step starts at once
//	running   -> retrying     an attempt failed and RetryPolicy.Next allows
//	                          another; RunInfo.NextAttemptAt says when
//	                          (reported only with CapRunStates; other drivers
//	                          show queued or running for the same moment)
//	retrying  -> running      the backoff elapsed and a worker claimed it
//	retrying  -> queued       the backoff elapsed, waiting for a worker
//	running   -> succeeded    every step succeeded
//	running   -> failed       an attempt failed and RetryPolicy.Next says stop
//	                          (Max, MaxErrors, Until, Permanent, NoRetryOn),
//	                          and RetryPolicy.DiscardOnExhaust is off
//	running   -> discarded    a step returned Discard (CapDiscard +
//	                          CapRunStates), or the same stop with
//	                          RetryPolicy.DiscardOnExhaust on
//	                          (CapDiscardExhausted + CapRunStates)
//	any non-terminal -> cancelled   Cancel or CancelAll
//	                          (CapCancel); a Discard on a driver with only
//	                          CapDiscard also ends cancelled (see EndStatus)
//	any non-terminal -> failed      the run went past Workflow.RunTimeout
//	                          (CapRunTimeout) or ForceFail (CapForceRun)
//	any non-terminal -> succeeded   ForceComplete (CapForceRun)
//	failed    -> queued       Retry re-opens the run from its failed step
//	                          (CapRetryRun); the only move out of a terminal
//	                          status. A discarded or cancelled run is not
//	                          re-opened: enqueue a new job instead.
//
// The active statuses (queued, running, waiting, retrying) describe the run
// as a whole, so with parallel steps a run can move between any two of them
// as its steps progress (one step retrying while another waits). The
// Transitions table therefore lists every active-to-active pair that a
// driver with all of the gating capabilities can report; a driver without
// CapRunStates never reports retrying or discarded.
//
// Failed versus discarded: failed is a failure an operator must see, so
// OnFailure runs for it. Discarded is a run dropped on purpose or given up
// on by policy: OnFailure does not run, the chain does not continue, the
// idempotency key is released, and the Err matches ErrDiscarded and
// ErrCancelled. A run exhausts into discarded only when its effective
// policy (job, step or workflow, see EffectiveRetriesFor) opted in with
// DiscardOnExhaust; the default is failed.
//
// Code that switches on a Status must keep a default branch: drivers that
// declare CapRunStates report StatusRetrying and StatusDiscarded, which
// older drivers never did, and a later version may add more.
type Status string

// The run statuses.
const (
	StatusScheduled Status = "scheduled" // a delayed start that has not begun
	StatusQueued    Status = "queued"    // waiting for a worker
	StatusRunning   Status = "running"   // a step is executing
	StatusWaiting   Status = "waiting"   // suspended on a wait or sleep step
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"

	// StatusRetrying: no step is executing and at least one step attempt
	// failed and waits on its retry backoff. RunInfo.NextAttemptAt says
	// when it runs again. Only drivers with CapRunStates report it; the
	// others report StatusQueued or StatusRunning for the same moment.
	StatusRetrying Status = "retrying"

	// StatusDiscarded is terminal: the run was dropped on purpose rather
	// than failed. Two things give it: a step returned Discard(err), or a
	// step stopped retrying (attempts ran out, MaxErrors or Until stopped
	// it, or a Permanent error) under a RetryPolicy with DiscardOnExhaust
	// set. Without that opt-in a run that runs out of retries ends
	// StatusFailed, because that is a failure an operator must see. Its
	// Err matches ErrDiscarded and ErrCancelled. Only drivers with
	// CapRunStates report it; the others report StatusCancelled (with
	// CapDiscard) or StatusFailed for the same run.
	StatusDiscarded Status = "discarded"
)

// IsTerminal reports whether the run has finished and will not change
// again, with one exception: Retry re-opens a failed run (CapRetryRun). An
// idempotency key is released once its run is terminal.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled, StatusDiscarded:
		return true
	}
	return false
}

// valid reports whether s is one of the Status constants.
func (s Status) valid() bool {
	switch s {
	case StatusScheduled, StatusQueued, StatusRunning, StatusWaiting, StatusRetrying,
		StatusSucceeded, StatusFailed, StatusCancelled, StatusDiscarded:
		return true
	}
	return false
}

// Valid reports whether s is one of the Status constants, so a driver or
// an admin surface can refuse a status it read from storage or a request.
func (s Status) Valid() bool { return s.valid() }

// EndStatus is the terminal status a driver records for a run whose
// step ended it with err, given the driver's capabilities: a discarded
// run (IsDiscarded) is StatusDiscarded with CapRunStates, StatusCancelled
// with CapDiscard alone, and StatusFailed otherwise (Discard falls back
// to Permanent, the direction that never hides a failure); any other
// error is StatusFailed. Every driver calls it, so none decides the
// mapping differently.
func EndStatus(have Caps, err error) Status {
	if !IsDiscarded(err) {
		return StatusFailed
	}
	switch {
	case have.Has(CapRunStates):
		return StatusDiscarded
	case have.Has(CapDiscard):
		return StatusCancelled
	}
	return StatusFailed
}

// Transition is one legal move of the run state machine: a run in From may
// be reported as To when Trigger happens on a driver that has every
// capability in Needs.
type Transition struct {
	From, To Status

	// Trigger says what causes the move, in a few words.
	Trigger string

	// Needs is every capability the driver must declare for the move to be
	// reported; zero means any driver.
	Needs Caps
}

// activeStatuses are the non-terminal statuses a started run moves among.
var activeStatuses = []Status{StatusQueued, StatusRunning, StatusWaiting, StatusRetrying}

// transitions is built once; Transitions hands out copies.
var transitions = buildTransitions()

func buildTransitions() []Transition {
	var t []Transition
	t = append(t,
		Transition{StatusScheduled, StatusQueued, "the start time arrives", CapDelayedStart},
		Transition{StatusQueued, StatusRunning, "a worker claims a ready step", 0},
		Transition{StatusRunning, StatusQueued, "an attempt is handed back or the next step is ready", 0},
		Transition{StatusRunning, StatusWaiting, "a Wait step parks", CapWaitForEvent},
		Transition{StatusRunning, StatusWaiting, "a Sleep step parks", CapDurableSleep},
		Transition{StatusRunning, StatusWaiting, "a step returned Snooze", CapSnooze},
		Transition{StatusWaiting, StatusQueued, "the event arrives or the sleep ends", 0},
		Transition{StatusWaiting, StatusRunning, "the resumed step starts at once", 0},
		Transition{StatusRunning, StatusRetrying, "an attempt failed and the policy allows another", CapRunStates},
		Transition{StatusRetrying, StatusRunning, "the backoff elapsed and a worker claimed the step", CapRunStates},
		Transition{StatusRetrying, StatusQueued, "the backoff elapsed", CapRunStates},
		Transition{StatusRunning, StatusSucceeded, "every step succeeded", 0},
		Transition{StatusRunning, StatusFailed, "the policy stopped retrying a failed attempt", 0},
		Transition{StatusRunning, StatusDiscarded, "a step returned Discard", CapDiscard | CapRunStates},
		Transition{StatusRunning, StatusDiscarded, "the policy stopped retrying and has DiscardOnExhaust", CapDiscardExhausted | CapRunStates},
		Transition{StatusFailed, StatusQueued, "Retry re-opens the run from its failed step", CapRetryRun},
	)
	// Parallel steps let the run as a whole move between any two active
	// statuses; the specific pairs above are the ones with a single cause.
	for _, from := range activeStatuses {
		for _, to := range activeStatuses {
			if from == to || hasTransition(t, from, to) {
				continue
			}
			t = append(t, Transition{from, to, "parallel steps changed which status describes the run", reportCaps(from, to)})
		}
	}
	// Operator and run-level endings reach every non-terminal status.
	for _, from := range append([]Status{StatusScheduled}, activeStatuses...) {
		t = append(t,
			Transition{from, StatusCancelled, "Cancel or CancelAll", CapCancel},
			Transition{from, StatusFailed, "the run went past Workflow.RunTimeout", CapRunTimeout},
			Transition{from, StatusFailed, "ForceFail", CapForceRun},
			Transition{from, StatusSucceeded, "ForceComplete", CapForceRun},
		)
	}
	return t
}

// reportCaps is what a move between two active statuses needs: the
// retrying status exists only with CapRunStates.
func reportCaps(from, to Status) Caps {
	if from == StatusRetrying || to == StatusRetrying {
		return CapRunStates
	}
	return 0
}

func hasTransition(t []Transition, from, to Status) bool {
	for _, x := range t {
		if x.From == from && x.To == to {
			return true
		}
	}
	return false
}

// Transitions returns every legal move of the run state machine. The
// slice is a copy. Drivers and tests use it to check that a status change
// they are about to store (or one they read) is a move the machine allows.
func Transitions() []Transition { return append([]Transition(nil), transitions...) }

// TransitionsFrom returns the moves that leave s: none for a terminal
// status except failed, which Retry re-opens.
func (s Status) TransitionsFrom() []Transition {
	var out []Transition
	for _, t := range transitions {
		if t.From == s {
			out = append(out, t)
		}
	}
	return out
}

// CanTransitionTo reports whether a run in s may be reported as to by a
// driver with every capability of some transition between them. It ignores
// capabilities (a driver without CapRunStates never reports retrying); use
// TransitionsFrom to see what gates each move.
func (s Status) CanTransitionTo(to Status) bool { return hasTransition(transitions, s, to) }
