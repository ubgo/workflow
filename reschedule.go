package workflow

import (
	"context"
	"errors"
	"time"
)

// RunRescheduler moves a delayed run that has not started to a new time.
//
// Reschedule needs a run in StatusScheduled (a Job.RunAt enqueue not yet
// due): it runs at `at` instead, keeps its RunRef, payload, labels and
// idempotency key, and RunInfo.RunAt reads `at`. An `at` in the past (or
// zero) means now. Any other status returns ErrWrongState, an unknown ref
// ErrNotFound. A client without it returns an UnsupportedError from the
// Forward wrapper; the fallback is Cancel plus Enqueue.
type RunRescheduler interface {
	Reschedule(ctx context.Context, ref RunRef, at time.Time) (RunRef, error)
}

// ErrStepCancelled is the cause of a step attempt's ctx when an operator
// cancelled that step (StepCanceller), and matches the step's error when
// it ends because of it.
var ErrStepCancelled = errors.New("workflow: step cancelled")

// StepCanceller withdraws one step of a run, leaving the rest of the run
// to decide what happens next.
//
// CancelStep acts on a step that is running, waiting on its retry
// backoff, snoozed, or a wait or sleep step that is suspended:
//
//   - a running attempt's ctx is cancelled with cause ErrStepCancelled
//     (context.Cause). Whatever the handler then returns decides: an
//     output means the step succeeded and the run continues (a handler
//     can return a fallback, the "interrupting boundary" of a process
//     model); an error fails the step for good, without retry, with an
//     error matching ErrStepCancelled, and the run fails as any failed
//     run does (OnFailure runs);
//   - a step with no attempt running (backoff, snooze, wait, sleep)
//     fails for good at once with an error matching ErrStepCancelled.
//
// A step that has not started, or has ended, returns ErrWrongState; an
// unknown run or step ErrNotFound. Cancelling the whole run is Cancel.
type StepCanceller interface {
	CancelStep(ctx context.Context, ref RunRef, step string) error
}
