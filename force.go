package workflow

import (
	"context"
	"errors"
)

// ErrForced is the context cause of a step attempt cut short because an
// operator forced its run to end (RunForcer), and the error a
// force-failed run reports (it matches ErrForced).
var ErrForced = errors.New("workflow: run ended by an operator")

// RunForcer ends a run from outside, for an operator handling a work
// item a step cannot finish on its own. A client implements it when it
// declares CapForceRun.
//
// ForceComplete ends a run as StatusSucceeded without running its
// remaining steps. It accepts a run that is not terminal, or one that
// is StatusFailed (accepting a failure as done). Attempts in progress
// have their context cancelled with cause ErrForced and their results
// are ignored. The run is then treated exactly as a success: its
// idempotency key is released, its Job.Chain continues, and run-finished
// observers see StatusSucceeded (again, for a run that had failed: they
// already tolerate repeats). Other terminal statuses return
// ErrWrongState.
//
// ForceFail ends a run that is not terminal as StatusFailed, with an
// error matching ErrForced that carries reason. No retry is made and
// OnFailure does not run (the operator has decided; it is not the
// workflow's failure path); RunRetrier can still re-run it. A terminal
// run returns ErrWrongState.
//
// Both return ErrNotFound for an unknown ref. Neither moves a run to
// another worker or queue: re-running a failed run elsewhere is
// RunRetrier (any worker may pick it up).
type RunForcer interface {
	ForceComplete(ctx context.Context, ref RunRef) error
	ForceFail(ctx context.Context, ref RunRef, reason string) error
}

// forcedError is the error of a force-failed run.
type forcedError struct{ reason string }

func (e *forcedError) Error() string {
	if e.reason == "" {
		return ErrForced.Error()
	}
	return ErrForced.Error() + ": " + e.reason
}

func (e *forcedError) Unwrap() error { return ErrForced }

// ForcedFailure returns the error a force-failed run carries: it matches
// ErrForced and its message ends with reason. Drivers store its message
// as RunInfo.Error.
func ForcedFailure(reason string) error { return &forcedError{reason: reason} }

// CanForceComplete reports whether ForceComplete accepts a run in status
// st. Every driver decides with it.
func CanForceComplete(st Status) bool { return !st.IsTerminal() || st == StatusFailed }

// CanForceFail reports whether ForceFail accepts a run in status st.
func CanForceFail(st Status) bool { return !st.IsTerminal() }
