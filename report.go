package workflow

import (
	"context"
	"time"
)

// Failure is one failure handed to a Reporter or an alert sink: a run
// that ended badly (Final), or, when the reporter opted in, one failed
// step attempt that will be retried. It never carries the payload, so a
// third-party error tracker never receives job data.
type Failure struct {
	// Ref is the run.
	Ref RunRef

	// Workflow is the workflow name.
	Workflow string

	// Status is the run's end status for a final failure (StatusFailed,
	// or StatusDiscarded/StatusCancelled when the caller asked for
	// those); StatusRetrying for an attempt failure.
	Status Status

	// Step is the failed step when known; empty for a run-level report
	// from a driver that does not say which step failed.
	Step string

	// Attempt is the attempt number for an attempt failure; zero for a
	// final failure.
	Attempt int

	// Err is the error.
	Err error

	// Final is true when the run has ended, false for an attempt that
	// will be retried.
	Final bool

	// Labels are the run's labels. Receivers must not modify them; an
	// integration forwards them only on the caller's opt-in.
	Labels map[string]string

	// At is when it happened, on the observing process's clock.
	At time.Time
}

// Reporter receives failures for an error tracker (Sentry and the like).
// It must be safe for concurrent use and should not block for long: it
// is called on a driver goroutine. Errors are the reporter's to log.
type Reporter interface {
	Report(ctx context.Context, f Failure)
}

// ReporterFunc adapts a function to Reporter.
type ReporterFunc func(ctx context.Context, f Failure)

// Report calls fn.
func (fn ReporterFunc) Report(ctx context.Context, f Failure) { fn(ctx, f) }

// FailureOf turns a run-finished event into a final Failure, or reports
// false when the event is not a failure: StatusFailed always counts;
// StatusDiscarded counts when discarded is true; anything else does not.
func FailureOf(ev RunEvent, discarded bool, at time.Time) (Failure, bool) {
	switch {
	case ev.Status == StatusFailed:
	case ev.Status == StatusDiscarded && discarded:
	default:
		return Failure{}, false
	}
	return Failure{
		Ref:      ev.Ref,
		Workflow: ev.Workflow,
		Status:   ev.Status,
		Step:     ev.Step,
		Err:      ev.Err,
		Final:    true,
		Labels:   ev.Labels,
		At:       at,
	}, true
}
