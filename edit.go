package workflow

import (
	"context"
	"time"
)

// JobEdit is a change to a run that has not started (AD-04). A nil field
// is left as it is.
type JobEdit struct {
	// Payload replaces the run's payload. The driver's payload limit
	// applies, as at Enqueue.
	Payload *[]byte

	// Priority replaces Job.Priority.
	Priority *int

	// RunAt moves the start, as Reschedule does (a past time means now).
	RunAt *time.Time
}

// IsZero reports whether e changes nothing.
func (e JobEdit) IsZero() bool { return e.Payload == nil && e.Priority == nil && e.RunAt == nil }

// RunEditor changes a delayed run that has not started: its payload, its
// priority and its start (AD-04, an operator fixing a scheduled job's
// arguments before it runs). The run keeps its RunRef, labels,
// idempotency key and queue; a concurrency or rate key computed from the
// payload is computed again from the new one.
//
// EditRun needs a run in StatusScheduled; any other status returns
// ErrWrongState (a run that was admitted may already be running on
// another node), an unknown ref ErrNotFound, an empty edit ErrInvalid. It
// returns the run's RunRef after the edit (a RunAt now or past admits the
// run, so the ref then carries its RunID). Memory, sqlstore and Redis
// implement it; it answers for itself, as RunRescheduler does, so it has
// no capability: a client without it returns an UnsupportedError from the
// Forward wrapper.
type RunEditor interface {
	EditRun(ctx context.Context, ref RunRef, edit JobEdit) (RunRef, error)
}

// CheckJobEdit refuses an empty edit; drivers call it first.
func CheckJobEdit(e JobEdit) error {
	if e.IsZero() {
		return invalidf("job edit changes nothing")
	}
	return nil
}
