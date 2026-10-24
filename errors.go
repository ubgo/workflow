package workflow

import (
	"errors"
	"fmt"
)

// Sentinel errors. Callers dispatch on them with errors.Is, never on the
// message text.
var (
	// ErrUnsupported means the driver cannot honour a field or a
	// workflow's needs. Drivers return it wrapped in an UnsupportedError
	// naming the field, never by silently dropping the field.
	ErrUnsupported = errors.New("workflow: unsupported by this driver")

	// ErrNotFound means the workflow, run or schedule does not exist.
	ErrNotFound = errors.New("workflow: not found")

	// ErrNotStarted means a call that needs workers came before Start.
	ErrNotStarted = errors.New("workflow: client not started")

	// ErrAlreadyStarted means Start was called twice, or Register after
	// Start.
	ErrAlreadyStarted = errors.New("workflow: client already started")

	// ErrDuplicate means an idempotency key collided in a way the driver
	// reports as an error rather than by returning the existing run.
	ErrDuplicate = errors.New("workflow: duplicate idempotency key")

	// ErrInvalid means a Workflow, Job or Schedule failed validation.
	ErrInvalid = errors.New("workflow: invalid spec")

	// ErrCancelled is the error a run's steps see when it is cancelled;
	// it is never retried.
	ErrCancelled = errors.New("workflow: run cancelled")

	// ErrUnavailable means the backend could not be reached or did not
	// answer (a refused connection, a timeout talking to the engine or
	// database), as opposed to refusing the request. Drivers mark such
	// errors with Unavailable, so a caller such as a failover client can
	// tell an outage from an app bug without parsing driver messages.
	ErrUnavailable = errors.New("workflow: backend unavailable")

	// ErrDiscarded is the Err of a run a step ended with Discard. A
	// discarded run's Err matches ErrCancelled too, so code that treats
	// cancelled runs as "not failed" keeps working.
	ErrDiscarded = errors.New("workflow: run discarded")

	// ErrWrongState means the run exists but its status does not allow
	// the call: Retry of a run that did not fail, Delete of an unfinished
	// run.
	ErrWrongState = errors.New("workflow: run is in the wrong state for this call")
)

// unavailableError marks err as a backend outage.
type unavailableError struct{ err error }

func (u *unavailableError) Error() string { return u.err.Error() }
func (u *unavailableError) Unwrap() error { return u.err }

// Is makes errors.Is(err, ErrUnavailable) true while Unwrap keeps the
// original reachable, so both checks hold.
func (u *unavailableError) Is(target error) bool { return target == ErrUnavailable }

// Unavailable marks err as "the backend could not be reached": errors.Is
// matches both ErrUnavailable and err itself. Unavailable(nil) is nil.
func Unavailable(err error) error {
	if err == nil {
		return nil
	}
	return &unavailableError{err: err}
}

// IsUnavailable reports whether err, or any error it wraps, is
// ErrUnavailable or was marked by Unavailable.
func IsUnavailable(err error) bool { return errors.Is(err, ErrUnavailable) }

// UnsupportedError names what a driver cannot do. It matches
// ErrUnsupported under errors.Is.
type UnsupportedError struct {
	// Field is the spec field or feature, such as "Job.RunAt" or
	// "Schedule.Location".
	Field string

	// Missing is the capabilities that would be needed, when the refusal
	// comes from a capability check; zero otherwise.
	Missing Caps

	// Reason is an optional detail, such as the workflow name.
	Reason string
}

// Unsupported returns an UnsupportedError for field.
func Unsupported(field, reason string) error {
	return &UnsupportedError{Field: field, Reason: reason}
}

// Error names the field, the missing capabilities and the reason, so the
// message alone tells an operator what to change.
func (e *UnsupportedError) Error() string {
	msg := "workflow: " + e.Field + " unsupported by this driver"
	if e.Missing != 0 {
		msg += " (missing " + e.Missing.String() + ")"
	}
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	return msg
}

// Is makes errors.Is(err, ErrUnsupported) true.
func (e *UnsupportedError) Is(target error) bool { return target == ErrUnsupported }

// invalidf returns an error wrapping ErrInvalid.
func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalid}, args...)...)
}

// permanentError marks an error that must not be retried.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent marks err as non-retryable: the step fails now and its
// remaining attempts are skipped. Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, was marked by
// Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// discardError marks a run to be dropped rather than failed.
type discardError struct{ err error }

func (d *discardError) Error() string { return d.err.Error() }
func (d *discardError) Unwrap() error { return d.err }

// Discard marks err as "drop this run": the step is not retried, OnFailure does not
// run, and the run ends StatusDiscarded on a driver with CapRunStates
// (StatusCancelled on one with only CapDiscard; see EndStatus), with an
// Err matching ErrDiscarded and ErrCancelled. Use it when the work no longer makes sense (the order
// it was about is gone), as opposed to Permanent, which fails the run so
// an operator sees it. On a driver without CapDiscard it behaves as
// Permanent: the run fails, the direction that never hides a failure.
// Discard(nil) is nil.
func Discard(err error) error {
	if err == nil {
		return nil
	}
	return &discardError{err: err}
}

// IsDiscarded reports whether err, or any error it wraps, was marked by
// Discard.
func IsDiscarded(err error) bool {
	var d *discardError
	return errors.As(err, &d)
}

// DiscardedError is the Err a driver records for a run a step discarded:
// it matches ErrDiscarded, ErrCancelled and the step's own error under
// errors.Is. step names the step; err is what the step returned.
func DiscardedError(step string, err error) error {
	return fmt.Errorf("%w: %w: step %q: %w", ErrCancelled, ErrDiscarded, step, err)
}
