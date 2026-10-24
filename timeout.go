package workflow

import (
	"errors"
	"fmt"
)

// The timeout errors. Each of ErrStepTimeout, ErrWaitTimeout,
// ErrHeartbeatTimeout and ErrRunTimeout matches ErrTimeout too, so code
// reacting to "a deadline was missed" checks one sentinel (IsTimeout), and
// code that cares which deadline checks the specific one. Drivers with
// CapRunTimeout wrap every timeout they report with TimeoutError.
var (
	// ErrTimeout matches every timeout below.
	ErrTimeout = errors.New("workflow: deadline missed")

	// ErrStepTimeout: a step attempt ran past Step.Timeout (or the
	// workflow's Timeout). The retry policy applies.
	ErrStepTimeout = errors.New("workflow: step attempt timed out")

	// ErrWaitTimeout: a wait step reached its Deadline or Timeout with no
	// matching event.
	ErrWaitTimeout = errors.New("workflow: wait expired")

	// ErrHeartbeatTimeout: an attempt went Step.HeartbeatTimeout without
	// calling Heartbeat. The retry policy applies.
	ErrHeartbeatTimeout = errors.New("workflow: heartbeat timed out")

	// ErrRunTimeout: the run went past Workflow.RunTimeout. It is never
	// retried.
	ErrRunTimeout = errors.New("workflow: run timed out")
)

// timeoutError ties a specific timeout sentinel to ErrTimeout and keeps the
// driver's own detail.
type timeoutError struct {
	kind error // one of the specific sentinels
	msg  string
}

func (e *timeoutError) Error() string { return e.msg }

// Is matches ErrTimeout and the specific kind.
func (e *timeoutError) Is(target error) bool { return target == ErrTimeout || target == e.kind }

// TimeoutError returns an error matching kind (ErrStepTimeout,
// ErrWaitTimeout, ErrHeartbeatTimeout or ErrRunTimeout) and ErrTimeout,
// with detail in its message, such as `step "fetch" attempt 2 after 30s`.
// A kind that is not one of them is still matched by kind, and by
// ErrTimeout. Drivers build every timeout error with it, so IsTimeout is
// reliable on every driver with CapRunTimeout.
func TimeoutError(kind error, detail string) error {
	msg := kind.Error()
	if detail != "" {
		msg = fmt.Sprintf("%s: %s", msg, detail)
	}
	return &timeoutError{kind: kind, msg: msg}
}

// IsTimeout reports whether err, or any error it wraps, is a missed
// deadline (matches ErrTimeout).
func IsTimeout(err error) bool { return errors.Is(err, ErrTimeout) }

// FailureHandler is the handler a driver runs when a run fails for good
// with err: wf.OnTimeout when err is a timeout and OnTimeout is set, else
// wf.OnFailure (nil when neither applies). Every driver calls it, so none
// decides differently.
func FailureHandler(wf Workflow, err error) Handler {
	if wf.OnTimeout != nil && IsTimeout(err) {
		return wf.OnTimeout
	}
	return wf.OnFailure
}
