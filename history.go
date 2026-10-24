package workflow

import (
	"errors"
	"time"
)

// DefaultErrorHistory is how many attempt errors a run keeps when the
// driver's error-history option is not set: enough to see a pattern in a
// run that retried a dozen times, small enough that a run retried for a
// week does not grow a row without bound.
const DefaultErrorHistory = 20

// MaxErrorStack caps AttemptError.Stack in bytes, so one deep panic does
// not bloat a stored run. A longer stack keeps its first MaxErrorStack
// bytes, where the panicking frame is.
const MaxErrorStack = 8 << 10

// AttemptError is one failed step attempt in a run's error history
// (RunInfo.Errors). Drivers with CapErrorHistory record one for every
// attempt that returned an error, oldest first.
type AttemptError struct {
	// Step is the step whose attempt failed.
	Step string

	// Attempt is the attempt number within that step, from 1.
	Attempt int

	// Error is the attempt's error message. It is a string because an
	// engine stores only the message.
	Error string

	// At is when the attempt ended, on the driver's clock.
	At time.Time

	// Node is the node that ran the attempt; empty on a driver without
	// CapNodeIdentity.
	Node string

	// Stack is the error's stack trace when it carries one (StackOf);
	// empty otherwise.
	Stack string
}

// StackTracer is an error that carries a stack trace, such as the panic
// error of middleware.Recover. StackOf finds it anywhere in an error's
// chain.
type StackTracer interface {
	error
	StackTrace() string
}

// StackOf returns the stack trace of the first error in err's chain that
// implements StackTracer, cut to MaxErrorStack bytes; empty when there is
// none.
func StackOf(err error) string {
	var st StackTracer
	if !errors.As(err, &st) {
		return ""
	}
	s := st.StackTrace()
	if len(s) > MaxErrorStack {
		s = s[:MaxErrorStack]
	}
	return s
}

// NewAttemptError builds the history entry of a failed attempt.
func NewAttemptError(step string, attempt int, err error, at time.Time) AttemptError {
	return AttemptError{Step: step, Attempt: attempt, Error: err.Error(), At: at, Stack: StackOf(err)}
}

// AppendAttemptError appends e to hist and keeps the newest max entries,
// dropping the oldest. max <= 0 keeps none (history turned off). Every
// driver bounds its history with it, so the bound means the same thing on
// each backend.
func AppendAttemptError(hist []AttemptError, e AttemptError, max int) []AttemptError {
	if max <= 0 {
		return nil
	}
	hist = append(hist, e)
	if n := len(hist) - max; n > 0 {
		hist = append(hist[:0:0], hist[n:]...)
	}
	return hist
}
