package workflow

import (
	"errors"
	"fmt"
	"time"
)

// ErrSnoozed matches the error of a step that snoozed (Snooze), for code
// that inspects an attempt's outcome, such as a lifecycle observer.
var ErrSnoozed = errors.New("workflow: step snoozed")

// MaxSnooze bounds one snooze, so a bug that computes a huge delay cannot
// park a run for years. A longer delay is cut to it.
const MaxSnooze = 30 * 24 * time.Hour

// snoozeError carries the delay of a snooze.
type snoozeError struct{ d time.Duration }

func (s *snoozeError) Error() string { return fmt.Sprintf("workflow: step snoozed for %s", s.d) }

// Is makes errors.Is(err, ErrSnoozed) true.
func (s *snoozeError) Is(target error) bool { return target == ErrSnoozed }

// Snooze, returned by a step handler, puts the step back to run again
// after d without spending an attempt: the next try has the same
// StepInfo.Attempt, the retry policy is not consulted (a snooze is not a
// failure, so it spends none of Max or MaxErrors; Until keeps counting
// from the first attempt), it is not counted in RunInfo.Attempts nor
// recorded in RunInfo.Errors, and the run reports StatusWaiting
// meanwhile. Use it for "not yet" (a resource is not ready,
// an upstream limit is spent) as opposed to a failure. A negative d is
// zero (run again as soon as a worker is free); a d over MaxSnooze is
// MaxSnooze. A driver without CapSnooze treats it as an ordinary error, so
// it spends an attempt: check the capability when that matters.
func Snooze(d time.Duration) error {
	return &snoozeError{d: min(max(d, 0), MaxSnooze)}
}

// SnoozeDelay reports whether err, or any error it wraps, is a Snooze, and
// its delay.
func SnoozeDelay(err error) (time.Duration, bool) {
	var s *snoozeError
	if errors.As(err, &s) {
		return s.d, true
	}
	return 0, false
}
