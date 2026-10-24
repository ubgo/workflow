package workflow

import (
	"errors"
	"fmt"
)

// RunError is the error a driver records for a run that failed: it carries
// the run, the workflow, the step and the attempt as fields, so code
// holding the error (a RunEvent.Err, an error tracker's hook) finds which
// run to look up with RunErrorOf or errors.As. Its Error text is the
// step's own message unchanged, so RunInfo.Error, logs and dashboards
// keep showing what the app said; Describe gives the message with the
// context in front. It wraps the step's own error, so errors.Is and
// errors.As keep working through it.
type RunError struct {
	RunID    RunID
	Workflow string
	Step     string
	Attempt  int
	Err      error
}

// Error is the step's own message (Err.Error()).
func (e *RunError) Error() string {
	if e.Err == nil {
		return e.Describe()
	}
	return e.Err.Error()
}

// Describe reads `workflow "send-email" run r-12 step "deliver" attempt 3:
// <err>`, for a log line that has nothing else to say which run it was.
func (e *RunError) Describe() string {
	msg := fmt.Sprintf("workflow %q run %s", e.Workflow, e.RunID)
	if e.Step != "" {
		msg += fmt.Sprintf(" step %q", e.Step)
	}
	if e.Attempt > 0 {
		msg += fmt.Sprintf(" attempt %d", e.Attempt)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap returns the step's error.
func (e *RunError) Unwrap() error { return e.Err }

// RunErrorOf returns the RunError err carries, if any.
func RunErrorOf(err error) (*RunError, bool) {
	var re *RunError
	ok := errors.As(err, &re)
	return re, ok
}
