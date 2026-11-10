package workflow

import "context"

// Outcome says whether a run's work succeeded, separately from its Status,
// which says whether the worker ran it to the end. A sync that skipped
// because nothing changed, or that finished with 3 bad rows out of 100, is
// StatusSucceeded either way; its outcome tells them apart. A step sets it
// with SetOutcome; a run recorder stores the last one set and derives one
// from the status when none was (DefaultOutcome).
//
// Code that switches on it must keep a default branch: a later version may
// add outcomes.
type Outcome string

// The outcomes.
const (
	// OutcomeSuccess: the work was done in full.
	OutcomeSuccess Outcome = "success"

	// OutcomePartial: some of the work was done, some was not (bad rows,
	// a page that failed); the run still succeeded.
	OutcomePartial Outcome = "partial"

	// OutcomeError: the work was not done.
	OutcomeError Outcome = "error"

	// OutcomeSkipped: there was nothing to do (no changes since the last
	// sync, a feature turned off).
	OutcomeSkipped Outcome = "skipped"
)

// outcomes is the canonical list, for validation and for callers that
// iterate the set (filter chips, tests).
var outcomes = [...]Outcome{OutcomeSuccess, OutcomePartial, OutcomeError, OutcomeSkipped}

// Outcomes returns every outcome, as a fresh slice.
func Outcomes() []Outcome { return append([]Outcome(nil), outcomes[:]...) }

// Valid reports whether o is one of the Outcome constants.
func (o Outcome) Valid() bool {
	for _, k := range outcomes {
		if o == k {
			return true
		}
	}
	return false
}

// MaxOutcomeNote is the longest note SetOutcome accepts, in bytes.
const MaxOutcomeNote = 1 << 10

// DefaultOutcome is the outcome of a run that ended with status and set
// none: OutcomeSuccess for StatusSucceeded, OutcomeError for StatusFailed
// and StatusDiscarded, and "" for every other status (a cancelled or
// unfinished run has no outcome yet).
func DefaultOutcome(status Status) Outcome {
	switch status {
	case StatusSucceeded:
		return OutcomeSuccess
	case StatusFailed, StatusDiscarded:
		return OutcomeError
	}
	return ""
}

// OutcomeSetter stores the outcome a step sets. A run recorder binds one on
// the step's ctx (WithOutcomeSetter); drivers keep none.
type OutcomeSetter interface {
	SetOutcome(ctx context.Context, o Outcome, note string) error
}

// OutcomeFunc adapts a function to OutcomeSetter.
type OutcomeFunc func(ctx context.Context, o Outcome, note string) error

// SetOutcome calls f.
func (f OutcomeFunc) SetOutcome(ctx context.Context, o Outcome, note string) error {
	return f(ctx, o, note)
}

type outcomeSetterKey struct{}

// WithOutcomeSetter returns a ctx carrying s. A run recorder's step
// middleware calls it.
func WithOutcomeSetter(ctx context.Context, s OutcomeSetter) context.Context {
	return context.WithValue(ctx, outcomeSetterKey{}, s)
}

// SetOutcome records the run's outcome from inside a step, with an
// optional note for a person ("3 of 100 rows failed"). The last call in a
// run wins. It validates o and the note's length first (ErrInvalid).
// Without a setter on ctx (no run recorder, or ctx is not a step's) it
// returns an UnsupportedError, as ReportProgress does: the caller asked
// for data to be kept, so dropping it silently would be a lie. A step that
// sets an outcome whether or not one is recorded can ignore that error.
func SetOutcome(ctx context.Context, o Outcome, note string) error {
	if !o.Valid() {
		return invalidf("outcome %q is not one of %v", o, outcomes)
	}
	if len(note) > MaxOutcomeNote {
		return invalidf("outcome note is %d bytes, more than %d", len(note), MaxOutcomeNote)
	}
	s, ok := ctx.Value(outcomeSetterKey{}).(OutcomeSetter)
	if !ok || s == nil {
		return Unsupported("SetOutcome", "no run recorder is bound on ctx, or ctx is not a step's")
	}
	return s.SetOutcome(ctx, o, note)
}
