package recorder

import (
	"log/slog"
	"slices"
	"time"

	"github.com/ubgo/workflow"
)

// RunRecord is one run as a Store keeps it: the RunInfo a RunQuerier
// answers with, plus what only a recorder knows.
//
// The embedded RunInfo is filled from the recorder's own writes, never from
// the driver: Payload is the run's input as enqueued (after redaction, and
// nil when it was larger than the recorder's MaxPayload; PayloadSize says
// how big it was), Output is the run's result when the recorder could read
// one (WithResultReader) or nil, Errors is the attempt error history, and
// AttemptedBy the nodes that ran attempts. NextAttempt, NextAttemptAt and
// Progress stay zero: a recorder does not see them.
type RunRecord struct {
	workflow.RunInfo

	// Source is what started the run (workflow.SourceOf of its labels, or
	// the recorder's default source, or SourceExternal for a backfilled
	// run); SourceRef is the reference within it.
	Source    workflow.Source
	SourceRef string

	// Outcome is the last outcome a step set (workflow.SetOutcome), or
	// workflow.DefaultOutcome of the terminal status when none was set;
	// "" while the run is unfinished and nothing was set. OutcomeNote is
	// the note that came with a set outcome.
	Outcome     workflow.Outcome
	OutcomeNote string

	// OutcomeSet is true when a step set the outcome, so a later default
	// never overwrites it.
	OutcomeSet bool

	// PayloadSize and OutputSize are the sizes of the input and result as
	// the recorder saw them; larger than len(Payload) or len(Output) when
	// the body was not kept (WithMaxPayload).
	PayloadSize int
	OutputSize  int

	// Stale is set by the Reconciler when the client still reports the run
	// unfinished (or does not know it) and nothing was recorded for it for
	// longer than its StaleAfter. It is a flag for an operator; nothing is
	// cancelled. Any later write for the run clears it.
	Stale bool

	// UpdatedAt is the time of the last write that changed the run's life
	// (enqueue, step start or end, event, outcome, finish), on the
	// recorder's clock. The Reconciler measures staleness from it.
	UpdatedAt time.Time

	// CheckedAt is when the Reconciler last compared the record with the
	// client; zero when it never did. It orders Pending so every
	// unfinished record gets its turn.
	CheckedAt time.Time

	// Steps is every step attempt, ordered by start time then step name
	// then attempt, each with its events. Store.Run fills it; listings
	// (Records, Runs) leave it nil to stay cheap.
	Steps []StepRecord
}

// Key is the record's identity within a Store: the RunID once known, else
// the ScheduledID. A delayed run is first recorded under its ScheduledID;
// the first write that carries both IDs moves it under the RunID.
func (r RunRecord) Key() string { return KeyOf(r.Ref) }

// KeyOf is the Store identity of ref (see RunRecord.Key). The prefixes keep
// a RunID and a ScheduledID with the same text apart.
func KeyOf(ref workflow.RunRef) string {
	if !ref.RunID.IsZero() {
		return "r:" + string(ref.RunID)
	}
	if !ref.ScheduledID.IsZero() {
		return "s:" + string(ref.ScheduledID)
	}
	return ""
}

// Clone returns a deep copy, so a store can hand a record out without
// sharing its state.
func (r RunRecord) Clone() RunRecord {
	r.RunInfo = r.RunInfo.Clone()
	if r.Steps != nil {
		steps := make([]StepRecord, len(r.Steps))
		for i, s := range r.Steps {
			steps[i] = s.Clone()
		}
		r.Steps = steps
	}
	return r
}

// StepStatus is where one step attempt is.
//
// Code that switches on it must keep a default branch: a later version may
// add statuses.
type StepStatus string

// The step attempt statuses.
const (
	// StepRunning: the attempt started and has not ended (or its worker
	// died before it could say so).
	StepRunning StepStatus = "running"

	// StepSucceeded: the handler returned no error.
	StepSucceeded StepStatus = "succeeded"

	// StepFailed: the handler returned an error (the step may retry).
	StepFailed StepStatus = "failed"

	// StepSnoozed: the handler returned workflow.Snooze; the step runs
	// again later without spending an attempt.
	StepSnoozed StepStatus = "snoozed"
)

// stepStatuses is the canonical list, for validation.
var stepStatuses = [...]StepStatus{StepRunning, StepSucceeded, StepFailed, StepSnoozed}

// StepStatuses returns every step status, as a fresh slice.
func StepStatuses() []StepStatus { return slices.Clone(stepStatuses[:]) }

// Valid reports whether s is one of the StepStatus constants.
func (s StepStatus) Valid() bool { return slices.Contains(stepStatuses[:], s) }

// StepRecord is one step attempt.
type StepRecord struct {
	// Step is the step name; Attempt is 1 for the first attempt. A snoozed
	// attempt that runs again keeps its number, so its record is updated
	// in place by the next try.
	Step    string
	Attempt int

	// Status is where the attempt is.
	Status StepStatus

	// Input and Output are the step's input and output after redaction;
	// nil when the recorder keeps no step bodies (WithStepBodies(false))
	// or the body was larger than MaxPayload (InputSize and OutputSize say
	// how big it was).
	Input      []byte
	Output     []byte
	InputSize  int
	OutputSize int

	// Error is the attempt's error message and Stack its stack trace
	// (workflow.StackOf), both empty unless Status is StepFailed or
	// StepSnoozed.
	Error string
	Stack string

	// Node ran the attempt (workflow.StepInfo.Node); empty on a driver
	// without CapNodeIdentity.
	Node string

	// StartedAt and FinishedAt are on the recorder's clock; FinishedAt is
	// zero while the attempt runs.
	StartedAt  time.Time
	FinishedAt time.Time

	// Events is the attempt's timeline (workflow.RecordEvent), in the
	// order recorded.
	Events []EventRecord
}

// Duration is how long the attempt ran; zero while it runs.
func (s StepRecord) Duration() time.Duration {
	if s.FinishedAt.IsZero() {
		return 0
	}
	return s.FinishedAt.Sub(s.StartedAt)
}

// Clone returns a deep copy.
func (s StepRecord) Clone() StepRecord {
	s.Input = slices.Clone(s.Input)
	s.Output = slices.Clone(s.Output)
	if s.Events != nil {
		ev := make([]EventRecord, len(s.Events))
		for i, e := range s.Events {
			ev[i] = e.Clone()
		}
		s.Events = ev
	}
	return s
}

// EventRecord is one stored step event.
type EventRecord struct {
	// Seq orders the events of one attempt; the store assigns it (1, 2,
	// ...).
	Seq int

	// Message, Level and Attrs are workflow.StepEvent's, after redaction.
	Message string
	Level   slog.Level
	Attrs   []workflow.EventAttr

	// At is when it was recorded, on the recorder's clock.
	At time.Time
}

// Clone returns a deep copy.
func (e EventRecord) Clone() EventRecord {
	e.Attrs = slices.Clone(e.Attrs)
	return e
}
