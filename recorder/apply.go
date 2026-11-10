package recorder

import (
	"maps"
	"slices"
	"time"

	"github.com/ubgo/workflow"
)

// The merge rules every Store follows, as pure functions over a record, so
// the in-memory store, the SQL store and an app's own store agree on what
// a write does. A store loads the record (or starts from NewRecord), calls
// the function, and saves the result; the functions never read a clock.
//
// The shared principles:
//
//   - Fill, don't overwrite: a field the record already holds (workflow,
//     queue, tenant, labels, source, payload) is kept, because the first
//     writer to know it (usually the enqueue seam) knew it best, and a
//     duplicate or late write must not change it.
//   - Enqueue time is the earliest known; start time the earliest attempt.
//   - A terminal status changes only through ApplyFinish (a later finish
//     wins) or a new attempt after it (a retried run re-opens).
//   - A later write about the run's life clears Stale and moves
//     UpdatedAt.

// NewRecord is the empty record a store starts from for a run it has never
// seen.
func NewRecord(ref workflow.RunRef) RunRecord {
	return RunRecord{RunInfo: workflow.RunInfo{Ref: ref}}
}

// fillRef adds the ID ref knows and r lacks.
func fillRef(r *RunRecord, ref workflow.RunRef) {
	if r.Ref.RunID.IsZero() {
		r.Ref.RunID = ref.RunID
	}
	if r.Ref.ScheduledID.IsZero() {
		r.Ref.ScheduledID = ref.ScheduledID
	}
}

// fillIdentity fills the run-level fields every write may carry.
func fillIdentity(r *RunRecord, wf, queue, tenant string, labels map[string]string, src workflow.Source, srcRef string) {
	if r.Workflow == "" {
		r.Workflow = wf
	}
	if r.Queue == "" {
		r.Queue = queue
	}
	if r.Tenant == "" {
		r.Tenant = tenant
	}
	if r.Labels == nil && len(labels) > 0 {
		r.Labels = maps.Clone(labels)
	}
	if r.Source == "" && src != "" {
		r.Source, r.SourceRef = src, srcRef
	}
}

// touch marks a write about the run's life.
// A write no later than the last one (a backfill that only fills fields)
// leaves UpdatedAt and the Stale flag as they were.
func touch(r *RunRecord, at time.Time) {
	if at.After(r.UpdatedAt) {
		r.UpdatedAt = at
		r.Stale = false
	}
}

// earliest returns the earlier non-zero time of a and b.
func earliest(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

// ApplyRunWrite merges w into r. isNew says r came from NewRecord, so its
// status is set from w.
func ApplyRunWrite(r *RunRecord, isNew bool, w RunWrite) {
	fillRef(r, w.Ref)
	fillIdentity(r, w.Workflow, w.Queue, w.Tenant, w.Labels, w.Source, w.SourceRef)
	if isNew || r.Status == "" {
		r.Status = w.Status
	}
	if r.Payload == nil && w.Payload != nil {
		r.Payload = slices.Clone(w.Payload)
	}
	if r.PayloadSize == 0 {
		r.PayloadSize = w.PayloadSize
	}
	if r.Priority == 0 {
		r.Priority = w.Priority
	}
	if r.RunAt.IsZero() {
		r.RunAt = w.RunAt
	}
	r.EnqueuedAt = earliest(r.EnqueuedAt, w.EnqueuedAt)
	touch(r, w.At)
}

// ApplyStepStart merges the run-level part of s into r: the run is running
// (a terminal run re-opens, because only a retry starts an attempt after
// the end), its first start and attempt count move, and the node joins
// AttemptedBy. The step record itself is NewStepRecord(s). isNewAttempt
// says the store had no record of this (step, attempt) yet, so a repeated
// start (at-least-once, or a snoozed attempt's next try) does not count
// twice.
func ApplyStepStart(r *RunRecord, isNewAttempt bool, s StepStart) {
	fillRef(r, s.Ref)
	fillIdentity(r, s.Workflow, s.Queue, s.Tenant, s.Labels, s.Source, s.SourceRef)
	// A start older than the recorded end is a late duplicate of an
	// attempt that ran before the end: count it, keep the end.
	lateDuplicate := r.Status.IsTerminal() && !r.FinishedAt.IsZero() && s.At.Before(r.FinishedAt)
	if !lateDuplicate {
		r.Status = workflow.StatusRunning
		r.FinishedAt = time.Time{}
		r.Error, r.FailedStep = "", ""
	}
	r.StartedAt = earliest(r.StartedAt, s.At)
	r.EnqueuedAt = earliest(r.EnqueuedAt, s.At)
	if isNewAttempt {
		r.Attempts++
	}
	if s.Node != "" && !slices.Contains(r.AttemptedBy, s.Node) {
		r.AttemptedBy = append(r.AttemptedBy, s.Node)
	}
	touch(r, s.At)
}

// StartsNewAttempt reports whether a StepStart counts as a new attempt,
// given the store's record of that (step, attempt), nil when it has none.
// A record with a zero StartedAt is a placeholder an event created when it
// raced ahead of its attempt's start, so the start still counts; a record
// that started before is a repeat (at-least-once, or a snoozed attempt's
// next try) and does not.
func StartsNewAttempt(existing *StepRecord) bool {
	return existing == nil || existing.StartedAt.IsZero()
}

// NewStepRecord is the step record a StepStart creates. A store that
// already holds the (step, attempt) record (a snoozed attempt runs again)
// resets it with ResetStepRecord instead, keeping its events.
func NewStepRecord(s StepStart) StepRecord {
	return StepRecord{
		Step: s.Step, Attempt: s.Attempt, Status: StepRunning, Node: s.Node,
		Input: slices.Clone(s.Input), InputSize: s.InputSize, StartedAt: s.At,
	}
}

// ResetStepRecord applies a repeated StepStart to an existing attempt
// record: running again, its end cleared, its events kept.
func ResetStepRecord(sr *StepRecord, s StepStart) {
	sr.Status = StepRunning
	sr.FinishedAt = time.Time{}
	sr.Output, sr.OutputSize = nil, 0
	sr.Error, sr.Stack = "", ""
	if s.Node != "" {
		sr.Node = s.Node
	}
	if sr.Input == nil {
		sr.Input, sr.InputSize = slices.Clone(s.Input), s.InputSize
	}
	sr.StartedAt = earliest(sr.StartedAt, s.At)
}

// ApplyStepEnd merges the run-level part of e into r: a failed attempt
// joins the error history (bounded by e.ErrorHistory) and, while the run
// is unfinished, becomes its current Error, as RunInfo.Error documents for
// a run waiting on its retry. A snooze is not a failure and adds nothing.
func ApplyStepEnd(r *RunRecord, e StepEnd) {
	fillRef(r, e.Ref)
	if e.Status == StepFailed {
		ae := workflow.AttemptError{Step: e.Step, Attempt: e.Attempt, Error: e.Error, At: e.At, Node: e.Node, Stack: e.Stack}
		if !slices.ContainsFunc(r.Errors, func(x workflow.AttemptError) bool { return x.Step == ae.Step && x.Attempt == ae.Attempt }) {
			r.Errors = workflow.AppendAttemptError(r.Errors, ae, e.ErrorHistory)
		}
		if !r.Status.IsTerminal() {
			r.Error = e.Error
		}
	}
	touch(r, e.At)
}

// ApplyStepEndToStep applies e to the attempt's own record. An end for an
// attempt the store never saw start creates it from e (sr is then the zero
// StepRecord with Step and Attempt set by the store).
func ApplyStepEndToStep(sr *StepRecord, e StepEnd) {
	sr.Step, sr.Attempt = e.Step, e.Attempt
	sr.Status = e.Status
	sr.Output, sr.OutputSize = slices.Clone(e.Output), e.OutputSize
	sr.Error, sr.Stack = e.Error, e.Stack
	if sr.Node == "" {
		sr.Node = e.Node
	}
	sr.FinishedAt = e.At
	if sr.StartedAt.IsZero() {
		sr.StartedAt = e.At
	}
}

// NewEventRecord is the stored form of e; the store sets Seq to one more
// than the attempt's last event.
func NewEventRecord(e EventWrite, seq int) EventRecord {
	return EventRecord{Seq: seq, Message: e.Message, Level: e.Level, Attrs: slices.Clone(e.Attrs), At: e.At}
}

// ApplyEvent merges the run-level part of an event: activity.
func ApplyEvent(r *RunRecord, e EventWrite) {
	fillRef(r, e.Ref)
	touch(r, e.At)
}

// ApplyOutcome records a set outcome; the last one set wins.
func ApplyOutcome(r *RunRecord, o OutcomeWrite) {
	fillRef(r, o.Ref)
	r.Outcome, r.OutcomeNote, r.OutcomeSet = o.Outcome, o.Note, true
	touch(r, o.At)
}

// ApplyFinish merges f into r and reports whether it did. A finish is
// ignored when an attempt started after it (latestStart, the newest
// StartedAt among the run's step records): it is a late duplicate of an
// end the run has since been retried out of. Otherwise the status, error,
// end time and result are f's (a repeated finish rewrites the same values),
// and the outcome defaults from the status unless a step set one.
func ApplyFinish(r *RunRecord, f Finish, latestStart time.Time) bool {
	if !f.FinishedAt.IsZero() && latestStart.After(f.FinishedAt) {
		return false
	}
	fillRef(r, f.Ref)
	fillIdentity(r, f.Workflow, "", f.Tenant, f.Labels, f.Source, f.SourceRef)
	r.Status = f.Status
	r.Error, r.FailedStep = f.Error, f.FailedStep
	r.FinishedAt = f.FinishedAt
	if f.Output != nil || f.OutputSize > 0 {
		r.Output, r.OutputSize = slices.Clone(f.Output), f.OutputSize
	}
	if !r.OutcomeSet {
		r.Outcome, r.OutcomeNote = workflow.DefaultOutcome(f.Status), ""
	}
	touch(r, f.At)
	return true
}

// ApplyCheck records a Reconciler check. It moves CheckedAt and the Stale
// flag, and the status when the client reported a different unfinished
// one; it never touches UpdatedAt, which measures the run's own activity.
// A record that became terminal meanwhile keeps its status.
func ApplyCheck(r *RunRecord, c Check) {
	fillRef(r, c.Ref)
	r.CheckedAt = c.At
	if r.Status.IsTerminal() {
		r.Stale = false
		return
	}
	if c.Status != "" && !c.Status.IsTerminal() {
		r.Status = c.Status
	}
	r.Stale = c.Stale
}

// MergeRecords joins sched, a record kept under a ScheduledID, into run,
// the record of the same run kept under its RunID, when a write reveals
// both IDs. run's fields win; sched fills what run lacks, the enqueue time
// is the earliest, and the steps of both are kept.
func MergeRecords(run, sched RunRecord) RunRecord {
	out := run.Clone()
	fillRef(&out, sched.Ref)
	fillIdentity(&out, sched.Workflow, sched.Queue, sched.Tenant, sched.Labels, sched.Source, sched.SourceRef)
	if out.Payload == nil {
		out.Payload = slices.Clone(sched.Payload)
	}
	if out.PayloadSize == 0 {
		out.PayloadSize = sched.PayloadSize
	}
	if out.Priority == 0 {
		out.Priority = sched.Priority
	}
	if out.RunAt.IsZero() {
		out.RunAt = sched.RunAt
	}
	out.EnqueuedAt = earliest(out.EnqueuedAt, sched.EnqueuedAt)
	if !out.OutcomeSet && sched.OutcomeSet {
		out.Outcome, out.OutcomeNote, out.OutcomeSet = sched.Outcome, sched.OutcomeNote, true
	}
	if sched.UpdatedAt.After(out.UpdatedAt) {
		out.UpdatedAt = sched.UpdatedAt
	}
	for _, s := range sched.Steps {
		out.Steps = append(out.Steps, s.Clone())
	}
	SortSteps(out.Steps)
	return out
}

// SortSteps orders step records by start time, then step name, then
// attempt, the order RunRecord.Steps documents.
func SortSteps(steps []StepRecord) {
	slices.SortStableFunc(steps, func(a, b StepRecord) int {
		if c := a.StartedAt.Compare(b.StartedAt); c != 0 {
			return c
		}
		if a.Step != b.Step {
			if a.Step < b.Step {
				return -1
			}
			return 1
		}
		return a.Attempt - b.Attempt
	})
}
