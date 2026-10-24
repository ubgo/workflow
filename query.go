package workflow

import (
	"context"
	"maps"
	"strings"
	"time"
)

// Page sizes of a run query.
const (
	// DefaultRunsLimit is the page size of a RunQuery whose Limit is zero.
	DefaultRunsLimit = 100

	// MaxRunsLimit is the largest Limit a RunQuery may ask for, so one
	// call cannot load an unbounded table into memory.
	MaxRunsLimit = 1000
)

// RunQuery filters runs for RunQuerier.Runs and BulkCanceller.CancelAll.
// Every field that is set must match; the zero value matches every run.
type RunQuery struct {
	// Ref picks one run: every field of Ref that is set must equal the
	// run's, so a RunRef that Enqueue returned for a delayed run (only its
	// ScheduledID) still finds the run after it started. The zero RunRef
	// matches any run. With Ref set, Runs returns zero or
	// one run, which is how an admin surface shows a single run.
	Ref RunRef

	// Workflow is the workflow name; empty matches any.
	Workflow string

	// Queue is the run's queue (EffectiveQueue); empty matches any.
	Queue string

	// Statuses matches runs in any of these statuses; empty matches any
	// (CancelAll gives it its own default).
	Statuses []Status

	// Labels matches runs whose Job.Labels hold every pair exactly.
	Labels map[string]string

	// LabelPrefixes matches runs whose Job.Labels hold every key with a
	// value starting with the given prefix (an empty prefix matches any
	// value under that key). It needs CapLabelPrefix; a driver without it
	// refuses the query (CheckRunQuery) rather than ignoring the filter.
	LabelPrefixes map[string]string

	// From and To bound the enqueue time to [From, To); a zero bound is
	// open.
	From, To time.Time

	// Tenant matches runs whose Job.Tenant equals it; empty matches any.
	// It needs CapTenant; a driver without it refuses the query
	// (CheckRunQuery).
	Tenant string

	// DueFrom and DueTo bound the time a delayed run is due (RunInfo.RunAt)
	// to [DueFrom, DueTo), for "what fires in the next hour". With either
	// set, only runs enqueued with a RunAt match. A zero bound is open.
	// They need CapDueQuery (CheckRunQuery).
	DueFrom, DueTo time.Time

	// Limit is the page size: zero means DefaultRunsLimit, more than
	// MaxRunsLimit is invalid. CancelAll refuses it.
	Limit int

	// Cursor continues a listing from RunPage.Next; empty starts at the
	// newest run. It is opaque and driver-specific. CancelAll refuses it.
	Cursor string
}

// PageLimit is the page size q asks for, after the default.
func (q RunQuery) PageLimit() int {
	if q.Limit == 0 {
		return DefaultRunsLimit
	}
	return q.Limit
}

// Match reports whether a run with these fields passes q's filters
// (everything but paging). In-process drivers use it so every driver
// reads a filter the same way.
func (q RunQuery) Match(info RunInfo) bool {
	if !q.Ref.RunID.IsZero() && info.Ref.RunID != q.Ref.RunID {
		return false
	}
	if !q.Ref.ScheduledID.IsZero() && info.Ref.ScheduledID != q.Ref.ScheduledID {
		return false
	}
	if q.Workflow != "" && info.Workflow != q.Workflow {
		return false
	}
	if q.Queue != "" && info.Queue != q.Queue {
		return false
	}
	if len(q.Statuses) > 0 {
		found := false
		for _, s := range q.Statuses {
			if s == info.Status {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for k, v := range q.Labels {
		if got, ok := info.Labels[k]; !ok || got != v {
			return false
		}
	}
	for k, p := range q.LabelPrefixes {
		if got, ok := info.Labels[k]; !ok || !strings.HasPrefix(got, p) {
			return false
		}
	}
	if !q.From.IsZero() && info.EnqueuedAt.Before(q.From) {
		return false
	}
	if !q.To.IsZero() && !info.EnqueuedAt.Before(q.To) {
		return false
	}
	if q.Tenant != "" && info.Tenant != q.Tenant {
		return false
	}
	if !q.DueFrom.IsZero() || !q.DueTo.IsZero() {
		if info.RunAt.IsZero() {
			return false
		}
		if !q.DueFrom.IsZero() && info.RunAt.Before(q.DueFrom) {
			return false
		}
		if !q.DueTo.IsZero() && !info.RunAt.Before(q.DueTo) {
			return false
		}
	}
	return true
}

// ValidateRunQuery checks q: known statuses, a valid queue name when one
// is set, From before To when both are set, and 0 <= Limit <=
// MaxRunsLimit. Errors wrap ErrInvalid.
func ValidateRunQuery(q RunQuery) error {
	for _, s := range q.Statuses {
		if !s.valid() {
			return invalidf("run query: unknown status %q", s)
		}
	}
	if q.Queue != "" {
		if err := ValidateQueueName(q.Queue); err != nil {
			return err
		}
	}
	if !q.From.IsZero() && !q.To.IsZero() && !q.From.Before(q.To) {
		return invalidf("run query: From %s is not before To %s", q.From, q.To)
	}
	if !q.DueFrom.IsZero() && !q.DueTo.IsZero() && !q.DueFrom.Before(q.DueTo) {
		return invalidf("run query: DueFrom %s is not before DueTo %s", q.DueFrom, q.DueTo)
	}
	if q.Tenant != "" {
		if err := ValidateTenant(q.Tenant); err != nil {
			return err
		}
	}
	if q.Limit < 0 || q.Limit > MaxRunsLimit {
		return invalidf("run query: Limit %d is outside [0, %d]", q.Limit, MaxRunsLimit)
	}
	return nil
}

// CheckRunQuery refuses what q asks for that have lacks: LabelPrefixes
// without CapLabelPrefix, Tenant without CapTenant, DueFrom or DueTo
// without CapDueQuery, as an *UnsupportedError (matches ErrUnsupported)
// naming the field. Every RunQuerier and BulkCanceller calls it after
// ValidateRunQuery, so no driver silently drops a filter.
func CheckRunQuery(have Caps, q RunQuery) error {
	if len(q.LabelPrefixes) > 0 && !have.Has(CapLabelPrefix) {
		return &UnsupportedError{Field: "RunQuery.LabelPrefixes", Missing: CapLabelPrefix}
	}
	if q.Tenant != "" && !have.Has(CapTenant) {
		return &UnsupportedError{Field: "RunQuery.Tenant", Missing: CapTenant}
	}
	if (!q.DueFrom.IsZero() || !q.DueTo.IsZero()) && !have.Has(CapDueQuery) {
		return &UnsupportedError{Field: "RunQuery.DueFrom", Missing: CapDueQuery}
	}
	return nil
}

// ValidateCancelQuery checks q for CancelAll: ValidateRunQuery, no
// terminal status, and no Limit or Cursor (a bulk cancel covers every
// match, so paging it would be ambiguous). Errors wrap ErrInvalid.
func ValidateCancelQuery(q RunQuery) error {
	if err := ValidateRunQuery(q); err != nil {
		return err
	}
	for _, s := range q.Statuses {
		if s.IsTerminal() {
			return invalidf("cancel query: status %q is terminal; a finished run cannot be cancelled", s)
		}
	}
	if q.Limit != 0 || q.Cursor != "" {
		return invalidf("cancel query: Limit and Cursor are not used by CancelAll")
	}
	return nil
}

// CancelStatuses is the statuses CancelAll cancels: q.Statuses, or, when
// empty, the runs not yet started (StatusScheduled and StatusQueued):
// clearing a backlog must not cut short work that has already begun.
func CancelStatuses(q RunQuery) []Status {
	if len(q.Statuses) > 0 {
		return q.Statuses
	}
	return []Status{StatusScheduled, StatusQueued}
}

// RunInfo is one run as a RunQuerier returns it.
type RunInfo struct {
	// Ref is the run.
	Ref RunRef

	// Workflow is the workflow name.
	Workflow string

	// Queue is the run's queue (EffectiveQueue).
	Queue string

	// Status is where the run is now.
	Status Status

	// Error is why the run failed, was discarded or was cancelled; while
	// it is StatusRetrying (CapRunStates), the error of the failed attempt
	// that waits on its backoff; empty otherwise. It is a string because
	// an engine stores only the message.
	Error string

	// FailedStep is the step whose failure ended the run; empty otherwise.
	FailedStep string

	// Labels are the run's Job.Labels. Callers must not modify them.
	Labels map[string]string

	// Payload is the job's payload as the driver stored it (after any
	// codec or claim check ran).
	Payload []byte

	// Priority is Job.Priority.
	Priority int

	// EnqueuedAt is when Enqueue accepted the run, on the driver's clock.
	EnqueuedAt time.Time

	// StartedAt is when the first step attempt started; zero before.
	StartedAt time.Time

	// FinishedAt is when the run became terminal; zero before.
	FinishedAt time.Time

	// Attempts counts the step attempts made across the run.
	Attempts int

	// NextAttempt is the attempt number a step waiting on its retry
	// backoff will run as (2 for the first retry), and NextAttemptAt
	// when it becomes ready; with several steps in backoff, the one
	// ready first. Both are zero unless the run is StatusRetrying, and
	// are filled only by drivers with CapRunStates.
	NextAttempt   int
	NextAttemptAt time.Time

	// Errors is the run's error history: one entry per failed step
	// attempt, oldest first, bounded by the driver's error-history
	// option. Filled only by drivers with CapErrorHistory.
	Errors []AttemptError

	// Tenant is Job.Tenant (CapTenant).
	Tenant string

	// RunAt is the time a delayed run is (or was) due: Job.RunAt, or the
	// time Reschedule moved it to. Zero for a run enqueued to start now.
	// Filled by drivers with CapDueQuery.
	RunAt time.Time

	// Output is the run's result (RunOutput) once it succeeded; nil
	// before and for other ends. Filled by drivers with CapResult.
	Output []byte

	// Progress is the latest progress a step reported (ReportProgress);
	// nil when none was reported or the driver keeps none.
	Progress *Progress

	// AttemptedBy lists the nodes that ran the run's step attempts, in
	// the order each first ran one, without repeats. Filled by drivers
	// with CapNodeIdentity.
	AttemptedBy []string
}

// Clone returns a copy of info whose Labels and Payload are its own, so a
// driver can hand it out without sharing its state.
func (info RunInfo) Clone() RunInfo {
	info.Labels = maps.Clone(info.Labels)
	if info.Payload != nil {
		info.Payload = append([]byte(nil), info.Payload...)
	}
	if info.Errors != nil {
		info.Errors = append([]AttemptError(nil), info.Errors...)
	}
	if info.Output != nil {
		info.Output = append([]byte(nil), info.Output...)
	}
	if info.Progress != nil {
		p := info.Progress.Clone()
		info.Progress = &p
	}
	if info.AttemptedBy != nil {
		info.AttemptedBy = append([]string(nil), info.AttemptedBy...)
	}
	return info
}

// RunPage is one page of a run listing, newest enqueue first (ties broken
// by RunID, descending). Next is empty on the last page.
type RunPage struct {
	Runs []RunInfo
	Next string
}

// RunQuerier lists runs, failed ones included, for an operator's view.
// A client implements it when it declares CapRunQuery. Errors: an invalid
// query wraps ErrInvalid; a cursor the driver did not issue wraps
// ErrInvalid too.
type RunQuerier interface {
	Runs(ctx context.Context, q RunQuery) (RunPage, error)
}

// RunRetrier re-runs a failed run from the step that failed, without
// re-running the steps that already succeeded.
// A client implements it when it declares CapRetryRun.
//
// Steps that succeeded keep their outputs and do not run again; the step
// that failed, and every step the failure cancelled, start again with
// fresh attempt counts. The run keeps its RunRef, goes back to
// StatusQueued and, when it ends again, is reported again to run-finished
// observers (which already tolerate repeats). Its idempotency key is taken
// again; when another unfinished run holds it, Retry returns ErrDuplicate.
// A run that is not StatusFailed returns ErrWrongState; an unknown ref
// returns ErrNotFound.
type RunRetrier interface {
	Retry(ctx context.Context, ref RunRef) (RunRef, error)
}

// RunDeleter forgets a terminal run: afterwards
// Status returns ErrNotFound and Runs no longer lists it. A client
// implements it when it declares CapDeleteRun. A run that is not terminal
// returns ErrWrongState; an unknown ref returns ErrNotFound. Forgetting
// many runs is Runs plus Delete, or a driver's retention.
type RunDeleter interface {
	Delete(ctx context.Context, ref RunRef) error
}

// BulkCanceller cancels every run matching a query, for example to clear
// a queue's backlog. A client implements it when it declares
// CapCancelAll.
//
// The statuses cancelled are CancelStatuses(q): by default only runs not
// yet started; running and waiting runs only when listed. Each run is
// cancelled exactly as Cancel would cancel it (its key released, its
// run-finished event delivered). q must pass ValidateCancelQuery. It
// returns how many runs it cancelled.
type BulkCanceller interface {
	CancelAll(ctx context.Context, q RunQuery) (int, error)
}
