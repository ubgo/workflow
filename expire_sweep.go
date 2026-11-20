package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ExpireResult is what one ExpireWaiting sweep did.
type ExpireResult struct {
	// Checked is how many not-yet-started runs the sweep looked at.
	Checked int

	// Cancelled lists the runs it cancelled, in listing order.
	Cancelled []RunRef

	// Raced counts runs that were over the bound when listed but had
	// moved on by the time the sweep came to cancel them (a worker took
	// it, it ended, or a delayed run became queued), so they were left
	// alone; a run still waiting is found again by the next sweep. Only a
	// client with CapCancelAll can tell; on any other it stays zero.
	Raced int
}

// ExpireWaiting cancels every run matching q that has waited longer than
// maxWait to start, measured at now: the schedule-to-start timeout for
// the case where no worker ever takes the run (every worker of its queue
// down, a queue nobody serves), which a step middleware cannot see
// because no step ever runs. Call it from a schedule or a ticker with
// the clock the app uses.
//
// What counts as waiting: a run in StatusQueued (waiting for a worker),
// or in StatusScheduled whose due time has passed. Its wait is measured
// from the later of RunInfo.EnqueuedAt and RunInfo.RunAt, so a delayed
// run starts waiting when it becomes due. RunAt is filled only by drivers
// with CapDueQuery: on a driver without it a delayed run that became
// queued is measured from its enqueue, which overstates its wait (it may
// be cancelled up to its delay early), and a StatusScheduled run is never
// cancelled, since its due time is unknown. q narrows the sweep by
// workflow, queue, tenant or labels; q.Statuses may name only
// StatusQueued and StatusScheduled (empty means both), and q.To is
// lowered to now minus maxWait, because no run enqueued later can have
// waited that long, which keeps each sweep to the runs that may qualify.
// Every page is read (q.Limit is the page size, default MaxRunsLimit).
//
// Each run is cancelled the way the client allows. With CapCancelAll and
// workflow.BulkCanceller, through CancelAll on that run's Ref and its
// listed status, so the driver cancels it only while it still has not
// started; a run a worker took between the listing and the cancel is
// counted in Raced, never cut short. Without it, through Client.Cancel,
// which cancels whatever state the run is in by then: a run a worker took
// in that instant is cancelled while running. A run that is gone
// (ErrNotFound) is skipped. A failed cancel does not stop the sweep: the
// errors are joined and returned with the result so far.
//
// It needs CapRunQuery (and workflow.RunQuerier) and CapCancel, and
// refuses with an UnsupportedError naming what is missing: memory,
// sqlstore, Redis and Hatchet have them all, CapCancelAll and
// CapDueQuery included.
// A maxWait of zero or less is refused with ErrInvalid, as is an invalid
// q.
func ExpireWaiting(ctx context.Context, c Client, q RunQuery, maxWait time.Duration, now time.Time) (ExpireResult, error) {
	var res ExpireResult
	if maxWait <= 0 {
		return res, invalidf("ExpireWaiting needs a positive maxWait, got %s", maxWait)
	}
	caps := c.Capabilities()
	if missing := caps.Missing(CapsOf(CapRunQuery, CapCancel)); !missing.IsZero() {
		return res, &UnsupportedError{Field: "ExpireWaiting", Missing: missing}
	}
	rq, ok := c.(RunQuerier)
	if !ok {
		return res, &UnsupportedError{Field: "ExpireWaiting", Missing: CapRunQuery, Reason: "the client does not implement workflow.RunQuerier"}
	}
	var bulk BulkCanceller
	if b, ok := c.(BulkCanceller); ok && caps.Has(CapCancelAll) {
		bulk = b
	}

	if len(q.Statuses) == 0 {
		q.Statuses = []Status{StatusQueued, StatusScheduled}
	}
	for _, s := range q.Statuses {
		if s != StatusQueued && s != StatusScheduled {
			return res, invalidf("ExpireWaiting: status %q has started; only %q and %q wait to start", s, StatusQueued, StatusScheduled)
		}
	}
	cutoff := now.Add(-maxWait)
	if q.To.IsZero() || q.To.After(cutoff) {
		q.To = cutoff
	}
	if !q.From.IsZero() && !q.From.Before(q.To) {
		return res, nil // the window holds no run old enough
	}
	if q.Limit == 0 {
		q.Limit = MaxRunsLimit
	}
	if err := ValidateRunQuery(q); err != nil {
		return res, err
	}

	var waiting []RunInfo
	for {
		page, err := rq.Runs(ctx, q)
		if err != nil {
			return res, fmt.Errorf("workflow: ExpireWaiting list: %w", err)
		}
		for _, info := range page.Runs {
			res.Checked++
			if overdue(info, maxWait, now) {
				waiting = append(waiting, info)
			}
		}
		if page.Next == "" {
			break
		}
		q.Cursor = page.Next
	}

	var errs []error
	for _, info := range waiting {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		cancelled, err := cancelWaiting(ctx, c, bulk, info)
		switch {
		case errors.Is(err, ErrNotFound):
		case err != nil:
			errs = append(errs, fmt.Errorf("workflow: ExpireWaiting cancel %+v: %w", info.Ref, err))
		case cancelled:
			res.Cancelled = append(res.Cancelled, info.Ref)
		default:
			res.Raced++
		}
	}
	return res, errors.Join(errs...)
}

// overdue reports whether info has waited more than maxWait to start at
// now, measured from the later of its enqueue and its due time.
func overdue(info RunInfo, maxWait time.Duration, now time.Time) bool {
	if info.Status != StatusQueued && info.Status != StatusScheduled {
		return false
	}
	if info.Status == StatusScheduled && info.RunAt.IsZero() {
		return false // a delayed run whose due time this driver does not report
	}
	since := info.EnqueuedAt
	if info.RunAt.After(since) {
		since = info.RunAt
	}
	return now.Sub(since) > maxWait
}

// cancelWaiting cancels one run: through bulk, guarded by its listed
// status, when the client offers it, else through Cancel.
func cancelWaiting(ctx context.Context, c Client, bulk BulkCanceller, info RunInfo) (bool, error) {
	if bulk == nil {
		return true, c.Cancel(ctx, info.Ref)
	}
	n, err := bulk.CancelAll(ctx, RunQuery{Ref: info.Ref, Statuses: []Status{info.Status}})
	return n > 0, err
}
