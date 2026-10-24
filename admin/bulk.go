package admin

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ubgo/workflow"
)

// DefaultMaxBulk bounds how many runs one RetryAll or DeleteAll call acts
// on, so one HTTP request does a bounded amount of work; a caller with
// more matches repeats the call (BulkResult.Truncated says so). It is a
// floor on request cost, not on what an operator can clear: each repeat
// takes the next runs, because the ones acted on no longer match.
const DefaultMaxBulk = 1000

// BulkSample is how many matched refs a BulkResult carries, so a dry run
// shows which runs it would touch without sending thousands of refs.
const BulkSample = 20

// WithMaxBulk bounds how many runs one RetryAll or DeleteAll call acts
// on. Zero or less keeps DefaultMaxBulk.
func WithMaxBulk(n int) Option { return func(a *Admin) { a.maxBulk = n } }

// BulkOptions shape one bulk call.
type BulkOptions struct {
	// DryRun lists what the call would act on and changes nothing.
	DryRun bool
	// Max lowers the Admin's bound (WithMaxBulk) for this call; zero, or a
	// value above that bound, keeps the bound.
	Max int
}

// BulkResult is what a bulk call matched and did.
type BulkResult struct {
	// Matched is how many runs the filter matched, up to the bound.
	Matched int
	// Done is how many runs were retried or deleted; zero on a dry run.
	Done int
	// Skipped is how many matched runs were left alone because they
	// changed between the listing and the action: no longer in a state
	// the action accepts (workflow.ErrWrongState), gone
	// (workflow.ErrNotFound), or, for a retry, their idempotency key held
	// by another unfinished run (workflow.ErrDuplicate).
	Skipped int
	// Truncated is true when more runs match than the bound: repeat the
	// call for the rest.
	Truncated bool
	// Sample is the first BulkSample matched refs, newest first.
	Sample []workflow.RunRef
}

// retryStatuses is the one status Retry accepts (workflow.RunRetrier).
var retryStatuses = []workflow.Status{workflow.StatusFailed}

// RetryAll retries every failed run q matches, from the step that failed,
// as Retry does one; see workflow.RunRetrier. q.Statuses must be empty or
// exactly [failed], since Retry refuses every other status; an empty
// filter retries every failed run, up to the bound. It needs OpRetryAll:
// RunQuerier to list and RunRetrier to act. The runs are listed first and
// then retried one by one, so a run that changes in between is counted
// in Skipped; any other error stops the call and is returned with the
// result so far.
func (a *Admin) RetryAll(ctx context.Context, q workflow.RunQuery, opts BulkOptions) (BulkResult, error) {
	if err := a.check(OpRetryAll); err != nil {
		return BulkResult{}, err
	}
	if len(q.Statuses) > 0 && !slices.Equal(q.Statuses, retryStatuses) {
		return BulkResult{}, fmt.Errorf("%w: admin: retry_all acts on failed runs only; leave statuses empty or [failed]", workflow.ErrInvalid)
	}
	q.Statuses = retryStatuses
	rt := a.c.(workflow.RunRetrier)
	return a.bulk(ctx, q, opts, func(ctx context.Context, ref workflow.RunRef) error {
		_, err := rt.Retry(ctx, ref)
		return err
	})
}

// DeleteAll forgets every terminal run q matches, as Delete does one. The
// filter must name at least one field (workflow, queue, statuses, labels,
// label prefixes, from or to), so "delete everything" is never an empty
// request: an operator who means every finished run says
// statuses=succeeded,failed,cancelled. q.Statuses may list terminal
// statuses only; empty means every terminal status the client reports
// (discarded only with CapRunStates). It needs OpDeleteAll: RunQuerier and
// RunDeleter. Runs that changed between listing and deleting are Skipped;
// any other error stops the call.
func (a *Admin) DeleteAll(ctx context.Context, q workflow.RunQuery, opts BulkOptions) (BulkResult, error) {
	if err := a.check(OpDeleteAll); err != nil {
		return BulkResult{}, err
	}
	if q.Workflow == "" && q.Queue == "" && len(q.Statuses) == 0 && len(q.Labels) == 0 && len(q.LabelPrefixes) == 0 && q.From.IsZero() && q.To.IsZero() {
		return BulkResult{}, fmt.Errorf("%w: admin: delete_all needs a filter; to forget every finished run, list the statuses", workflow.ErrInvalid)
	}
	for _, s := range q.Statuses {
		if !s.IsTerminal() {
			return BulkResult{}, fmt.Errorf("%w: admin: delete_all: status %q is not terminal; only finished runs can be deleted", workflow.ErrInvalid, s)
		}
	}
	if len(q.Statuses) == 0 {
		q.Statuses = []workflow.Status{workflow.StatusSucceeded, workflow.StatusFailed, workflow.StatusCancelled}
		if a.c.Capabilities().Has(workflow.CapRunStates) {
			q.Statuses = append(q.Statuses, workflow.StatusDiscarded)
		}
	}
	del := a.c.(workflow.RunDeleter)
	return a.bulk(ctx, q, opts, del.Delete)
}

// PreviewCancelAll is CancelAll's dry run: it counts the runs CancelAll
// would cancel (workflow.CancelStatuses) and samples them, changing
// nothing. It needs OpCancelAll and OpRuns. The count is a moment's
// snapshot: runs that start before the real call are no longer cancelled
// by its default statuses.
func (a *Admin) PreviewCancelAll(ctx context.Context, q workflow.RunQuery) (BulkResult, error) {
	if err := a.check(OpCancelAll); err != nil {
		return BulkResult{}, err
	}
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return BulkResult{}, err
	}
	if err := a.check(OpRuns); err != nil {
		return BulkResult{}, err
	}
	q.Statuses = workflow.CancelStatuses(q)
	refs, truncated, err := a.collect(ctx, q, a.maxScan)
	if err != nil {
		return BulkResult{}, err
	}
	return BulkResult{Matched: len(refs), Truncated: truncated, Sample: sample(refs)}, nil
}

// bulk lists the runs q matches up to the bound, then applies act to each
// unless opts.DryRun. Listing first and acting second keeps paging stable:
// a delete or retry changes the set a cursor walks.
func (a *Admin) bulk(ctx context.Context, q workflow.RunQuery, opts BulkOptions, act func(context.Context, workflow.RunRef) error) (BulkResult, error) {
	if q.Cursor != "" || q.Limit != 0 {
		return BulkResult{}, fmt.Errorf("%w: admin: a bulk call covers every match; Limit and Cursor are not used", workflow.ErrInvalid)
	}
	if opts.Max < 0 {
		return BulkResult{}, fmt.Errorf("%w: admin: bulk Max %d is negative", workflow.ErrInvalid, opts.Max)
	}
	if err := a.validateQuery(q); err != nil {
		return BulkResult{}, err
	}
	bound := a.maxBulk
	if opts.Max > 0 && opts.Max < bound {
		bound = opts.Max
	}
	refs, truncated, err := a.collect(ctx, q, bound)
	if err != nil {
		return BulkResult{}, err
	}
	res := BulkResult{Matched: len(refs), Truncated: truncated, Sample: sample(refs)}
	if opts.DryRun {
		return res, nil
	}
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		err := act(ctx, ref)
		switch {
		case err == nil:
			res.Done++
		case skippable(err):
			res.Skipped++
		default:
			return res, err
		}
	}
	return res, nil
}

// skippable reports whether a per-run error means the run changed under
// the bulk call, rather than that the call itself is failing.
func skippable(err error) bool {
	return errors.Is(err, workflow.ErrWrongState) || errors.Is(err, workflow.ErrNotFound) || errors.Is(err, workflow.ErrDuplicate)
}

// collect pages RunQuerier over q and returns at most bound refs, and
// whether more matched.
func (a *Admin) collect(ctx context.Context, q workflow.RunQuery, bound int) ([]workflow.RunRef, bool, error) {
	rq := a.c.(workflow.RunQuerier)
	q.Limit = min(workflow.MaxRunsLimit, bound+1)
	var refs []workflow.RunRef
	for {
		page, err := rq.Runs(ctx, q)
		if err != nil {
			return nil, false, err
		}
		for _, r := range page.Runs {
			if len(refs) == bound {
				return refs, true, nil
			}
			refs = append(refs, r.Ref)
		}
		if page.Next == "" {
			return refs, false, nil
		}
		q.Cursor = page.Next
	}
}

// sample is the first BulkSample refs.
func sample(refs []workflow.RunRef) []workflow.RunRef {
	return slices.Clone(refs[:min(len(refs), BulkSample)])
}
