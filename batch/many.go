package batch

import (
	"context"
	"errors"
	"fmt"

	"github.com/ubgo/workflow"
)

// ManyEnqueuer is an optional interface a workflow.Client implements to
// start several jobs in one call of its own (one transaction on a SQL
// store, one bulk request on an engine), which EnqueueMany prefers over
// every other path. It keeps EnqueueMany's contract: one RunRef per job in
// order, and on an error either nil refs or len(jobs) refs with a zero
// RunRef for each job that did not start.
type ManyEnqueuer interface {
	EnqueueMany(ctx context.Context, jobs []workflow.Job) ([]workflow.RunRef, error)
}

// bulkGroup is the group name EnqueueMany passes to a GroupRunner; it
// appears only in the engine's error messages, since a bulk enqueue
// tracks no batch.
const bulkGroup = "bulk"

// EnqueueMany starts jobs on c without batch tracking: no batch labels, no Store, no callbacks. It picks, in order:
//
//   - c's own EnqueueMany when c is a ManyEnqueuer;
//   - one RunGroup call when c is a GroupRunner and no job has RunAt or an
//     IdempotencyKey (a GroupRunner that answers ErrUnsupported falls
//     through to the loop);
//   - one Enqueue per job, in order, stopping at the first error.
//
// It returns one RunRef per job, in order. On an error the refs it
// returns has len(jobs) entries with a zero RunRef for each job that did
// not start, so the caller can cancel or retry exactly those; the error
// of the loop names the first job that failed. No jobs is a no-op.
//
// A client wrapper (workflow.Use, middleware.WithCodec, a Recorder)
// implements neither interface, so EnqueueMany through it is the loop and
// every job passes the wrapper's middleware.
func EnqueueMany(ctx context.Context, c workflow.Client, jobs []workflow.Job) ([]workflow.RunRef, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	if me, ok := c.(ManyEnqueuer); ok {
		refs, err := me.EnqueueMany(ctx, jobs)
		return placed(refs, len(jobs), err)
	}
	if gr, ok := c.(GroupRunner); ok && groupEligible(jobs) {
		refs, err := gr.RunGroup(ctx, bulkGroup, jobs)
		if !errors.Is(err, workflow.ErrUnsupported) {
			return placed(refs, len(jobs), err)
		}
	}
	refs := make([]workflow.RunRef, len(jobs))
	for i, job := range jobs {
		ref, err := c.Enqueue(ctx, job)
		if err != nil {
			return refs, fmt.Errorf("batch: EnqueueMany: job %d of %d (workflow %q): %w", i, len(jobs), job.Workflow, err)
		}
		refs[i] = ref
	}
	return refs, nil
}

// placed normalises a bulk call's refs to len n: a result of another
// length cannot be matched to its jobs, so it becomes all zero (and an
// error when there was none).
func placed(refs []workflow.RunRef, n int, err error) ([]workflow.RunRef, error) {
	if len(refs) == n {
		return refs, err
	}
	if err == nil {
		err = fmt.Errorf("batch: EnqueueMany: the client returned %d refs for %d jobs", len(refs), n)
	}
	return make([]workflow.RunRef, n), err
}
