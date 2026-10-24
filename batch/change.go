package batch

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/ubgo/workflow"
)

// ErrClosed is returned by Manager.Add for a batch that has finished or
// was cancelled: nothing would track the new jobs' ends to any callback.
var ErrClosed = errors.New("batch: the batch has finished or was cancelled")

// ErrDispatching is returned by Manager.Add and Manager.RetryFailed while
// another dispatch of the batch (Dispatch, Add, RetryFailed or a resumed
// dispatch) has not finished. It is transient: try again later, for
// example by returning it from the step that called Add so the step's
// retry policy repeats the call.
var ErrDispatching = errors.New("batch: a dispatch of this batch is in progress")

// WithRetryableJobs keeps every job's spec (workflow, payload, labels) in
// the batch's State for the batch's whole life, which Manager.RetryFailed
// needs to start a failed job again. It implies what
// WithResumableDispatch does, and costs the same: each State write
// carries every job's payload, so keep payloads small (a claim check
// reference rather than the data) in a batch that uses it.
func WithRetryableJobs() Option { return func(m *Manager) { m.retainSpecs = true } }

// dropSpec forgets the kept job specs once a dispatch no longer needs
// them, unless WithRetryableJobs keeps them for RetryFailed.
func (m *Manager) dropSpec(s *State) {
	if !m.retainSpecs {
		s.Spec = nil
	}
}

// claimChange runs inside Store.Update: it takes the dispatch lease for a
// change that starts more jobs (Add, RetryFailed), refusing a coordinated
// batch, whose jobs only a GroupSpawner starts, and a batch whose
// dispatch has not finished. A resumer finishes the new dispatch if this
// process dies part-way, as for Dispatch.
func (m *Manager) claimChange(s *State, owner, what string) error {
	if s.Coordinated {
		return &workflow.UnsupportedError{Field: "batch." + what, Reason: "batch " + string(s.ID) + " runs as one coordinator run (GroupSpawner), which starts every job itself"}
	}
	if !s.Dispatched {
		return fmt.Errorf("batch %s: %s: %w", s.ID, what, ErrDispatching)
	}
	now := m.clk.Now()
	s.Dispatched, s.Grouped = false, false
	s.DispatchOwner, s.DispatchUntil = owner, now.Add(m.dispatchLease)
	return nil
}

// Add adds jobs to a batch that is still running: a job of the batch can fan out further
// work, and Then, Catch and Finally wait for the new jobs too. The jobs
// are labelled and started as Dispatch starts its own (one group on a
// GroupRunner client when eligible), take the next indexes, and count in
// Progress from now on.
//
// It refuses jobs with a label under workflow.LabelPrefixBatch
// (workflow.ErrInvalid), a batch that has finished or was cancelled
// (ErrClosed), one whose dispatch is still running (ErrDispatching, try
// again), and a coordinated batch (an UnsupportedError). An unknown ID
// returns an error wrapping workflow.ErrNotFound. Adding no jobs is a
// no-op.
//
// When an Enqueue fails, the new jobs not yet started are recorded as
// cancelled and the batch is cancelled, exactly as for a Dispatch that
// fails part-way, and the error is returned.
func (m *Manager) Add(ctx context.Context, id ID, jobs ...workflow.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	if err := checkJobs(string(id), "added job ", jobs); err != nil {
		return err
	}
	ownerID, err := newID()
	if err != nil {
		return err
	}
	owner := string(ownerID)
	var spec []workflow.Job
	var idxs []int
	_, err = m.store.Update(ctx, id, func(s *State) error {
		if s.Finished || s.Cancelled {
			return fmt.Errorf("batch %s: add jobs: %w", s.ID, ErrClosed)
		}
		if err := m.claimChange(s, owner, "Add"); err != nil {
			return err
		}
		first := len(s.Jobs)
		added := labelJobs(s.ID, first, jobs)
		spec = make([]workflow.Job, first, first+len(added))
		if len(s.Spec) == first {
			copy(spec, s.Spec) // kept by WithRetryableJobs
		}
		spec = append(spec, added...)
		s.Jobs = append(s.Jobs, make([]JobState, len(added))...)
		idxs = make([]int, len(added))
		for k := range idxs {
			idxs[k] = first + k
		}
		s.Spec = nil
		if _, ok := m.client.(GroupRunner); (ok && groupEligible(added)) || m.resumable || m.retainSpecs {
			s.Spec = spec
		}
		return nil
	})
	if err != nil {
		return err
	}
	return m.start(ctx, id, owner, spec, idxs)
}

// RetryFailed starts the failed jobs of a finished batch again under the
// same batch and re-arms its callbacks. It returns how many jobs it started.
//
// The jobs it starts are those that failed and, when a failure cancelled
// the batch (no AllowFailures), also those that cancel stopped, since
// Then cannot run without them. It clears the batch's cancellation and
// its Catch, Then and Finally latches, so Catch fires again on a new
// failure and Then or Finally once the retried jobs end; those callbacks
// carry a new idempotency key (see State.Round). Each retried run carries
// LabelAttempt, and a late end of an earlier run is ignored.
//
// It needs the jobs' specs, so the Manager must be built with
// WithRetryableJobs (an UnsupportedError otherwise). It refuses a batch
// that has not finished and one cancelled by Manager.Cancel or a failed
// dispatch (workflow.ErrInvalid), one whose dispatch is still running
// (ErrDispatching) and a coordinated batch (an UnsupportedError). A batch
// with nothing to retry returns 0 and changes nothing.
func (m *Manager) RetryFailed(ctx context.Context, id ID) (int, error) {
	ownerID, err := newID()
	if err != nil {
		return 0, err
	}
	owner := string(ownerID)
	var spec []workflow.Job
	var idxs []int
	errNothing := errors.New("nothing to retry")
	_, err = m.store.Update(ctx, id, func(s *State) error {
		if !s.Finished {
			return fmt.Errorf("%w: batch %s: RetryFailed needs a finished batch; it still has running jobs", workflow.ErrInvalid, s.ID)
		}
		if s.Cancelled && !s.FailCancelled {
			return fmt.Errorf("%w: batch %s: RetryFailed does not revive a batch cancelled by Cancel or by a failed dispatch", workflow.ErrInvalid, s.ID)
		}
		if len(s.Spec) != len(s.Jobs) {
			return &workflow.UnsupportedError{Field: "batch.RetryFailed", Reason: "batch " + string(s.ID) + " kept no job specs; build the Manager with batch.WithRetryableJobs"}
		}
		idxs = retryable(*s)
		if len(idxs) == 0 {
			return errNothing
		}
		if err := m.claimChange(s, owner, "RetryFailed"); err != nil {
			return err
		}
		s.Round++
		s.Finished, s.Cancelled, s.FailCancelled, s.CatchFired = false, false, false, false
		s.FinishedAt, s.CancelledAt = time.Time{}, time.Time{}
		for _, i := range idxs {
			j := &s.Jobs[i]
			j.Attempt++
			j.Status, j.Ref = "", workflow.RunRef{}
			job := s.Spec[i]
			job.Labels = maps.Clone(job.Labels)
			job.Labels[LabelAttempt] = strconv.Itoa(j.Attempt)
			s.Spec[i] = job
		}
		spec = append([]workflow.Job(nil), s.Spec...)
		return nil
	})
	if errors.Is(err, errNothing) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return len(idxs), m.start(ctx, id, owner, spec, idxs)
}

// retryable returns, in index order, the jobs RetryFailed starts again:
// the failed ones, plus the cancelled ones when a failure cancelled the
// batch.
func retryable(s State) []int {
	var out []int
	for i, j := range s.Jobs {
		if st := s.counted(j.Status); st == workflow.StatusFailed || (s.FailCancelled && st == workflow.StatusCancelled) {
			out = append(out, i)
		}
	}
	return out
}

// Prune deletes the batches f matches (see PruneFilter) from a Store that
// implements Pruner and returns how many it deleted. A Store without Pruner returns an
// UnsupportedError, and a filter with no rule an error wrapping
// workflow.ErrInvalid. Run it on a schedule the app owns; the Manager
// starts no pruning of its own.
func (m *Manager) Prune(ctx context.Context, f PruneFilter) (int, error) {
	if f.IsZero() {
		return 0, fmt.Errorf("%w: batch.PruneFilter has no rule; set FinishedBefore, UnfinishedBefore or CancelledBefore", workflow.ErrInvalid)
	}
	p, ok := m.store.(Pruner)
	if !ok {
		return 0, &workflow.UnsupportedError{Field: "batch.Manager.Prune", Reason: "the batch.Store does not implement batch.Pruner"}
	}
	return p.Prune(ctx, f)
}
