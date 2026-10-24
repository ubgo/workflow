package batch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// DefaultDispatchLease is how long a Dispatch may go without writing to
// the Store before ResumeDispatches treats its process as dead and takes
// the batch over. It must exceed the longest single Enqueue or RunGroup
// call, since the lease is renewed between calls, and the engine's run
// list lag, so a RunFinder sees every run the dead process started.
const DefaultDispatchLease = 5 * time.Minute

// WithDispatchLease sets the dispatch lease; see DefaultDispatchLease for
// what it must exceed. A value that is not positive keeps the default.
func WithDispatchLease(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.dispatchLease = d
		}
	}
}

// WithResumableDispatch keeps every batch's job specs in its State until
// its dispatch finishes on the one-Enqueue-per-job path too, so a resumer
// can start the jobs a dead process never started. The GroupRunner path
// keeps them anyway. The cost is that each saved ref rewrites the State
// with every job's payload, so a dispatch of n jobs writes O(n²) payload
// bytes to the Store; without it, those never-started jobs are recorded
// as cancelled instead.
func WithResumableDispatch() Option { return func(m *Manager) { m.resumable = true } }

// RunFinder is an optional interface a workflow.Client implements to list
// the runs that exist for a batch's jobs, found by their LabelBatch label
// whatever their status, keyed by their LabelIndex. ResumeDispatches uses
// it to tell which jobs of a dispatch that died part-way had started, so
// it can save their refs and start only the others. since is the batch's
// CreatedAt; an implementation widens it for clock skew as it needs.
// A delayed start that has not fired need not be found, and a job with a
// RunAt is therefore never restarted on the finder's word alone.
type RunFinder interface {
	FindBatchRuns(ctx context.Context, id ID, since time.Time) (map[int]workflow.RunRef, error)
}

// ResumeDispatches finishes the dispatch of every open batch whose
// dispatch did not finish and whose lease has ended, which means the
// process dispatching it died or stalled past the lease. The status
// poller, the callback retry loop and the Hatchet BatchSource call it on
// each tick; an app with neither calls it itself.
//
// For each such batch it takes the lease and looks at the jobs that are
// not finished and have no ref:
//
//   - one a RunFinder (on the client) lists as started gets its ref saved;
//   - one known never to have started is started again, when its spec was
//     kept (see WithResumableDispatch) and the batch is not cancelled;
//   - every other one is recorded as cancelled, with the reason logged.
//
// A job is known never to have started when the finder does not list it
// and it has no RunAt, or, without a finder, when it comes after the first
// job without a ref on the one-Enqueue-per-job path (which saves each ref
// before the next Enqueue). The first such job, and after a GroupRunner
// call every such job, may have started with its ref lost, and is
// cancelled rather than risk running it twice; if it did run, its end is
// then ignored, since a job's first recorded status wins. A cancelled
// batch starts nothing: its unstarted jobs are recorded as cancelled and
// the found runs are cancelled.
func (m *Manager) ResumeDispatches(ctx context.Context) error {
	open, err := m.store.Unfinished(ctx)
	if err != nil {
		return fmt.Errorf("batch: list batches to resume: %w", err)
	}
	now := m.clk.Now()
	var errs []error
	for _, st := range open {
		if st.Dispatched || now.Before(st.DispatchUntil) {
			continue
		}
		errs = append(errs, m.resume(ctx, st.ID))
	}
	return errors.Join(errs...)
}

// resume takes over one batch's dispatch; see ResumeDispatches.
func (m *Manager) resume(ctx context.Context, id ID) error {
	ownerID, err := newID()
	if err != nil {
		return err
	}
	owner := string(ownerID)
	claimed := false
	st, err := m.store.Update(ctx, id, func(s *State) error {
		now := m.clk.Now()
		if s.Dispatched || now.Before(s.DispatchUntil) {
			return nil
		}
		claimed = true
		s.DispatchOwner, s.DispatchUntil = owner, now.Add(m.dispatchLease)
		return nil
	})
	if err != nil || !claimed {
		return err
	}
	if st.Coordinated {
		return m.resumeSpawn(ctx, id, owner, st)
	}
	var unstarted []int
	for i, j := range st.Jobs {
		if j.Ref.IsZero() && !j.Status.IsTerminal() {
			unstarted = append(unstarted, i)
		}
	}
	if len(unstarted) == 0 {
		return m.finishDispatch(ctx, id, owner)
	}
	finder, hasFinder := m.client.(RunFinder)
	var found map[int]workflow.RunRef
	if hasFinder {
		if found, err = finder.FindBatchRuns(ctx, id, st.CreatedAt); err != nil {
			// The lease stays taken, so the next try comes after it ends.
			return fmt.Errorf("batch %s: resume: find runs: %w", id, err)
		}
	}
	hasSpec := len(st.Spec) == len(st.Jobs)
	var start, cancel []int
	foundRefs := map[int]workflow.RunRef{}
	for k, i := range unstarted {
		// A finder lists runs by index only, so for a job RetryFailed
		// started again it may list the earlier attempt's run: neither its
		// listing nor its silence says anything about the new attempt.
		retried := st.Jobs[i].Attempt > 0
		if ref, ok := found[i]; ok && !retried {
			foundRefs[i] = ref
			continue
		}
		neverStarted := !st.Grouped && k > 0
		if hasFinder && !retried && (!hasSpec || st.Spec[i].RunAt.IsZero()) {
			neverStarted = true
		}
		if neverStarted && hasSpec && !st.Cancelled {
			start = append(start, i)
			continue
		}
		cancel = append(cancel, i)
	}
	saved, err := m.store.Update(ctx, id, func(s *State) error {
		if err := m.renew(s, owner); err != nil {
			return err
		}
		for i, ref := range foundRefs {
			if s.Jobs[i].Ref.IsZero() {
				s.Jobs[i].Ref = ref
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("batch %s: resume: save found refs: %w", id, err)
	}
	if saved.Cancelled {
		var errs []error
		for i := range foundRefs {
			errs = append(errs, m.cancelRun(ctx, saved.Jobs[i]))
		}
		cancel = append(cancel, start...)
		start = nil
		if err := errors.Join(errs...); err != nil {
			return err
		}
	}
	if len(cancel) > 0 {
		m.log.WarnContext(ctx, "batch: resumed dispatch records jobs as cancelled", "batch", string(id), "jobs", cancel,
			"why", "the batch is cancelled, or the job may have started with its ref lost, or its spec was not kept (WithResumableDispatch)")
		if err := m.recordUnstarted(ctx, id, owner, cancel); err != nil {
			return err
		}
	}
	return m.start(ctx, id, owner, st.Spec, start)
}

// recordUnstarted records the jobs at idxs that still have no ref and no
// status as cancelled, under owner's lease, and carries out the effects.
func (m *Manager) recordUnstarted(ctx context.Context, id ID, owner string, idxs []int) error {
	var eff effects
	var claimed []PendingCallback
	st, err := m.store.Update(ctx, id, func(s *State) error {
		if err := m.renew(s, owner); err != nil {
			return err
		}
		now := m.clk.Now()
		for _, i := range idxs {
			if !s.Jobs[i].Ref.IsZero() {
				continue
			}
			e := recordFinish(s, i, workflow.StatusCancelled, now)
			eff.then, eff.finally = eff.then || e.then, eff.finally || e.finally
		}
		claimed = m.claimCallbacks(s, eff, now)
		return nil
	})
	if err != nil {
		return fmt.Errorf("batch %s: resume: record unstarted jobs: %w", id, err)
	}
	return m.apply(ctx, st, eff, claimed)
}

// resumeSpawn finishes a coordinated dispatch: SpawnGroup is idempotent
// per batch, so calling it again returns the coordinator the dead process
// started, or starts it if the call never landed.
func (m *Manager) resumeSpawn(ctx context.Context, id ID, owner string, st State) error {
	sp, ok := m.client.(GroupSpawner)
	if !ok || len(st.Spec) != len(st.Jobs) {
		// The lease stays taken, so the next try comes after it ends.
		return fmt.Errorf("batch %s: resume: a coordinated dispatch needs a GroupSpawner client and its kept specs", id)
	}
	parent, err := sp.SpawnGroup(owned(ctx), string(id), st.Spec)
	if err != nil {
		return fmt.Errorf("batch %s: resume: spawn group: %w", id, err)
	}
	return m.saveParent(ctx, sp, id, owner, parent)
}
