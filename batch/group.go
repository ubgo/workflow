package batch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// GroupRunner is an optional interface a workflow.Client implements to
// start every job of a batch in one call, as one group on its engine (a
// bulk run), instead of one Enqueue per job. Manager.Dispatch uses it when
// the client implements it and no job carries RunAt or IdempotencyKey;
// otherwise it enqueues the jobs one by one.
//
// The batch learns of each child's end exactly as for an enqueued job,
// through its Sources, so an implementation must keep two promises:
//
//   - each job's Labels (which carry LabelBatch and LabelIndex) reach that
//     child's workflow.RunEvent.Labels when the client is a RunNotifier;
//   - each returned RunRef answers Client.Status and Client.Cancel like a
//     ref from Enqueue.
//
// A client wrapper (middleware.WrapClient, middleware.WithCodec,
// workflow.Use) embeds workflow.Client and so hides GroupRunner: Dispatch
// then enqueues through the wrapper, and its middleware (a codec sealing
// payloads, say) is never bypassed.
type GroupRunner interface {
	// RunGroup starts jobs, which all have an empty RunAt and
	// IdempotencyKey, and returns one RunRef per job in the same order.
	// group is the batch ID, for the engine's grouping or metadata.
	//
	// An error wrapping workflow.ErrUnsupported, returned before anything
	// started (for example a group the engine cannot run as one, such as
	// jobs of several workflows), makes Dispatch fall back to one Enqueue
	// per job. Any other error means the group did not fully start: refs
	// is then nil, or has len(jobs) entries with a zero RunRef for each job
	// that did not start, and Dispatch cancels the batch (which cancels the
	// children that did start).
	RunGroup(ctx context.Context, group string, jobs []workflow.Job) ([]workflow.RunRef, error)
}

// groupEligible reports whether every job can go through a GroupRunner: a
// delayed start or an idempotency key needs Enqueue's own handling.
func groupEligible(jobs []workflow.Job) bool {
	for _, j := range jobs {
		if !j.RunAt.IsZero() || j.IdempotencyKey != "" {
			return false
		}
	}
	return true
}

// dispatchGroup starts sub (the jobs at idxs) through gr. handled is
// false when gr declined the group (ErrUnsupported), and the caller must
// enqueue them one by one instead. Grouped is saved before the call, so a
// resumer after a crash inside it knows any of these jobs may have
// started.
func (m *Manager) dispatchGroup(ctx context.Context, gr GroupRunner, id ID, owner string, idxs []int, sub []workflow.Job) (handled bool, err error) {
	if _, err := m.store.Update(ctx, id, func(s *State) error {
		if err := m.renew(s, owner); err != nil {
			return err
		}
		s.Grouped = true
		return nil
	}); err != nil {
		return true, fmt.Errorf("batch %s: mark grouped: %w", id, err)
	}
	refs, runErr := gr.RunGroup(owned(ctx), string(id), sub)
	if errors.Is(runErr, workflow.ErrUnsupported) {
		return false, nil
	}
	if runErr == nil && len(refs) != len(sub) {
		runErr = fmt.Errorf("GroupRunner returned %d refs for %d jobs", len(refs), len(sub))
	}
	if runErr != nil {
		if len(refs) != len(sub) {
			refs = nil // a ref we cannot place on its job is cancelled below instead
		}
		return true, errors.Join(fmt.Errorf("batch %s: run group: %w", id, runErr), m.abortJobs(ctx, id, owner, idxs, refs))
	}
	saved, err := m.store.Update(ctx, id, func(s *State) error {
		if err := m.renew(s, owner); err != nil {
			return err
		}
		for k, i := range idxs {
			s.Jobs[i].Ref = refs[k]
		}
		s.Dispatched = true
		m.dropSpec(s)
		return nil
	})
	if err != nil {
		return true, fmt.Errorf("batch %s: save group refs: %w", id, err)
	}
	// A child may have failed and cancelled the batch before its refs were
	// saved, when the cancel could not reach the children; reach them now.
	if saved.Cancelled {
		var errs []error
		for _, j := range saved.Jobs {
			errs = append(errs, m.cancelRun(ctx, j))
		}
		return true, errors.Join(errs...)
	}
	return true, nil
}

// GroupSpawner is an optional interface a workflow.Client implements to
// start a batch as one coordinator run on its engine, which then starts
// every job as its own child run. Dispatch is one trigger whatever the
// batch's size, and the engine, not the dispatching process, sees the
// jobs through to started. The Hatchet driver implements it behind
// hatchet.Config.BatchCoordinator.
//
// The children get no RunRef: the engine names a child only when the
// coordinator starts it, after Dispatch has returned. So the batch keeps
// Parent instead, learns each child's end by its LabelBatch label (which
// is why Dispatch refuses a coordinated batch unless a LabelSource is
// attached), and cancels with CancelGroup. The engine must not let a
// retried coordinator start a child twice.
type GroupSpawner interface {
	// SpawnsGroups reports whether Dispatch should use SpawnGroup for a
	// new batch. A client that implements the interface but has it
	// switched off returns false, and Dispatch takes the other paths.
	SpawnsGroups() bool

	// SpawnGroup starts the coordinator run for group (the batch ID) over
	// jobs, which all have an empty RunAt and IdempotencyKey and carry
	// the batch labels, and returns its ref. It must be idempotent per
	// group: a second call for the same group, as a resumed dispatch
	// makes, returns the first coordinator rather than starting another.
	// An error wrapping workflow.ErrUnsupported, returned before anything
	// started (a group too large for one coordinator, say), makes Dispatch
	// start the jobs another way.
	SpawnGroup(ctx context.Context, group string, jobs []workflow.Job) (workflow.RunRef, error)

	// CancelGroup cancels the coordinator parent and every unfinished
	// child of group created since since (the batch's CreatedAt). It does
	// not rely on the engine cancelling children with their parent, and
	// works with a zero parent (a SpawnGroup call that failed but may have
	// landed) by finding the runs by label.
	CancelGroup(ctx context.Context, group string, parent workflow.RunRef, since time.Time) error
}

// LabelSource marks a Source that learns job ends by the batch labels of
// the engine's runs, without the jobs' refs, and records the jobs a
// coordinator never started once it has ended. A coordinated batch
// (GroupSpawner) needs one; Attach records whether one is attached.
type LabelSource interface {
	Source
	FindsJobsByLabel()
}

// dispatchSpawn starts the batch through sp. handled is false when sp
// declined (ErrUnsupported), and Dispatch must start the jobs another
// way. Coordinated is saved before the call, so a resumer after a crash
// inside it calls SpawnGroup again, which returns the same coordinator.
func (m *Manager) dispatchSpawn(ctx context.Context, sp GroupSpawner, id ID, owner string, jobs []workflow.Job) (handled bool, err error) {
	if _, err := m.store.Update(ctx, id, func(s *State) error {
		if err := m.renew(s, owner); err != nil {
			return err
		}
		s.Coordinated = true
		return nil
	}); err != nil {
		return true, fmt.Errorf("batch %s: mark coordinated: %w", id, err)
	}
	parent, spawnErr := sp.SpawnGroup(owned(ctx), string(id), jobs)
	if errors.Is(spawnErr, workflow.ErrUnsupported) {
		if _, err := m.store.Update(ctx, id, func(s *State) error {
			if err := m.renew(s, owner); err != nil {
				return err
			}
			s.Coordinated = false
			return nil
		}); err != nil {
			return true, fmt.Errorf("batch %s: unmark coordinated: %w", id, err)
		}
		return false, nil
	}
	if spawnErr != nil {
		// The trigger may have landed although the call failed (a timeout
		// after the engine accepted it), so the group is cancelled by
		// label as well, coordinator ref or not.
		st, err := m.store.Get(ctx, id)
		if err != nil {
			return true, errors.Join(spawnErr, err)
		}
		return true, errors.Join(fmt.Errorf("batch %s: spawn group: %w", id, spawnErr),
			m.abortJobs(ctx, id, owner, allIndexes(len(jobs)), nil),
			sp.CancelGroup(ctx, string(id), workflow.RunRef{}, st.CreatedAt))
	}
	return true, m.saveParent(ctx, sp, id, owner, parent)
}

// saveParent records the coordinator ref and ends the dispatch; a batch
// cancelled meanwhile has its group cancelled now that the parent is
// known.
func (m *Manager) saveParent(ctx context.Context, sp GroupSpawner, id ID, owner string, parent workflow.RunRef) error {
	saved, err := m.store.Update(ctx, id, func(s *State) error {
		if err := m.renew(s, owner); err != nil {
			return err
		}
		s.Parent = parent
		s.Dispatched = true
		m.dropSpec(s)
		return nil
	})
	if err != nil {
		return fmt.Errorf("batch %s: save coordinator: %w", id, err)
	}
	if saved.Cancelled {
		return sp.CancelGroup(ctx, string(id), parent, saved.CreatedAt)
	}
	return nil
}

func allIndexes(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// owned returns ctx allowing the batch labels this package stamps
// (LabelPrefixBatch) through the client's reserved-label check.
func owned(ctx context.Context) context.Context {
	return workflow.AllowReservedPrefix(ctx, workflow.LabelPrefixBatch)
}
