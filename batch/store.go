package batch

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// ID identifies one batch.
type ID string

// String returns the ID as a string.
func (id ID) String() string { return string(id) }

// State is everything a Store keeps for one batch. The Manager is the only
// writer; a Store persists it as one record and never interprets it.
type State struct {
	ID   ID
	Name string

	// Labels is Batch.Labels without the reserved batch labels, copied
	// onto every callback job.
	Labels map[string]string `json:",omitempty"`

	// Then, Catch and Finally are the callback workflow names; empty means
	// none.
	Then, Catch, Finally string

	// Before and Progress are Batch.Before and Batch.Progress: the
	// workflows enqueued once when the batch is created and once per job
	// end reported by a run. Empty means none.
	Before   string `json:",omitempty"`
	Progress string `json:",omitempty"`

	// AllowFailures is Batch.AllowFailures: it stops a failure from
	// cancelling the other jobs, and Catch fires on the first failure anyway.
	AllowFailures bool

	// DiscardedAs is Batch.DiscardedAs; empty means StatusFailed.
	DiscardedAs workflow.Status `json:",omitempty"`

	// Jobs holds one entry per job, in the order of Batch.Jobs; a job's
	// index is its identity, because its RunRef is not known until Enqueue
	// returns and a fast run can finish before that.
	Jobs []JobState

	// Cancelled is set by Manager.Cancel, or by the first failure when
	// AllowFailures is false. A cancelled batch never runs Then.
	Cancelled bool

	// FailCancelled is set together with Cancelled when a failed job
	// cancelled the batch, as opposed to Manager.Cancel or a dispatch that
	// failed. Manager.RetryFailed revives only such a batch, and then also
	// re-runs the jobs that cancel stopped.
	FailCancelled bool `json:",omitempty"`

	// Round counts Manager.RetryFailed calls that re-ran jobs. It goes
	// into the idempotency key of every callback decided after a retry,
	// so a re-armed Then, Catch or Finally is not deduplicated against the
	// one the previous round enqueued.
	Round int `json:",omitempty"`

	// CatchFired and Finished are the once-only latches: each flips from
	// false to true inside one Store.Update, so exactly one caller wins the
	// right to enqueue Catch, and exactly one wins Then and Finally.
	CatchFired bool
	Finished   bool

	// Pending holds the callbacks decided but not yet enqueued, in the
	// order they were decided; see PendingCallback.
	Pending []PendingCallback `json:",omitempty"`

	CreatedAt   time.Time
	CancelledAt time.Time // zero unless Cancelled
	FinishedAt  time.Time // zero unless Finished

	// Dispatch progress, which lets another process finish a Dispatch whose
	// process died part-way (see Manager.ResumeDispatches).
	//
	// Dispatched is set once every job was started or recorded as never
	// started. DispatchOwner names the Dispatch (or resume) that may start
	// jobs, and DispatchUntil is when its lease ends; every Store.Update of
	// a dispatch checks the owner and extends the lease, so a resumer takes
	// over only after the dispatcher stopped writing. Grouped is set before
	// a GroupRunner call, after which no job without a saved ref is known
	// to be unstarted. Spec holds the labelled jobs while the dispatch is
	// open, so a resumer can start the ones never started; it is kept on
	// the GroupRunner path, and on the one-Enqueue-per-job path only with
	// WithResumableDispatch, because there every saved ref rewrites the
	// whole State. A State written before these fields existed reads as
	// not dispatched with an expired lease; a resumer then finds every job
	// started and only sets Dispatched.
	//
	// Coordinated is set before a GroupSpawner call, and Parent holds the
	// coordinator run it returned: the batch's jobs are that run's
	// children, which never get a ref of their own (see GroupSpawner).
	Dispatched    bool            `json:",omitempty"`
	DispatchOwner string          `json:",omitempty"`
	DispatchUntil time.Time       `json:",omitzero"`
	Grouped       bool            `json:",omitempty"`
	Spec          []workflow.Job  `json:",omitempty"`
	Coordinated   bool            `json:",omitempty"`
	Parent        workflow.RunRef `json:",omitzero"`
}

// JobState is one job of a batch.
type JobState struct {
	// Ref is the run, set once Enqueue returns; zero before that or when
	// the job was never enqueued.
	Ref workflow.RunRef

	// Status is the run's terminal status, or "" while it has not
	// finished. Only terminal statuses are recorded.
	Status workflow.Status

	// Attempt is 0 for the job's first run and grows by one each time
	// Manager.RetryFailed starts it again. A retried job's run carries it
	// in LabelAttempt, and a run end whose attempt differs is ignored, so
	// the late end of an earlier run never counts for the new one.
	Attempt int `json:",omitempty"`
}

// Open reports whether the batch still needs the Manager: a job has not
// finished, or a callback is pending. Store.Unfinished returns exactly the
// open batches.
func (s State) Open() bool { return !s.Finished || len(s.Pending) > 0 }

// clone returns a deep copy, so a Store never shares a slice with a
// caller.
func (s State) clone() State {
	s.Labels = maps.Clone(s.Labels)
	s.Jobs = append([]JobState(nil), s.Jobs...)
	s.Pending = append([]PendingCallback(nil), s.Pending...)
	s.Spec = append([]workflow.Job(nil), s.Spec...)
	return s
}

// Store persists batch state. Update must be atomic per batch: the read,
// the mutation and the write are one step with no other Update for the same
// ID in between (a SQL store uses a transaction with SELECT ... FOR UPDATE,
// or a version column), because the once-only latches in State rely on it.
// The guarantee a batch gives across processes is exactly the store's:
// MemoryStore holds only inside one process.
type Store interface {
	// Create saves a new batch. An existing ID is an error.
	Create(ctx context.Context, st State) error

	// Get returns a batch, or an error wrapping workflow.ErrNotFound.
	Get(ctx context.Context, id ID) (State, error)

	// Update applies fn to the current state and saves the result
	// atomically, returning the saved state. When fn returns an error
	// nothing is saved and Update returns that error. An unknown ID
	// returns an error wrapping workflow.ErrNotFound.
	Update(ctx context.Context, id ID, fn func(*State) error) (State, error)

	// Unfinished returns every open batch (State.Open: a job not finished
	// or a callback pending), for the status poller and RetryCallbacks. A
	// SQL store backs it with an indexed flag written from Open, so a long
	// history of closed batches costs nothing.
	Unfinished(ctx context.Context) ([]State, error)
}

// Pruner is an optional interface a Store implements to delete old batch
// state. Manager.Prune
// calls it and refuses with an UnsupportedError for a Store without it.
// An implementation deletes exactly the batches PruneFilter.Matches
// accepts (a SQL store may express the same rule in its WHERE clause) and
// returns how many it deleted.
type Pruner interface {
	Prune(ctx context.Context, f PruneFilter) (int, error)
}

// PruneFilter says which batches Prune deletes. Each non-zero time adds
// one rule, and a batch matching any rule is deleted: finished, unfinished
// and cancelled batches each have their own age. A filter with every time zero matches nothing, and
// Manager.Prune refuses it as a mistake.
type PruneFilter struct {
	// FinishedBefore deletes closed batches (every job finished and no
	// callback pending) that finished before it.
	FinishedBefore time.Time

	// UnfinishedBefore deletes open batches (State.Open: a job still
	// running or a callback still pending) created before it. Their jobs'
	// later ends are then ignored, since the batch is gone.
	UnfinishedBefore time.Time

	// CancelledBefore deletes cancelled batches cancelled before it,
	// whether or not their jobs have finished.
	CancelledBefore time.Time
}

// IsZero reports whether f has no rule.
func (f PruneFilter) IsZero() bool {
	return f.FinishedBefore.IsZero() && f.UnfinishedBefore.IsZero() && f.CancelledBefore.IsZero()
}

// Matches reports whether Prune deletes st under f. It is the whole
// pruning decision, so every Store applies the same rule.
func (f PruneFilter) Matches(st State) bool {
	switch {
	case !f.FinishedBefore.IsZero() && !st.Open() && st.FinishedAt.Before(f.FinishedBefore):
		return true
	case !f.UnfinishedBefore.IsZero() && st.Open() && st.CreatedAt.Before(f.UnfinishedBefore):
		return true
	case !f.CancelledBefore.IsZero() && st.Cancelled && st.CancelledAt.Before(f.CancelledBefore):
		return true
	}
	return false
}

// MemoryStore is an in-process Store, for tests and the memory driver. The
// zero value is not usable; call NewMemoryStore.
type MemoryStore struct {
	mu      sync.Mutex
	batches map[ID]State
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore { return &MemoryStore{batches: map[ID]State{}} }

// Create implements Store.
func (m *MemoryStore) Create(_ context.Context, st State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.batches[st.ID]; ok {
		return fmt.Errorf("batch: id %q already exists", st.ID)
	}
	m.batches[st.ID] = st.clone()
	return nil
}

// Get implements Store.
func (m *MemoryStore) Get(_ context.Context, id ID) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.batches[id]
	if !ok {
		return State{}, fmt.Errorf("batch %q: %w", id, workflow.ErrNotFound)
	}
	return st.clone(), nil
}

// Update implements Store.
func (m *MemoryStore) Update(_ context.Context, id ID, fn func(*State) error) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.batches[id]
	if !ok {
		return State{}, fmt.Errorf("batch %q: %w", id, workflow.ErrNotFound)
	}
	st = st.clone()
	if err := fn(&st); err != nil {
		return State{}, err
	}
	m.batches[id] = st
	return st.clone(), nil
}

// Prune implements Pruner.
func (m *MemoryStore) Prune(_ context.Context, f PruneFilter) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for id, st := range m.batches {
		if f.Matches(st) {
			delete(m.batches, id)
			n++
		}
	}
	return n, nil
}

// Unfinished implements Store.
func (m *MemoryStore) Unfinished(_ context.Context) ([]State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []State
	for _, st := range m.batches {
		if st.Open() {
			out = append(out, st.clone())
		}
	}
	return out, nil
}

var _ Pruner = (*MemoryStore)(nil)
