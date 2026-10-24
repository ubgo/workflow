package middleware

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// debounceNeeds is what Debounce asks of the driver: a delayed start to
// hold the run for the window, and Cancel to drop the run it replaces.
const debounceNeeds = workflow.CapDelayedStart | workflow.CapCancel

// DebounceBurst is one key's pending run, as a DebounceStore keeps it.
// Invariant: RunAt is the RunAt the pending run was enqueued with, and
// First <= RunAt <= First+maxWait. A burst whose RunAt has passed is over:
// Debounce treats it as absent, and a store may drop it at any time.
type DebounceBurst struct {
	// First is when the burst's first job was enqueued; maxWait counts
	// from it.
	First time.Time
	// RunAt is when the pending run starts.
	RunAt time.Time
	// Ref is the pending run, which the next job of the burst cancels.
	Ref workflow.RunRef
}

// DebounceStore holds the pending burst of each debounce key. It is where
// the debounce state lives, so its reach is Debounce's: MemoryDebounce
// debounces inside one process; a store shared by several processes (a
// SQL table, Redis) debounces a key enqueued from all of them.
//
// Swap must be atomic: replacing a key's burst and returning the one it
// replaced is one step, so when two processes debounce the same key at
// once each gets back a different previous run and every replaced run is
// cancelled by exactly one of them. Get is only a hint for First and may
// race; Swap is what decides which run is cancelled.
//
// An error from either method fails the enqueue closed: on Get the job is
// not enqueued; on Swap the job's new run is cancelled and the error
// returned, so the store and the engine never disagree about which run is
// pending.
type DebounceStore interface {
	// Get returns key's burst, or found false when there is none.
	Get(ctx context.Context, key string) (b DebounceBurst, found bool, err error)
	// Swap stores b as key's burst and returns the burst it replaced, or
	// found false when there was none.
	Swap(ctx context.Context, key string, b DebounceBurst) (prev DebounceBurst, found bool, err error)
}

// DebounceOption configures Debounce.
type DebounceOption func(*debouncer)

// WithDebounceStore keeps the bursts in s instead of a MemoryDebounce of
// the debouncer's own, for example a store shared by every process that
// enqueues the key. A nil s keeps the default.
func WithDebounceStore(s DebounceStore) DebounceOption {
	return func(d *debouncer) {
		if s != nil {
			d.store = s
		}
	}
}

// Debounce makes the last of a burst of jobs with the same key win: each
// enqueue delays its run to now+window and cancels the pending run of the
// previous job with that key, so only the last one runs, once the key has
// been quiet for window. maxWait caps the delay from the first job of the
// burst, so a key that never goes quiet still runs at least every maxWait;
// zero means no cap. A job for which key returns "" passes through.
//
// It is built on Job.RunAt and Client.Cancel of c, the client the jobs
// finally reach, and returns an UnsupportedError (matching
// workflow.ErrUnsupported) when c lacks CapDelayedStart or CapCancel,
// rather than letting every job through undebounced.
//
// Behaviour to know:
//   - The job's own RunAt is replaced.
//   - The new run is enqueued before the old one is cancelled, so a failed
//     enqueue never loses the burst. If the cancel then fails, Debounce
//     returns the new RunRef together with the error: both runs may fire.
//   - A previous run whose start time has passed is left alone and the job
//     opens a new burst; cancelling it could stop a run already working.
//   - Do not set Job.IdempotencyKey on debounced jobs: the pending run
//     holds the key, so the replacement would be folded into it.
//   - The bursts live in a DebounceStore, by default a MemoryDebounce of
//     the returned middleware's own, so debouncing is per process; a key
//     enqueued from two processes is debounced in each unless
//     WithDebounceStore names a store they share. Calls in one process are
//     serialised by one lock, held across the driver calls.
//
// A store shared by several processes, for example:
//
//	mw, err := middleware.Debounce(c, nil, byUser, 10*time.Second, time.Minute,
//		middleware.WithDebounceStore(myRedisDebounce))
func Debounce(c workflow.Client, clk clock.Clock, key func(workflow.Job) string, window, maxWait time.Duration, opts ...DebounceOption) (workflow.EnqueueMiddleware, error) {
	if missing := c.Capabilities().Missing(debounceNeeds); missing != 0 {
		return nil, &workflow.UnsupportedError{Field: "Debounce", Missing: missing}
	}
	if key == nil || window <= 0 || (maxWait != 0 && maxWait < window) {
		return nil, fmt.Errorf("%w: Debounce needs a key, a positive window and a maxWait of zero or at least window", workflow.ErrInvalid)
	}
	if clk == nil {
		clk = clock.Real{}
	}
	d := &debouncer{c: c, clk: clk, key: key, window: window, maxWait: maxWait}
	for _, o := range opts {
		o(d)
	}
	if d.store == nil {
		d.store = NewMemoryDebounce(clk)
	}
	return d.wrap, nil
}

type debouncer struct {
	c       workflow.Client
	clk     clock.Clock
	key     func(workflow.Job) string
	window  time.Duration
	maxWait time.Duration
	store   DebounceStore

	mu sync.Mutex
}

func (d *debouncer) wrap(next workflow.EnqueueFunc) workflow.EnqueueFunc {
	return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
		k := d.key(job)
		if k == "" {
			return next(ctx, job)
		}
		d.mu.Lock()
		defer d.mu.Unlock()

		now := d.clk.Now()
		prev, pending, err := d.store.Get(ctx, k)
		if err != nil {
			return workflow.RunRef{}, fmt.Errorf("workflow/middleware: debounce store get: %w", err)
		}
		first := now
		if pending && prev.RunAt.After(now) {
			first = prev.First
		}
		runAt := now.Add(d.window)
		if d.maxWait > 0 && runAt.After(first.Add(d.maxWait)) {
			runAt = first.Add(d.maxWait)
		}
		job.RunAt = runAt
		ref, err := next(ctx, job)
		if err != nil {
			return workflow.RunRef{}, err
		}
		// Swap decides which run is replaced: under a shared store another
		// process may have swapped in since Get, and its run is the one to
		// cancel now.
		prev, pending, err = d.store.Swap(ctx, k, DebounceBurst{First: first, RunAt: runAt, Ref: ref})
		if err != nil {
			// Fail closed: the store does not know the new run, so drop it
			// rather than let it fire beside the pending one.
			if cerr := d.c.Cancel(context.WithoutCancel(ctx), ref); cerr != nil && !errors.Is(cerr, workflow.ErrNotFound) {
				return ref, fmt.Errorf("workflow/middleware: debounce store swap: %w (and the new run could not be cancelled: %w)", err, cerr)
			}
			return workflow.RunRef{}, fmt.Errorf("workflow/middleware: debounce store swap: %w", err)
		}
		if pending && prev.RunAt.After(now) && prev.Ref != ref {
			if err := d.c.Cancel(ctx, prev.Ref); err != nil && !errors.Is(err, workflow.ErrNotFound) {
				return ref, fmt.Errorf("workflow/middleware: debounce cancel of the replaced run: %w", err)
			}
		}
		return ref, nil
	}
}

// MemoryDebounce is a DebounceStore inside one process, the default of
// Debounce. Bursts whose run has started are dropped on the next Swap.
// Use NewMemoryDebounce.
type MemoryDebounce struct {
	clk    clock.Clock
	mu     sync.Mutex
	bursts map[string]DebounceBurst
}

// NewMemoryDebounce returns an empty store reading time from clk (nil
// means clock.Real) to drop finished bursts.
func NewMemoryDebounce(clk clock.Clock) *MemoryDebounce {
	if clk == nil {
		clk = clock.Real{}
	}
	return &MemoryDebounce{clk: clk, bursts: map[string]DebounceBurst{}}
}

// Get implements DebounceStore.
func (m *MemoryDebounce) Get(_ context.Context, key string) (DebounceBurst, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.bursts[key]
	return b, ok, nil
}

// Swap implements DebounceStore.
func (m *MemoryDebounce) Swap(_ context.Context, key string, b DebounceBurst) (DebounceBurst, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clk.Now()
	for bk, old := range m.bursts { // drop bursts whose run has started
		if bk != key && !old.RunAt.After(now) {
			delete(m.bursts, bk)
		}
	}
	prev, ok := m.bursts[key]
	m.bursts[key] = b
	return prev, ok, nil
}
