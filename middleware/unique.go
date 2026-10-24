package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// The labels Unique stamps on every job it passes, so the run-finished
// observer of UniqueUntilFinished can find the lock from the RunEvent alone,
// in any process and after a restart, without an in-process table. Unique
// owns them: a job that arrives carrying either, or any other label under
// workflow.LabelPrefixInternal except ClaimCheckLabel (which
// ClaimCheck.Enqueue may have stamped before it), is refused with
// workflow.ErrInvalid, so a caller cannot release someone else's lock by
// setting them. A job enqueued straight on the driver, bypassing Unique,
// is not covered by that and must not carry them. One Unique per chain:
// a second would refuse the first one's labels.
const (
	UniqueKeyLabel   = "ubgo.workflow/unique-key"
	UniqueOwnerLabel = "ubgo.workflow/unique-owner"
)

// ownerTokenBytes is the size of the random owner token: 128 bits, so two
// enqueues never draw the same one.
const ownerTokenBytes = 16

// UniqueStore holds uniqueness locks. Acquire must be atomic: checking
// that key is free and taking it is one step, or two enqueues at once both
// pass (the first of the two caveats in the spec). The guarantee Unique
// gives is exactly the store's: a SQL store with a unique index on the key
// makes it hold across every process sharing the database; MemoryUnique
// holds only inside one process.
//
// Every lock has an owner, a token Unique draws per enqueue, and Release
// frees the key only while that owner still holds it. Run-finished events
// arrive at least once, so a late duplicate for an old run must not free
// the lock a newer job has taken since.
type UniqueStore interface {
	// Acquire takes key for owner for ttl and reports true, or reports
	// false when another holder has it and its ttl has not run out.
	Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	// Release frees key if owner holds it. Releasing a free key, or one
	// held by another owner, is a no-op, not an error.
	Release(ctx context.Context, key, owner string) error
}

// Unique refuses a job whose key, from by, is already held: the enqueue
// returns an error wrapping workflow.ErrDuplicate and nothing reaches the
// driver. A job for which by
// returns "" is not unique and passes through.
//
// The lock lasts ttl from the enqueue. Use UniqueUntilFinished to also
// free it when the run ends, which is what an app usually wants; plain
// Unique suits a driver with no run-finished hook. If the driver refuses
// the enqueue, the lock is released at once so a retry can pass. A ttl of
// zero or less fails each enqueue with workflow.ErrInvalid rather than
// holding a lock forever.
func Unique(by func(workflow.Job) string, store UniqueStore, ttl time.Duration) workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if err := refuseInternalLabels("Unique", job.Labels, ClaimCheckLabel); err != nil {
				return workflow.RunRef{}, err
			}
			key := by(job)
			if key == "" {
				return next(ctx, job)
			}
			if ttl <= 0 {
				return workflow.RunRef{}, fmt.Errorf("%w: Unique needs a positive ttl", workflow.ErrInvalid)
			}
			owner, err := newOwnerToken()
			if err != nil {
				return workflow.RunRef{}, err
			}
			ok, err := store.Acquire(ctx, key, owner, ttl)
			if err != nil {
				return workflow.RunRef{}, fmt.Errorf("workflow/middleware: unique acquire: %w", err)
			}
			if !ok {
				return workflow.RunRef{}, fmt.Errorf("%w: unique key %q is held", workflow.ErrDuplicate, key)
			}
			job.Labels = stampUnique(job.Labels, key, owner)
			ref, err := next(workflow.AllowReservedLabels(ctx, UniqueKeyLabel, UniqueOwnerLabel), job)
			if err != nil {
				// The job never reached the engine, so the lock guards
				// nothing; a release failure only delays the retry by ttl.
				_ = store.Release(context.WithoutCancel(ctx), key, owner)
				return workflow.RunRef{}, err
			}
			return ref, nil
		}
	}
}

// UniqueUntilFinished is Unique whose lock is also freed when the run
// ends, whatever its status: once the work is done, failed or not, a new
// job for the key may be enqueued. c is the driver's client or any wrapper
// from this package (they forward workflow.RunNotifier), and this must be
// called before c.Start: the driver refuses a late observer with
// workflow.ErrAlreadyStarted, which is returned here.
//
// ttl stays the safety net: without CapRunFinished the driver's delivery
// is best-effort, and a release can fail, so a lost event costs at most
// ttl of blocked enqueues, never a lock held forever. A client that does
// not implement workflow.RunNotifier gets an UnsupportedError naming
// CapRunFinished; use Unique there and accept the TTL alone.
func UniqueUntilFinished(c workflow.Client, by func(workflow.Job) string, store UniqueStore, ttl time.Duration) (workflow.EnqueueMiddleware, error) {
	n, ok := c.(workflow.RunNotifier)
	if !ok {
		return nil, &workflow.UnsupportedError{Field: "UniqueUntilFinished", Missing: workflow.CapRunFinished, Reason: "the client does not implement workflow.RunNotifier"}
	}
	err := n.OnRunFinished(func(ctx context.Context, ev workflow.RunEvent) {
		key, owner := ev.Labels[UniqueKeyLabel], ev.Labels[UniqueOwnerLabel]
		if key == "" || owner == "" {
			return
		}
		// A failed release is not reported: the observer has no caller to
		// tell, and the ttl frees the key anyway.
		_ = store.Release(context.WithoutCancel(ctx), key, owner)
	})
	if err != nil {
		return nil, fmt.Errorf("workflow/middleware: register unique release: %w", err)
	}
	return Unique(by, store, ttl), nil
}

// UniqueUntilStarted is Unique whose lock is also freed as soon as a step
// of the run starts, so a second job for the same key may be enqueued
// while the first one runs. It
// returns the two halves: enqueue goes in the enqueuing process's
// WrapClient chain, step in the worker's step middleware
// (memory.WithMiddleware, sqlstore.WithMiddleware, hatchet.Config.Middleware).
// A worker in another process from the enqueuer installs
// ReleaseUniqueOnStart(store) with the same store instead.
//
// Without the step half on the worker, nothing frees the lock early and it
// lasts ttl, exactly like Unique: the ttl stays the safety net for a run
// that never starts, and for a release that fails. The store must be one
// every enqueuing and working process shares (a SQL store, not
// MemoryUnique) when they are separate processes.
func UniqueUntilStarted(by func(workflow.Job) string, store UniqueStore, ttl time.Duration) (enqueue workflow.EnqueueMiddleware, step workflow.Middleware) {
	return Unique(by, store, ttl), ReleaseUniqueOnStart(store)
}

// ReleaseUniqueOnStart is the step half of UniqueUntilStarted: before a
// step runs it frees the lock that Unique stamped on the run's labels
// (UniqueKeyLabel and UniqueOwnerLabel, read from the driver's
// workflow.StepInfo), then calls the step whatever the release returned. A
// run without those labels, or a ctx without a StepInfo, passes straight
// through.
//
// It releases on every step attempt, not only the first: Release is a
// no-op once the lock is gone, and frees nothing a newer job holds because
// it names this run's owner token, so the repeats are safe and also retry
// a release that failed earlier. Each costs one store call. A failed
// release is not reported: the step has no reason to fail for it, and the
// ttl frees the key anyway.
func ReleaseUniqueOnStart(store UniqueStore) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if info, ok := workflow.StepInfoFrom(ctx); ok {
				key, owner := info.Labels[UniqueKeyLabel], info.Labels[UniqueOwnerLabel]
				if key != "" && owner != "" {
					_ = store.Release(context.WithoutCancel(ctx), key, owner)
				}
			}
			return next(ctx, in)
		}
	}
}

// stampUnique returns a copy of labels with the unique labels set to key
// and owner, so the caller's map is never mutated.
func stampUnique(labels map[string]string, key, owner string) map[string]string {
	out := make(map[string]string, len(labels)+2)
	maps.Copy(out, labels)
	out[UniqueKeyLabel], out[UniqueOwnerLabel] = key, owner
	return out
}

// refuseInternalLabels refuses an incoming app label under
// workflow.LabelPrefixInternal, the prefix this package owns, with an
// error wrapping workflow.ErrInvalid, rather than dropping or overwriting
// it. allow names the keys a sibling middleware of this package stamps
// and that may already be on the job when owner sees it. Keys are checked
// in sorted order, so the error is the same on every call.
func refuseInternalLabels(owner string, labels map[string]string, allow ...string) error {
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		if strings.HasPrefix(k, workflow.LabelPrefixInternal) && !slices.Contains(allow, k) {
			return fmt.Errorf("%w: %s: label %q uses the reserved prefix %q", workflow.ErrInvalid, owner, k, workflow.LabelPrefixInternal)
		}
	}
	return nil
}

func newOwnerToken() (string, error) {
	b := make([]byte, ownerTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("workflow/middleware: unique owner token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// MemoryUnique is a UniqueStore inside one process, for tests, a single
// server, or a cap that only needs to hold per process. Expired keys are
// dropped when they are next touched. Use NewMemoryUnique.
type MemoryUnique struct {
	clk  clock.Clock
	mu   sync.Mutex
	held map[string]uniqueLock
}

type uniqueLock struct {
	owner   string
	expires time.Time
	ref     workflow.RunRef // the holder's run, once SetHolder recorded it
}

// NewMemoryUnique returns an empty store reading time from clk (nil means
// clock.Real).
func NewMemoryUnique(clk clock.Clock) *MemoryUnique {
	if clk == nil {
		clk = clock.Real{}
	}
	return &MemoryUnique{clk: clk, held: map[string]uniqueLock{}}
}

// Acquire implements UniqueStore.
func (m *MemoryUnique) Acquire(_ context.Context, key, owner string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clk.Now()
	if l, ok := m.held[key]; ok && now.Before(l.expires) {
		return false, nil
	}
	m.held[key] = uniqueLock{owner: owner, expires: now.Add(ttl)}
	return true, nil
}

// SetHolder implements UniqueHolders.
func (m *MemoryUnique) SetHolder(_ context.Context, key, owner string, ref workflow.RunRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.held[key]; ok && l.owner == owner {
		l.ref = ref
		m.held[key] = l
	}
	return nil
}

// Holder implements UniqueHolders.
func (m *MemoryUnique) Holder(_ context.Context, key string) (string, workflow.RunRef, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.held[key]
	if !ok || !m.clk.Now().Before(l.expires) || l.ref.IsZero() {
		return "", workflow.RunRef{}, false, nil
	}
	return l.owner, l.ref, true, nil
}

// Release implements UniqueStore.
func (m *MemoryUnique) Release(_ context.Context, key, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.held[key]; ok && l.owner == owner {
		delete(m.held, key)
	}
	return nil
}
