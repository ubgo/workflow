package middleware

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ubgo/workflow"
)

// ErrEndpointDisabled matches the error AutoDisable discards an attempt
// with while its key is disabled.
var ErrEndpointDisabled = errors.New("workflow/middleware: endpoint disabled")

// DisableStore keeps AutoDisable's state per key: a count of consecutive
// failures and whether the key is disabled. Its reach is AutoDisable's:
// MemoryDisable counts within one process; a store shared by every
// worker counts across them and switches a key off for all of them
// (sqlstore.DisableStore on SQLite, Postgres or MySQL, redis.DisableStore).
// Fail must be atomic (count and maybe disable in one step), so exactly
// one caller sees the crossing and onDisable runs once, fleet-wide.
// workflowtest.DisableStoreContract checks an implementation.
type DisableStore interface {
	// Disabled reports whether key is disabled.
	Disabled(ctx context.Context, key string) (bool, error)
	// Fail counts one more consecutive failure of key and disables it
	// when the count reaches after; newly is true only for the call that
	// disabled it.
	Fail(ctx context.Context, key string, after int) (newly bool, err error)
	// Succeed resets key's count of consecutive failures.
	Succeed(ctx context.Context, key string) error
	// Enable re-enables key and resets its count.
	Enable(ctx context.Context, key string) error
}

// AutoDisableOption configures AutoDisable.
type AutoDisableOption func(*autoDisableConfig)

type autoDisableConfig struct {
	final *workflow.RetryPolicy
}

// DisableCountFinal counts only failures that end the step: an error
// policy will not retry (policy.Retryable false: permanent, discarded,
// NoRetryOn, or the last attempt Max allows). Pass the policy the step
// runs under; Until and MaxErrors are not seen, so a step they stop
// early is counted on its next delivery's final attempt instead. Without
// it, every failed attempt counts, which suits endpoint health: each
// attempt is a delivery the endpoint failed.
func DisableCountFinal(policy workflow.RetryPolicy) AutoDisableOption {
	return func(c *autoDisableConfig) { c.final = &policy }
}

// AutoDisable is step middleware that switches off a failing endpoint:
// key names the endpoint of an attempt (from its workflow.StepInfo and
// input, "" for a step that has none), and after consecutive failures of
// one key the key is disabled, onDisable(ctx, key) is called once (a
// nil onDisable is skipped), and every later attempt for the key is
// discarded with workflow.Discard of an error matching
// ErrEndpointDisabled, without running, until store.Enable(ctx, key). A
// success resets the count. Snoozes and cancellations count as neither.
// By default every failed attempt counts; DisableCountFinal counts only
// the failures that end a step.
//
// An after below one fails each attempt with a permanent error wrapping
// workflow.ErrInvalid. A store error reading the switch fails the
// attempt (retryable) rather than calling a disabled endpoint; a store
// error recording an outcome is dropped, so the switch trips one failure
// late. It runs on every driver; the discard ends the run StatusDiscarded
// with CapRunStates (see workflow.Discard).
//
// Per-endpoint ordering and isolation are not this middleware's job: a
// workflow with Concurrency{Key: endpoint, MaxRuns: 1, FIFO: true}
// delivers one message per endpoint at a time, in order (FIFO needs
// CapKeyFIFO), so a slow endpoint holds back only its own queue.
func AutoDisable(store DisableStore, key func(workflow.StepInfo, []byte) string, after int, onDisable func(ctx context.Context, key string), opts ...AutoDisableOption) workflow.Middleware {
	var cfg autoDisableConfig
	for _, o := range opts {
		o(&cfg)
	}
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if store == nil || key == nil || after < 1 {
				return nil, workflow.Permanent(fmt.Errorf("%w: AutoDisable needs a store, a key function and after of at least one", workflow.ErrInvalid))
			}
			info, _ := workflow.StepInfoFrom(ctx)
			k := key(info, in)
			if k == "" {
				return next(ctx, in)
			}
			off, err := store.Disabled(ctx, k)
			if err != nil {
				return nil, fmt.Errorf("workflow/middleware: auto-disable read: %w", err)
			}
			if off {
				return nil, workflow.Discard(fmt.Errorf("%w: %q", ErrEndpointDisabled, k))
			}
			out, err := next(ctx, in)
			bg := context.WithoutCancel(ctx)
			switch {
			case err == nil:
				_ = store.Succeed(bg, k)
			case errors.Is(err, workflow.ErrSnoozed) || errors.Is(err, workflow.ErrCancelled):
			case cfg.final != nil && cfg.final.Retryable(err, info.Attempt):
			default:
				if newly, ferr := store.Fail(bg, k, after); ferr == nil && newly && onDisable != nil {
					onDisable(bg, k)
				}
			}
			return out, err
		}
	}
}

// MemoryDisable is a DisableStore inside one process, for tests and a
// single worker; workers in several processes each count apart with
// their own, so give them a shared store instead (see DisableStore). Use
// NewMemoryDisable.
type MemoryDisable struct {
	mu   sync.Mutex
	keys map[string]disableState
}

type disableState struct {
	fails int
	off   bool
}

// NewMemoryDisable returns an empty store: every key enabled.
func NewMemoryDisable() *MemoryDisable { return &MemoryDisable{keys: map[string]disableState{}} }

// Disabled implements DisableStore.
func (m *MemoryDisable) Disabled(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keys[key].off, nil
}

// Fail implements DisableStore.
func (m *MemoryDisable) Fail(_ context.Context, key string, after int) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.keys[key]
	s.fails++
	newly := !s.off && s.fails >= after
	if newly {
		s.off = true
	}
	m.keys[key] = s
	return newly, nil
}

// Succeed implements DisableStore.
func (m *MemoryDisable) Succeed(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.keys[key]; ok {
		s.fails = 0
		m.keys[key] = s
	}
	return nil
}

// Enable implements DisableStore.
func (m *MemoryDisable) Enable(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.keys, key)
	return nil
}
