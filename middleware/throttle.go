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

// ErrThrottled is returned, without calling the step, while a key's
// circuit is open. It is retryable on purpose: the retry policy's backoff
// is what brings the step back after the cooldown, as a release back onto
// the queue would.
var ErrThrottled = errors.New("workflow/middleware: step throttled after repeated errors")

// ThrottleConfig configures ThrottleErrors.
type ThrottleConfig struct {
	// Clock reads time for the window and the cooldown of the default
	// store. Nil means clock.Real. A custom Store keeps its own time.
	Clock clock.Clock

	// Store holds each key's circuit. Nil means a MemoryCircuit of the
	// middleware's own, so each process trips its own circuit; a store
	// shared by the fleet (a SQL table, Redis) gives one circuit per key
	// across every worker, for a downstream with a fleet-wide error
	// budget.
	Store CircuitStore

	// Key picks the circuit a call belongs to, such as the downstream API
	// the step calls. Nil keys by the workflow name from the
	// workflow.StepInfo on ctx, so one failing workflow does not throttle
	// the others; a ctx without StepInfo falls into the "" circuit.
	Key func(ctx context.Context, in []byte) string

	// MaxErrors is how many errors inside Window open the circuit; at
	// least 1.
	MaxErrors int

	// Window is how long errors are counted from the first one; positive.
	Window time.Duration

	// Cooldown is how long an open circuit fails fast; positive.
	Cooldown time.Duration
}

// CircuitPolicy is the part of ThrottleConfig a CircuitStore applies when
// it counts an error.
type CircuitPolicy struct {
	MaxErrors int
	Window    time.Duration
	Cooldown  time.Duration
}

// CircuitStore holds ThrottleErrors' circuits, one per key. The store owns
// the counting, so a shared store can count every worker's errors in one
// atomic step (an increment, not a read and a write) and open one circuit
// for the whole fleet.
//
// An IsOpen error fails closed: the step is not run and the call returns
// an error wrapping ErrThrottled and the store's error, retryable like any
// throttled call. A RecordError or Reset error never turns a step's result
// around: it is joined onto a failed step's error and dropped after a
// successful one, where the worst it can do is let the circuit open early.
type CircuitStore interface {
	// IsOpen reports whether key's circuit is open now.
	IsOpen(ctx context.Context, key string) (bool, error)
	// RecordError counts one error for key and opens its circuit for
	// p.Cooldown once p.MaxErrors errors fall inside p.Window, counted
	// from the first of them.
	RecordError(ctx context.Context, key string, p CircuitPolicy) error
	// Reset closes key's circuit and clears its count, after a success.
	Reset(ctx context.Context, key string) error
}

// ThrottleErrors is a circuit breaker per key: after MaxErrors errors
// within Window, every call for that key returns ErrThrottled without
// running the step until Cooldown has passed. A success resets the count.
// The state lives in cfg.Store, by default a MemoryCircuit of the returned
// middleware's own, so it is per process; each worker process trips its
// own circuit, which is the point of a breaker that protects a downstream
// service from this process's calls. A shared Store makes it fleet-wide.
//
// Errors marked Permanent and ErrThrottled itself are not counted: a
// permanent error is about the input, not the downstream service.
func ThrottleErrors(cfg ThrottleConfig) (workflow.Middleware, error) {
	if cfg.MaxErrors < 1 || cfg.Window <= 0 || cfg.Cooldown <= 0 {
		return nil, fmt.Errorf("%w: ThrottleErrors needs MaxErrors >= 1 and a positive Window and Cooldown", workflow.ErrInvalid)
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryCircuit(cfg.Clock)
	}
	b := &breaker{cfg: cfg, policy: CircuitPolicy{MaxErrors: cfg.MaxErrors, Window: cfg.Window, Cooldown: cfg.Cooldown}}
	return b.wrap, nil
}

type breaker struct {
	cfg    ThrottleConfig
	policy CircuitPolicy
}

func (b *breaker) wrap(next workflow.Handler) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		var key string
		if b.cfg.Key != nil {
			key = b.cfg.Key(ctx, in)
		} else if info, ok := workflow.StepInfoFrom(ctx); ok {
			key = info.Workflow
		}
		open, err := b.cfg.Store.IsOpen(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("%w (key %q): circuit store: %w", ErrThrottled, key, err)
		}
		if open {
			return nil, fmt.Errorf("%w (key %q)", ErrThrottled, key)
		}
		out, err := next(ctx, in)
		switch {
		case err == nil:
			_ = b.cfg.Store.Reset(context.WithoutCancel(ctx), key) // see CircuitStore
		case workflow.IsPermanent(err) || errors.Is(err, ErrThrottled):
		default:
			if rerr := b.cfg.Store.RecordError(context.WithoutCancel(ctx), key, b.policy); rerr != nil {
				err = errors.Join(err, fmt.Errorf("workflow/middleware: circuit store: %w", rerr))
			}
		}
		return out, err
	}
}

// MemoryCircuit is a CircuitStore inside one process, the default of
// ThrottleErrors. Use NewMemoryCircuit.
type MemoryCircuit struct {
	clk      clock.Clock
	mu       sync.Mutex
	circuits map[string]*circuit
}

// circuit is one key's state. Invariant: openUntil is zero while closed;
// errors counts only errors since windowStart.
type circuit struct {
	errors      int
	windowStart time.Time
	openUntil   time.Time
}

// NewMemoryCircuit returns an empty store reading time from clk (nil means
// clock.Real).
func NewMemoryCircuit(clk clock.Clock) *MemoryCircuit {
	if clk == nil {
		clk = clock.Real{}
	}
	return &MemoryCircuit{clk: clk, circuits: map[string]*circuit{}}
}

// IsOpen implements CircuitStore.
func (m *MemoryCircuit) IsOpen(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.circuits[key]
	if c == nil || c.openUntil.IsZero() {
		return false, nil
	}
	if m.clk.Now().Before(c.openUntil) {
		return true, nil
	}
	delete(m.circuits, key) // cooldown over: start closed and empty
	return false, nil
}

// RecordError implements CircuitStore.
func (m *MemoryCircuit) RecordError(_ context.Context, key string, p CircuitPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clk.Now()
	c := m.circuits[key]
	if c == nil {
		c = &circuit{}
		m.circuits[key] = c
	}
	if c.errors == 0 || now.Sub(c.windowStart) >= p.Window {
		c.errors, c.windowStart = 0, now
	}
	c.errors++
	if c.errors >= p.MaxErrors {
		c.openUntil = now.Add(p.Cooldown)
	}
	return nil
}

// Reset implements CircuitStore.
func (m *MemoryCircuit) Reset(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.circuits, key)
	return nil
}
