package middleware

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/ubgo/workflow"
)

// ErrPoolBusy matches the error an attempt fails with under
// PoolRetryAfter when every slot of its pool is taken: a retryable error
// marked with workflow.RetryAfter. Under the default the step snoozes
// instead (workflow.ErrSnoozed), which is not a failure.
var ErrPoolBusy = errors.New("workflow/middleware: no free slot in the pool")

// DefaultPoolRetry is how long a step that found no free slot waits
// before it tries again, unless PoolRetry says otherwise.
const DefaultPoolRetry = time.Second

// PoolOption configures Pool and CriticalSection.
type PoolOption func(*poolConfig)

type poolConfig struct {
	retry      time.Duration
	retryAfter bool
	steps      []string
}

// PoolRetry sets how long a step that found no free slot waits before
// it tries again (default DefaultPoolRetry; zero or less keeps it).
func PoolRetry(d time.Duration) PoolOption {
	return func(c *poolConfig) {
		if d > 0 {
			c.retry = d
		}
	}
}

// PoolRetryAfter makes a busy pool fail the attempt with
// workflow.RetryAfter(ErrPoolBusy, retry) instead of a workflow.Snooze,
// for a driver without CapSnooze (Hatchet). It spends an attempt each
// time, so the step needs retries to spare, and the retry delay is
// honoured only under a policy whose DelayFor reads it
// (workflow.RetryAfterDelay, needs CapRetryDelay); otherwise the
// policy's Backoff applies.
func PoolRetryAfter() PoolOption { return func(c *poolConfig) { c.retryAfter = true } }

// PoolSteps limits the pool to the named steps; other steps run without
// a slot. With no names, every step the middleware wraps takes one.
func PoolSteps(steps ...string) PoolOption {
	return func(c *poolConfig) { c.steps = append(c.steps, steps...) }
}

// Pool is step middleware that lets at most slots attempts hold the pool
// name at once, across every workflow, worker and process that shares
// store: a licence server with five seats, a GPU box with two cards, a
// partner API that allows three connections. Before the step runs it
// tries the slot keys "pool/<name>/0" to "pool/<name>/<slots-1>" with
// store.Acquire under a token of its own; holding one, it runs the step
// and releases the slot when the step returns, whatever the outcome.
// When every slot is taken, the step does not run: it returns
// workflow.Snooze(PoolRetry), which spends no attempt on a driver with
// CapSnooze, or, under PoolRetryAfter, a retryable error matching
// ErrPoolBusy.
//
// ttl bounds how long a slot is held, so a worker that crashes while
// holding one frees it after ttl. A step that runs longer than ttl loses
// its exclusivity: its slot expires and another step may take it while
// it still runs. Set ttl above the step's Timeout. The reach is the
// store's: MemoryUnique limits one process, sqlstore.UniqueStore and the
// Redis UniqueStore every process sharing them. The keys share the
// store's namespace with Unique's, so no unique key should start with
// "pool/" or "section/".
//
// A name that is empty, slots below one or a ttl of zero or less fail
// each attempt with a permanent error wrapping workflow.ErrInvalid; a
// store error fails the attempt (retryable), never runs the step
// without a slot. A slot that could not be released frees itself after
// ttl. It runs on every driver; the snooze needs CapSnooze (memory,
// sqlstore, Redis).
func Pool(store UniqueStore, name string, slots int, ttl time.Duration, opts ...PoolOption) workflow.Middleware {
	cfg := poolConfig{retry: DefaultPoolRetry}
	for _, o := range opts {
		o(&cfg)
	}
	var bad error
	switch {
	case store == nil || name == "":
		bad = fmt.Errorf("%w: Pool needs a store and a name", workflow.ErrInvalid)
	case slots < 1 || ttl <= 0:
		bad = fmt.Errorf("%w: Pool %q needs at least one slot and a positive ttl", workflow.ErrInvalid, name)
	}
	keys := func(workflow.StepInfo) ([]string, error) {
		if bad != nil {
			return nil, workflow.Permanent(bad)
		}
		out := make([]string, slots)
		for i := range out {
			out[i] = "pool/" + name + "/" + strconv.Itoa(i)
		}
		return out, nil
	}
	return slotGate(store, ttl, cfg, false, keys)
}

// CriticalSection is step middleware that keeps the named steps of one
// run from running at the same time, for parallel steps that touch the
// same record: a pool of one slot keyed by the run's ID and section
// ("section/<section>/<run>"), so steps of different runs never wait on
// each other. A step that finds the slot taken snoozes for
// DefaultPoolRetry, as Pool does. With no steps named, every step of the
// run is in the section. ttl, the store's reach and the invalid inputs
// are as for Pool. A ctx without a workflow.StepInfo (a handler called
// outside a driver) fails with workflow.ErrUnsupported, since the run is
// unknown and the section could not hold.
func CriticalSection(store UniqueStore, section string, ttl time.Duration, steps ...string) workflow.Middleware {
	cfg := poolConfig{retry: DefaultPoolRetry, steps: steps}
	var bad error
	switch {
	case store == nil || section == "":
		bad = fmt.Errorf("%w: CriticalSection needs a store and a section name", workflow.ErrInvalid)
	case ttl <= 0:
		bad = fmt.Errorf("%w: CriticalSection %q needs a positive ttl", workflow.ErrInvalid, section)
	}
	keys := func(info workflow.StepInfo) ([]string, error) {
		if bad != nil {
			return nil, workflow.Permanent(bad)
		}
		return []string{"section/" + section + "/" + info.RunID.String()}, nil
	}
	return slotGate(store, ttl, cfg, true, keys)
}

// slotGate runs a step while it holds one of the keys keys returns.
func slotGate(store UniqueStore, ttl time.Duration, cfg poolConfig, needInfo bool, keys func(workflow.StepInfo) ([]string, error)) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := workflow.StepInfoFrom(ctx)
			if len(cfg.steps) > 0 && (!ok || !slices.Contains(cfg.steps, info.Step)) {
				return next(ctx, in)
			}
			if needInfo && !ok {
				return nil, workflow.Unsupported("CriticalSection", "the step's ctx carries no workflow.StepInfo")
			}
			ks, err := keys(info)
			if err != nil {
				return nil, err
			}
			owner, err := newOwnerToken()
			if err != nil {
				return nil, err
			}
			for _, k := range ks {
				got, err := store.Acquire(ctx, k, owner, ttl)
				if err != nil {
					return nil, fmt.Errorf("workflow/middleware: pool acquire: %w", err)
				}
				if !got {
					continue
				}
				defer func() { _ = store.Release(context.WithoutCancel(ctx), k, owner) }()
				return next(ctx, in)
			}
			if cfg.retryAfter {
				return nil, workflow.RetryAfter(fmt.Errorf("%w: %d slot(s) taken", ErrPoolBusy, len(ks)), cfg.retry)
			}
			return nil, workflow.Snooze(cfg.retry)
		}
	}
}
