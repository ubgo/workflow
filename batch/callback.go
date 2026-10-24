package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/ubgo/workflow"
)

// DefaultCallbackRetry is how long a callback enqueue is claimed before
// another attempt may take it over. It must outlast one Enqueue, or two
// attempts could overlap; thirty seconds is far beyond any healthy enqueue
// and short enough that a failed Then or Finally runs within a minute of
// the engine recovering. Change it with WithCallbackRetry.
const DefaultCallbackRetry = 30 * time.Second

// CallbackKind names one of a batch's callbacks.
type CallbackKind string

// The callbacks, in the order they are enqueued when several are decided
// at once. Before is decided only when the batch is created, Progress once
// per job end reported by a run.
const (
	CallbackBefore   CallbackKind = "before"
	CallbackProgress CallbackKind = "progress"
	CallbackCatch    CallbackKind = "catch"
	CallbackThen     CallbackKind = "then"
	CallbackFinally  CallbackKind = "finally"
)

// PendingCallback is a callback that was decided but whose Enqueue has not
// yet succeeded. The decision itself stays once-only (State's latches); a
// pending entry only carries that decision until the Enqueue lands, so a
// failed enqueue is retried instead of lost.
type PendingCallback struct {
	Kind     CallbackKind
	Workflow string

	// Job is the index of the job whose end decided a Progress callback;
	// zero for the other kinds.
	Job int `json:",omitempty"`

	// Round is State.Round when the callback was decided; it keeps the
	// idempotency keys of callbacks from different RetryFailed rounds
	// apart.
	Round int `json:",omitempty"`

	// Attempts counts the enqueues started, including one in flight.
	Attempts int

	// RetryAt is when the current attempt's claim ends: until then no
	// other caller starts an attempt, so two never overlap. An attempt
	// that fails leaves it, so the next try comes after the claim.
	RetryAt time.Time

	// LastError is the latest failed attempt's error, for operators.
	LastError string `json:",omitempty"`
}

// WithCallbackRetry sets how long a callback enqueue is claimed before it
// may be retried (see DefaultCallbackRetry). A value that is not positive
// keeps the default.
func WithCallbackRetry(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.callbackRetry = d
		}
	}
}

// WithManualCallbackRetry stops Attach from starting the callback retry
// loop it adds when no Source is a poller. Use it only when the app calls
// Manager.RetryCallbacks on its own schedule; without either, a callback
// that fails after the batch's last job end is never retried.
func WithManualCallbackRetry() Option { return func(m *Manager) { m.manualRetry = true } }

// retryLoop calls RetryCallbacks and ResumeDispatches every callbackRetry
// on the Manager's clock until ctx is cancelled. Each tick is two
// Store.Unfinished reads; it enqueues only callbacks whose claim has ended
// and resumes only dispatches whose lease has ended.
func (m *Manager) retryLoop(ctx context.Context) {
	for {
		t := m.clk.NewTimer(m.callbackRetry)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C():
		}
		if err := m.RetryCallbacks(ctx); err != nil {
			m.log.ErrorContext(ctx, "batch: retry callbacks", "error", err)
		}
		if err := m.ResumeDispatches(ctx); err != nil {
			m.log.ErrorContext(ctx, "batch: resume dispatches", "error", err)
		}
	}
}

// claimCallbacks runs inside Store.Update: it queues the callbacks eff
// decided and claims every pending callback that is due, returning the
// ones this caller must enqueue now.
func (m *Manager) claimCallbacks(s *State, eff effects, now time.Time) []PendingCallback {
	if s.Progress != "" {
		for _, i := range eff.progress {
			s.Pending = append(s.Pending, PendingCallback{Kind: CallbackProgress, Workflow: s.Progress, Job: i, Round: s.Round})
		}
	}
	for _, cb := range []struct {
		fire bool
		kind CallbackKind
		name string
	}{{eff.catch, CallbackCatch, s.Catch}, {eff.then, CallbackThen, s.Then}, {eff.finally, CallbackFinally, s.Finally}} {
		if cb.fire && cb.name != "" {
			s.Pending = append(s.Pending, PendingCallback{Kind: cb.kind, Workflow: cb.name, Round: s.Round})
		}
	}
	var claimed []PendingCallback
	for i := range s.Pending {
		p := &s.Pending[i]
		if p.RetryAt.After(now) {
			continue
		}
		p.Attempts++
		p.RetryAt = now.Add(m.callbackRetry)
		claimed = append(claimed, *p)
	}
	return claimed
}

// enqueueCallbacks enqueues the claimed callbacks, then records the
// outcome: a callback that landed leaves Pending, one that failed keeps
// its claim and error and is retried once the claim ends. When recording
// fails, the landed callbacks stay pending and are enqueued again; the
// idempotency key keeps that from doubling them on a driver with
// workflow.CapIdempotencyKey.
func (m *Manager) enqueueCallbacks(ctx context.Context, st State, claimed []PendingCallback) error {
	if len(claimed) == 0 {
		return nil
	}
	var errs []error
	landed := map[callbackID]bool{}
	failed := map[callbackID]string{}
	for _, cb := range claimed {
		if err := m.callback(ctx, st, cb); err != nil {
			errs = append(errs, err)
			failed[cb.id()] = err.Error()
			continue
		}
		landed[cb.id()] = true
	}
	_, err := m.store.Update(ctx, st.ID, func(s *State) error {
		s.Pending = slices.DeleteFunc(s.Pending, func(p PendingCallback) bool { return landed[p.id()] })
		for i := range s.Pending {
			if msg, ok := failed[s.Pending[i].id()]; ok {
				s.Pending[i].LastError = msg
			}
		}
		return nil
	})
	if err != nil {
		errs = append(errs, fmt.Errorf("batch %s: record callback outcome: %w", st.ID, err))
	}
	return errors.Join(errs...)
}

// callbackID tells pending callbacks apart: one Progress per job end, and
// one of every kind per RetryFailed round.
type callbackID struct {
	kind       CallbackKind
	job, round int
}

func (p PendingCallback) id() callbackID { return callbackID{p.Kind, p.Job, p.Round} }

// callbackKeyPrefix starts every callback's idempotency key. The full key,
// "batch:<id>:<kind>", is a documented contract (package doc): an engine
// dedups a retried callback by it, so changing its shape lets a callback
// enqueued before an upgrade run again after it.
const callbackKeyPrefix = "batch:"

// callbackKey is the idempotency key of batch id's callback cb: one per
// batch and kind, so every retry of that callback carries the same key.
// A Progress key adds ":<job>", and a callback decided in RetryFailed
// round n > 0 adds ":r<n>", so neither is deduplicated against another
// callback; the keys of round 0 keep the original shape.
func callbackKey(id ID, cb PendingCallback) string {
	key := callbackKeyPrefix + string(id) + ":" + string(cb.Kind)
	if cb.Kind == CallbackProgress {
		key += ":" + strconv.Itoa(cb.Job)
	}
	if cb.Round > 0 {
		key += ":r" + strconv.Itoa(cb.Round)
	}
	return key
}

// callback enqueues one callback with the batch's progress as payload.
func (m *Manager) callback(ctx context.Context, st State, cb PendingCallback) error {
	payload, err := json.Marshal(progressOf(st))
	if err != nil {
		return err
	}
	job := workflow.Job{Workflow: cb.Workflow, Payload: payload, Labels: callbackLabels(st.Labels)}
	if m.client.Capabilities().Has(workflow.CapIdempotencyKey) {
		job.IdempotencyKey = callbackKey(st.ID, cb)
	}
	if _, err := m.client.Enqueue(owned(ctx), job); err != nil {
		return fmt.Errorf("batch %s: enqueue %s callback %q: %w", st.ID, cb.Kind, cb.Workflow, err)
	}
	return nil
}

// RetryCallbacks enqueues every pending callback whose claim has ended,
// across every batch in the store, and returns the errors joined. The
// status poller calls it each tick, and every recorded job end retries its
// own batch's callbacks. Without a poller, Attach starts a loop that calls
// it (see WithManualCallbackRetry), because once a batch has finished no
// further job end arrives to retry a failed Finally.
func (m *Manager) RetryCallbacks(ctx context.Context) error {
	open, err := m.store.Unfinished(ctx)
	if err != nil {
		return fmt.Errorf("batch: list batches with pending callbacks: %w", err)
	}
	now := m.clk.Now()
	var errs []error
	for _, st := range open {
		if !hasDue(st, now) {
			continue
		}
		errs = append(errs, m.retryBatch(ctx, st.ID))
	}
	return errors.Join(errs...)
}

// retryBatch claims and enqueues one batch's due callbacks.
func (m *Manager) retryBatch(ctx context.Context, id ID) error {
	var claimed []PendingCallback
	st, err := m.store.Update(ctx, id, func(s *State) error {
		claimed = m.claimCallbacks(s, effects{}, m.clk.Now())
		return nil
	})
	if err != nil {
		return err
	}
	return m.enqueueCallbacks(ctx, st, claimed)
}

func hasDue(st State, now time.Time) bool {
	for _, p := range st.Pending {
		if !p.RetryAt.After(now) {
			return true
		}
	}
	return false
}
