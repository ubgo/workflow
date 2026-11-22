package workflow

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// PromiseEvent is the event name ResolvePromise and RejectPromise signal,
// with the promise's id as the event key, so a wait step
// (WaitSpec{Event: PromiseEvent, Key: id}) resumes when a promise settles.
//
//ds:def id=promiseevent-9gzm4g33 owner=@khanakia stability=stable
const PromiseEvent = "ubgo.workflow.promise"

// ErrPromiseSettled matches the error of a ResolvePromise or RejectPromise
// on a promise that was already settled with a different outcome: the
// first settlement wins.
//
//ds:def id=errpromisesettled-cbm2esnm owner=@khanakia stability=stable
var ErrPromiseSettled = errors.New("workflow: promise already settled")

// PromiseStore keeps settled promises: a value stored once under an id
// and read by anyone who knows the id. Settle is atomic insert-if-absent,
// so of two processes settling one promise at once exactly one wins and
// both learn the stored value; that is what makes a promise resolve once
// across a fleet. MemoryPromises is the in-process store; sqlstore and
// redis ship durable ones.
//
//ds:def id=promisestore-fpf8x3m4 owner=@khanakia stability=stable
type PromiseStore interface {
	// Settle stores val under id unless a value is stored already. It
	// returns the value stored after the call and whether this call
	// stored it.
	Settle(ctx context.Context, id string, val []byte) (stored []byte, first bool, err error)

	// Get returns the value stored under id, and false when none is.
	Get(ctx context.Context, id string) ([]byte, bool, error)

	// Delete removes id's value; deleting an unknown id is not an error.
	Delete(ctx context.Context, id string) error
}

// MemoryPromises is an in-process PromiseStore. The zero value is ready.
type MemoryPromises struct {
	mu   sync.Mutex
	vals map[string][]byte
}

// NewMemoryPromises returns an empty MemoryPromises.
//
//ds:def id=newmemorypromises-capatjkk owner=@khanakia stability=stable
func NewMemoryPromises() *MemoryPromises { return &MemoryPromises{} }

// Settle implements PromiseStore.
func (m *MemoryPromises) Settle(_ context.Context, id string, val []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.vals[id]; ok {
		return bytes.Clone(v), false, nil
	}
	if m.vals == nil {
		m.vals = map[string][]byte{}
	}
	m.vals[id] = bytes.Clone(val)
	return bytes.Clone(val), true, nil
}

// Get implements PromiseStore.
func (m *MemoryPromises) Get(_ context.Context, id string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.vals[id]
	return bytes.Clone(v), ok, nil
}

// Delete implements PromiseStore.
func (m *MemoryPromises) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.vals, id)
	return nil
}

// settlement is what a PromiseStore holds for a settled promise.
type settlement struct {
	OK     bool   `json:"ok"`
	Value  []byte `json:"value,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// PromiseRejectedError is the error of a promise settled by
// RejectPromise.
//
//ds:def id=promiserejectederror-t8d9km2g owner=@khanakia stability=stable
type PromiseRejectedError struct {
	ID     string
	Reason string
}

// Error names the promise and the reason.
func (e *PromiseRejectedError) Error() string {
	return fmt.Sprintf("workflow: promise %q rejected: %s", e.ID, e.Reason)
}

// NewPromiseID returns a random promise id (128 bits, hex), for a caller
// that has no natural id such as an approval request's own key.
//
//ds:def id=newpromiseid-gs7uukr4 owner=@khanakia stability=stable
func NewPromiseID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("workflow: promise id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// ResolvePromise settles promise id with value (DE-13, a promise completed
// from outside: an API handler, a webhook, an operator), unless it is
// settled already: resolving again with the same value is a no-op, and a
// different outcome returns an error matching ErrPromiseSettled. When
// client is not nil it then signals PromiseEvent with key id and the
// value as payload, so a run waiting on it in a wait step resumes at
// once; a run that awaits it with AwaitPromise sees it on its next look.
// The signal is sent even when the promise was settled already (by this
// value), so a signal lost to a crash after the store write is sent again
// by a retried call.
//
//ds:def id=resolvepromise-ej2zjk37 owner=@khanakia stability=stable
func ResolvePromise(ctx context.Context, client Client, store PromiseStore, id string, value []byte) error {
	return settlePromise(ctx, client, store, id, settlement{OK: true, Value: value})
}

// RejectPromise settles promise id as failed with reason, as
// ResolvePromise does for a value; AwaitPromise returns a Permanent
// *PromiseRejectedError for it.
//
//ds:def id=rejectpromise-e6uyyurn owner=@khanakia stability=stable
func RejectPromise(ctx context.Context, client Client, store PromiseStore, id string, reason string) error {
	return settlePromise(ctx, client, store, id, settlement{Reason: reason})
}

//ds:def id=settlepromise-e3d6pbhr owner=@khanakia stability=stable
func settlePromise(ctx context.Context, client Client, store PromiseStore, id string, s settlement) error {
	if store == nil || id == "" {
		return invalidf("settle promise: needs a store and an id")
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("workflow: promise %q: encode: %w", id, err)
	}
	stored, first, err := store.Settle(ctx, id, raw)
	if err != nil {
		return fmt.Errorf("workflow: promise %q: settle: %w", id, err)
	}
	if !first && !bytes.Equal(stored, raw) {
		return fmt.Errorf("%w: promise %q", ErrPromiseSettled, id)
	}
	if client == nil {
		return nil
	}
	payload := s.Value
	if !s.OK {
		payload = raw
	}
	if err := client.Signal(ctx, Event{Name: PromiseEvent, Key: id, Payload: payload}); err != nil {
		return fmt.Errorf("workflow: promise %q: signal: %w", id, err)
	}
	return nil
}

// PromiseResult returns promise id's value once it is settled: settled
// false while it is not, a *PromiseRejectedError when it was rejected.
//
//ds:def id=promiseresult-f3bszagg owner=@khanakia stability=stable
func PromiseResult(ctx context.Context, store PromiseStore, id string) (value []byte, settled bool, err error) {
	if store == nil || id == "" {
		return nil, false, invalidf("promise result: needs a store and an id")
	}
	raw, ok, err := store.Get(ctx, id)
	if err != nil {
		return nil, false, fmt.Errorf("workflow: promise %q: read: %w", id, err)
	}
	if !ok {
		return nil, false, nil
	}
	var s settlement
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, true, fmt.Errorf("workflow: promise %q: stored value: %w", id, err)
	}
	if !s.OK {
		return nil, true, &PromiseRejectedError{ID: id, Reason: s.Reason}
	}
	return s.Value, true, nil
}

// DefaultPromisePoll is how long AwaitPromise snoozes between looks.
//
//ds:def id=defaultpromisepoll-2vu4hbw2 owner=@khanakia stability=stable
const DefaultPromisePoll = 5 * time.Second

// AwaitPromise returns promise id's value from inside a step, waiting
// without holding a worker: while the promise is unsettled it returns
// Snooze(poll) (DefaultPromisePoll when poll is zero), so the step looks
// again later without spending an attempt, which needs a driver with
// CapSnooze. A rejected promise fails the step with a Permanent
// *PromiseRejectedError. A wait step on PromiseEvent with key id is the
// other way to wait, woken by the settlement's signal instead of a poll.
//
//ds:def id=awaitpromise-338jz9js owner=@khanakia stability=stable
func AwaitPromise(ctx context.Context, store PromiseStore, id string, poll time.Duration) ([]byte, error) {
	v, settled, err := PromiseResult(ctx, store, id)
	var rej *PromiseRejectedError
	switch {
	case errors.As(err, &rej):
		return nil, Permanent(err)
	case err != nil:
		return nil, err
	case !settled:
		if poll <= 0 {
			poll = DefaultPromisePoll
		}
		return nil, Snooze(poll)
	}
	return v, nil
}

// WaitPromise blocks outside a step (an HTTP handler, a CLI) until promise
// id settles or ctx ends, asking the store every opts.Poll, and returns
// its value or its *PromiseRejectedError.
//
//ds:def id=waitpromise-4tdkst6u owner=@khanakia stability=stable
func WaitPromise(ctx context.Context, store PromiseStore, id string, opts AwaitOptions) ([]byte, error) {
	for {
		v, settled, err := PromiseResult(ctx, store, id)
		if err != nil || settled {
			return v, err
		}
		if err := opts.sleep(ctx); err != nil {
			return nil, err
		}
	}
}
