// Package events starts runs from events: publish/subscribe inside the
// library (EV-01) and an inbound webhook handler that verifies a
// signature and enqueues (EV-03), with HMAC signing schemes for both
// directions (WH-03).
//
// A Bus is not a message broker. Publish turns one event into one Enqueue
// per matching subscription on an ordinary workflow.Client, so an event
// is exactly as durable as the client's runs: on a durable driver, once
// Publish returns, every subscriber's run is stored. Subscriptions are
// code, registered at boot in every process that publishes, as workflows
// are. Delivery is at least once per subscription and deduplicated by the
// event's ID: each run carries the idempotency key
// "event/<subscription>/<id>", so publishing the same ID again (a retry
// after a timeout, a webhook delivered twice) starts nothing new while
// the first run is unfinished.
package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/ubgo/workflow"
)

// Labels every run started by Publish carries. Publish refuses reserved
// keys in Message.Labels and in the labels a Subscription.Job returns, and
// sets these itself, overwriting any value a job carried, and enqueues
// under workflow.AllowReservedLabels for these three keys only: they are
// refused on a public Enqueue unless the owner opts in
// (workflow.CheckJobLabels, on drivers that enforce it), so they cannot
// arrive by accident or from untrusted input. Like every reserved label
// they are a guard, not a sandbox.
const (
	// LabelEvent is the event's Name.
	LabelEvent = workflow.LabelPrefixInternal + "event"

	// LabelEventID is the event's ID.
	LabelEventID = workflow.LabelPrefixInternal + "event-id"

	// LabelSubscription is the subscription that started the run.
	LabelSubscription = workflow.LabelPrefixInternal + "subscription"
)

// Message is one event.
type Message struct {
	// ID identifies the event for deduplication; publish the same event
	// again with the same ID. Empty means the Bus makes a new one, so
	// that publish is never deduplicated.
	ID string

	// Name is what subscriptions match on, exactly ("order.paid").
	Name string

	// Key is passed to Signal under WithSignal (the WaitSpec key a waiting
	// run matches), and is there for Filter.
	Key string

	// Payload becomes each subscriber's Job.Payload (unless the
	// subscription's Job builds the job itself).
	Payload []byte

	// Labels are copied onto each subscriber's job, beside the bus's own.
	// Keys under a reserved prefix are refused.
	Labels map[string]string
}

// Subscription starts a run of Workflow for every published message
// whose Name is Event and that Filter accepts.
type Subscription struct {
	// Name identifies the subscription; unique within a Bus and stable
	// across deploys, because it is part of every run's idempotency key.
	Name string

	// Event is the message Name it matches.
	Event string

	// Workflow is the workflow to run. Ignored when Job is set.
	Workflow string

	// Filter, when set, must return true for the message to start a run.
	Filter func(Message) bool

	// Job, when set, builds the job from the message (a different payload,
	// a queue, a priority, RunAt). Reserved label keys in it are refused
	// (ErrInvalid). The bus still adds its labels and,
	// when the job has none, its idempotency key. Nil means
	// Job{Workflow: Workflow, Payload: m.Payload}.
	Job func(Message) (workflow.Job, error)
}

// Delivery is what one subscription did with one message.
type Delivery struct {
	Subscription string
	Ref          workflow.RunRef
	Err          error
}

// Published is the outcome of Publish.
type Published struct {
	// ID is the message's ID, the one given or the one the bus made.
	ID string

	// Deliveries has one entry per subscription the message matched, in
	// the order they were subscribed.
	Deliveries []Delivery

	// Signalled reports whether the message was also signalled to
	// waiting runs (WithSignal) without error.
	Signalled bool
}

// Option configures a Bus.
type Option func(*Bus)

// WithSignal makes Publish also deliver each message to runs waiting on
// it, as client.Signal(Event{Name, Key, Payload}) would, so a run's wait
// step and a new subscriber's run see the same event.
func WithSignal() Option { return func(b *Bus) { b.signal = true } }

// WithIDFunc sets how the bus makes an ID for a message published
// without one (a UUIDv7 or ULID generator). The default is 16 random
// bytes in hex.
func WithIDFunc(f func() (string, error)) Option { return func(b *Bus) { b.newID = f } }

// AllowDuplicates lets New accept a client without
// CapIdempotencyKey; on it, publishing an ID twice starts its runs twice.
func AllowDuplicates() Option { return func(b *Bus) { b.allowDup = true } }

// Bus routes published messages to subscriptions. It is safe for
// concurrent use; Subscribe may be called at any time.
type Bus struct {
	client   workflow.Client
	signal   bool
	allowDup bool
	newID    func() (string, error)

	mu   sync.RWMutex
	subs []Subscription
}

// New returns a bus that enqueues on client. It refuses a client without
// CapIdempotencyKey with an UnsupportedError, unless AllowDuplicates is
// given, because deduplication by ID is what makes a republish safe.
func New(client workflow.Client, opts ...Option) (*Bus, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: events: client is nil", workflow.ErrInvalid)
	}
	b := &Bus{client: client, newID: randomID}
	for _, o := range opts {
		o(b)
	}
	if b.newID == nil {
		b.newID = randomID
	}
	if !b.allowDup && !client.Capabilities().Has(workflow.CapIdempotencyKey) {
		return nil, &workflow.UnsupportedError{Field: "events.Bus", Missing: workflow.CapIdempotencyKey,
			Reason: "a republished event would start its runs twice; pass AllowDuplicates to accept that"}
	}
	return b, nil
}

// randomID returns 16 random bytes in hex.
func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Subscribe adds s. It returns an error wrapping ErrInvalid when s has no
// Name or Event, has neither Workflow nor Job, or reuses a Name.
func (b *Bus) Subscribe(s Subscription) error {
	if s.Name == "" || s.Event == "" {
		return fmt.Errorf("%w: events: subscription needs a Name and an Event", workflow.ErrInvalid)
	}
	if s.Workflow == "" && s.Job == nil {
		return fmt.Errorf("%w: events: subscription %q needs a Workflow or a Job", workflow.ErrInvalid, s.Name)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if slices.ContainsFunc(b.subs, func(x Subscription) bool { return x.Name == s.Name }) {
		return fmt.Errorf("%w: events: subscription %q already exists", workflow.ErrInvalid, s.Name)
	}
	b.subs = append(b.subs, s)
	return nil
}

// Subscriptions returns the names of the subscriptions, in order.
func (b *Bus) Subscriptions() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, len(b.subs))
	for i, s := range b.subs {
		names[i] = s.Name
	}
	return names
}

// Publish starts one run per subscription matching m, and signals m to
// waiting runs under WithSignal. Every matching subscription is tried even
// when one fails; the error joins each failure (its Delivery holds it
// too), so the caller publishes again with the same ID and only the
// deliveries that failed start a run. A message matching nothing is not
// an error. m.Name is required and reserved labels in m.Labels are
// refused, both with ErrInvalid.
func (b *Bus) Publish(ctx context.Context, m Message) (Published, error) {
	if m.Name == "" {
		return Published{}, fmt.Errorf("%w: events: message has no Name", workflow.ErrInvalid)
	}
	if err := workflow.ValidateLabels(m.Labels); err != nil {
		return Published{}, err
	}
	if m.ID == "" {
		id, err := b.newID()
		if err != nil {
			return Published{}, fmt.Errorf("events: new message ID: %w", err)
		}
		m.ID = id
	}
	b.mu.RLock()
	subs := slices.Clone(b.subs)
	b.mu.RUnlock()

	out := Published{ID: m.ID}
	var errs []error
	for _, s := range subs {
		if s.Event != m.Name || (s.Filter != nil && !s.Filter(m)) {
			continue
		}
		ref, err := b.deliver(ctx, s, m)
		if err != nil {
			err = fmt.Errorf("events: subscription %q: %w", s.Name, err)
			errs = append(errs, err)
		}
		out.Deliveries = append(out.Deliveries, Delivery{Subscription: s.Name, Ref: ref, Err: err})
	}
	if b.signal {
		if err := b.client.Signal(ctx, workflow.Event{Name: m.Name, Key: m.Key, Payload: m.Payload}); err != nil {
			errs = append(errs, fmt.Errorf("events: signal %q: %w", m.Name, err))
		} else {
			out.Signalled = true
		}
	}
	return out, errors.Join(errs...)
}

// deliver builds the job for one subscription and enqueues it.
func (b *Bus) deliver(ctx context.Context, s Subscription, m Message) (workflow.RunRef, error) {
	job := workflow.Job{Workflow: s.Workflow, Payload: m.Payload}
	if s.Job != nil {
		var err error
		if job, err = s.Job(m); err != nil {
			return workflow.RunRef{}, err
		}
		if err := workflow.ValidateLabels(job.Labels); err != nil {
			return workflow.RunRef{}, err
		}
	}
	labels := maps.Clone(job.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	maps.Copy(labels, m.Labels)
	labels[LabelEvent] = m.Name
	labels[LabelEventID] = m.ID
	labels[LabelSubscription] = s.Name
	job.Labels = labels
	if job.IdempotencyKey == "" {
		job.IdempotencyKey = "event/" + s.Name + "/" + m.ID
	}
	return b.client.Enqueue(workflow.AllowReservedLabels(ctx, LabelEvent, LabelEventID, LabelSubscription), job)
}
