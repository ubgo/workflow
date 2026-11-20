package events

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// LabelReplay is the replay ID on every run Replay starts, beside
// LabelEvent, LabelEventID and LabelSubscription. Like them it is under
// the reserved internal prefix: Publish refuses it in Message.Labels and
// the bus alone sets it.
const LabelReplay = workflow.LabelPrefixInternal + "replay"

// EventLog keeps every message a Bus publishes, for Replay (EV-04). It is
// an interface so the log can live where the app's data does (a table, an
// object store, a stream); MemoryLog is the in-process default.
//
// Contract: Append stores a copy of m at time at and returns only once it
// is stored. Range calls fn for every stored message whose time is in
// [from, to) (a zero bound is open), in time order and, for equal times,
// in append order; it stops at and returns fn's first error, and returns
// ctx's error when ctx ends. A message may be stored more than once (a
// publish retried with the same ID); Replay delivers each ID once.
type EventLog interface {
	Append(ctx context.Context, m Message, at time.Time) error
	Range(ctx context.Context, from, to time.Time, fn func(m Message, at time.Time) error) error
}

// WithLog makes Publish append every message it accepts to log, before
// delivering it and whether or not any subscription matches, so Replay
// can deliver it again later. Messages Publish refuses (no Name, reserved
// labels, a version newer than current) are not logged. An Append error
// fails the Publish before anything is delivered.
func WithLog(log EventLog) Option { return func(b *Bus) { b.log = log } }

// WithClock sets the clock that stamps log entries. The default is
// clock.Real.
func WithClock(c clock.Clock) Option { return func(b *Bus) { b.clk = c } }

// MemoryLog is the in-process EventLog: entries in a slice in one
// process, lost on restart, and never pruned unless Prune is called. It
// suits tests and single-process apps that replay within a run; a fleet
// or a replay after a restart needs a durable EventLog. Safe for
// concurrent use.
type MemoryLog struct {
	mu      sync.Mutex
	entries []logEntry
}

type logEntry struct {
	m  Message
	at time.Time
}

var _ EventLog = (*MemoryLog)(nil)

// NewMemoryLog returns an empty MemoryLog.
func NewMemoryLog() *MemoryLog { return &MemoryLog{} }

// cloneMessage copies m's payload and labels, so a stored message is not
// changed by its publisher or reader.
func cloneMessage(m Message) Message {
	m.Payload = slices.Clone(m.Payload)
	m.Labels = maps.Clone(m.Labels)
	return m
}

// Append stores a copy of m at at, keeping time order (after any entry at
// the same time).
func (l *MemoryLog) Append(ctx context.Context, m Message, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	i := sort.Search(len(l.entries), func(i int) bool { return l.entries[i].at.After(at) })
	l.entries = slices.Insert(l.entries, i, logEntry{m: cloneMessage(m), at: at})
	return nil
}

// Range calls fn for each entry in [from, to), without holding the log's
// lock, so fn may publish or append.
func (l *MemoryLog) Range(ctx context.Context, from, to time.Time, fn func(Message, time.Time) error) error {
	l.mu.Lock()
	var picked []logEntry
	for _, e := range l.entries {
		if (!from.IsZero() && e.at.Before(from)) || (!to.IsZero() && !e.at.Before(to)) {
			continue
		}
		picked = append(picked, e)
	}
	l.mu.Unlock()
	for _, e := range picked {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(cloneMessage(e.m), e.at); err != nil {
			return err
		}
	}
	return nil
}

// Len returns how many entries the log holds.
func (l *MemoryLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Prune drops the entries stored before before and returns how many it
// dropped: retention for a long-lived process.
func (l *MemoryLog) Prune(before time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	i := sort.Search(len(l.entries), func(i int) bool { return !l.entries[i].at.Before(before) })
	l.entries = slices.Delete(l.entries, 0, i)
	return i
}

// ReplayOptions selects what Replay delivers again.
type ReplayOptions struct {
	// From and To bound the log time to [From, To); a zero bound is
	// open.
	From, To time.Time

	// Filter, when set, must return true for a logged message to be
	// replayed. It sees the message as logged, before upcasting.
	Filter func(Message) bool

	// Subscriptions names the subscriptions to deliver to; empty means
	// every subscription. A name the bus does not have, or a cancel
	// subscription's, is refused: cancels and diverts are never replayed,
	// because cancelling later would hit runs that did not exist when the
	// event happened.
	Subscriptions []string

	// ID identifies the replay. Replaying again with the same ID starts
	// nothing a previous replay with that ID started (see Replay); a new
	// ID delivers everything again. Empty means a new ID from the bus's
	// ID function, so every such call delivers again.
	ID string
}

// ReplayDelivery is what one subscription did with one replayed message.
type ReplayDelivery struct {
	MessageID    string
	Subscription string
	Ref          workflow.RunRef

	// Existing reports that a run an earlier replay with this ID started
	// was found (Ref is it) and nothing was enqueued.
	Existing bool

	Err error
}

// ReplayResult is the outcome of Replay.
type ReplayResult struct {
	// ID is the replay ID, the one given or the one the bus made.
	ID string

	// Messages is how many distinct message IDs in the range passed
	// Filter.
	Messages int

	// Deliveries has one entry per (message, subscription) pair that
	// matched, in log order then subscription order.
	Deliveries []ReplayDelivery
}

// Replay delivers logged messages again (EV-04): each message in the
// log's [From, To) range that passes Filter is offered to the selected
// subscriptions exactly as Publish would (event name, Filter, upcasting,
// Job), but under an idempotency key that adds the replay ID
// ("event/<subscription>/<id>/replay/<replay ID>") and with LabelReplay
// set, so the original delivery's deduplication does not swallow it.
// Replay does not run cancel subscriptions or diverts, does not signal,
// and does not append to the log. A message ID logged more than once is
// delivered once.
//
// Repeating a replay with the same ID starts nothing twice: while the
// first replay's runs are unfinished the idempotency key holds them on
// every driver; on a client that also implements workflow.RunQuerier
// (CapRunQuery: memory, sqlstore, Redis) Replay first looks for a run
// labelled with the message, subscription and replay ID in any status
// and reports it as Existing, so a finished replayed run is not started
// again either, until retention deletes it.
//
// It needs WithLog, else it returns ErrInvalid, as it does for From not
// before To and for an unknown subscription name. Every other failure is
// per delivery: in its Err and joined into the returned error, and the
// replay goes on, so calling it again with the same ID retries only the
// failures.
func (b *Bus) Replay(ctx context.Context, o ReplayOptions) (ReplayResult, error) {
	if b.log == nil {
		return ReplayResult{}, fmt.Errorf("%w: events: Replay needs an EventLog (WithLog)", workflow.ErrInvalid)
	}
	if !o.From.IsZero() && !o.To.IsZero() && !o.From.Before(o.To) {
		return ReplayResult{}, fmt.Errorf("%w: events: Replay From %s is not before To %s", workflow.ErrInvalid, o.From, o.To)
	}
	b.mu.RLock()
	subs := slices.Clone(b.subs)
	cancels := slices.Clone(b.cancels)
	b.mu.RUnlock()
	if len(o.Subscriptions) > 0 {
		var picked []Subscription
		for _, name := range o.Subscriptions {
			i := slices.IndexFunc(subs, func(s Subscription) bool { return s.Name == name })
			if i < 0 {
				if slices.ContainsFunc(cancels, func(c CancelSubscription) bool { return c.Name == name }) {
					return ReplayResult{}, fmt.Errorf("%w: events: Replay: %q is a cancel subscription; cancels are not replayed", workflow.ErrInvalid, name)
				}
				return ReplayResult{}, fmt.Errorf("%w: events: Replay: no subscription %q", workflow.ErrInvalid, name)
			}
			picked = append(picked, subs[i])
		}
		subs = picked
	}
	out := ReplayResult{ID: o.ID}
	if out.ID == "" {
		id, err := b.newID()
		if err != nil {
			return ReplayResult{}, fmt.Errorf("events: new replay ID: %w", err)
		}
		out.ID = id
	}
	querier, _ := b.client.(workflow.RunQuerier)
	if !b.client.Capabilities().Has(workflow.CapRunQuery) {
		querier = nil
	}
	seen := map[string]bool{}
	var errs []error
	err := b.log.Range(ctx, o.From, o.To, func(m Message, _ time.Time) error {
		if seen[m.ID] || (o.Filter != nil && !o.Filter(m)) {
			return nil
		}
		seen[m.ID] = true
		out.Messages++
		um, err := b.upcast(m)
		for _, s := range subs {
			if s.Event != m.Name {
				continue
			}
			if err != nil {
				// The upcast failed: the message cannot be offered to
				// its subscriptions' Filters at all.
				errs = append(errs, fmt.Errorf("events: replay of %q to %q: %w", m.ID, s.Name, err))
				out.Deliveries = append(out.Deliveries, ReplayDelivery{MessageID: m.ID, Subscription: s.Name, Err: err})
				continue
			}
			if s.Filter != nil && !s.Filter(um) {
				continue
			}
			d := b.replayOne(ctx, querier, s, um, out.ID)
			if d.Err != nil {
				errs = append(errs, fmt.Errorf("events: replay of %q to %q: %w", m.ID, s.Name, d.Err))
			}
			out.Deliveries = append(out.Deliveries, d)
		}
		return nil
	})
	if err != nil {
		errs = append(errs, fmt.Errorf("events: replay: read log: %w", err))
	}
	return out, errors.Join(errs...)
}

// replayOne delivers m to s under replayID, after looking for a run an
// earlier replay with this ID started when querier is set.
func (b *Bus) replayOne(ctx context.Context, querier workflow.RunQuerier, s Subscription, m Message, replayID string) ReplayDelivery {
	d := ReplayDelivery{MessageID: m.ID, Subscription: s.Name}
	if querier != nil {
		page, err := querier.Runs(ctx, workflow.RunQuery{Limit: 1, Labels: map[string]string{
			LabelEventID: m.ID, LabelSubscription: s.Name, LabelReplay: replayID,
		}})
		if err != nil {
			d.Err = fmt.Errorf("look for an earlier replayed run: %w", err)
			return d
		}
		if len(page.Runs) > 0 {
			d.Ref, d.Existing = page.Runs[0].Ref, true
			return d
		}
	}
	d.Ref, d.Err = b.deliver(ctx, s, m, replayID)
	return d
}
