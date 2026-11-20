package events

import (
	"context"
	"fmt"
	"slices"

	"github.com/ubgo/workflow"
)

// UnfinishedStatuses is the statuses a CancelSubscription cancels by
// default: every status that is not terminal, so a run that is running
// or waiting is cut short too (workflow.CancelAll alone cancels only runs
// not yet started).
var UnfinishedStatuses = []workflow.Status{
	workflow.StatusScheduled, workflow.StatusQueued, workflow.StatusRunning,
	workflow.StatusWaiting, workflow.StatusRetrying,
}

// CancelSubscription cancels runs when an event arrives (EV-05): for every
// published message whose Name is Event and that Filter accepts, it
// cancels every unfinished run whose label Label equals Key(m), through
// workflow.BulkCanceller.CancelAll. "order.cancelled" with Label "order"
// cancels the fulfilment, reminder and shipping runs of that order,
// whatever workflow they belong to.
//
// With Divert set it is an external trigger exception (WX-14): after the
// cancel succeeds, it starts a run of Divert with the message's payload,
// so the process leaves its normal path for the exception path. The
// divert runs whether the cancel found runs or not: a publish retried
// after a crash between the cancel and the divert finds nothing left to
// cancel and must still divert. The divert run carries the bus's labels
// (LabelSubscription is Name, LabelEventID the message ID) and the
// idempotency key "event/<Name>/<id>", like a subscription's run.
//
// A message whose divert run already exists, in any status, was handled:
// Publish looks for that run first (by LabelSubscription and
// LabelEventID) and, when found, neither cancels nor diverts again and
// reports it in Cancellation.Diverted. Without that look-up a republish
// would cancel the divert run itself whenever it carries the cancelled
// label (the message's labels are copied onto it), so a divert needs a
// client with workflow.RunQuerier (CapRunQuery).
type CancelSubscription struct {
	// Name identifies the subscription; unique within the bus across
	// subscriptions and cancel subscriptions, and stable across deploys
	// (it is part of the divert run's idempotency key).
	Name string

	// Event is the message Name it matches.
	Event string

	// Filter, when set, must return true for the message to cancel.
	Filter func(Message) bool

	// Label is the run label to match ("order"). Required.
	Label string

	// Key returns the label value to cancel from the message. Nil means
	// m.Key. A message for which it returns "" cancels nothing and its
	// Cancellation holds an ErrInvalid: an empty value would match every
	// run carrying the label empty.
	Key func(Message) string

	// Workflow, when set, narrows the cancel to runs of that workflow.
	Workflow string

	// Statuses are the statuses cancelled; nil means
	// UnfinishedStatuses. A terminal status is refused.
	Statuses []workflow.Status

	// Divert, when set, is the workflow started after the cancel, with
	// the message's payload. Ignored when DivertJob is set.
	Divert string

	// DivertJob, when set, builds the divert job from the message (a
	// different payload, a queue, RunAt). Reserved label keys in it are
	// refused (ErrInvalid); the bus adds its labels and, when the job has
	// none, its idempotency key.
	DivertJob func(Message) (workflow.Job, error)
}

// Cancellation is what one cancel subscription did with one message.
type Cancellation struct {
	Subscription string

	// Cancelled is how many runs CancelAll cancelled.
	Cancelled int

	// Diverted is the divert run; zero without Divert or when the cancel
	// failed.
	Diverted workflow.RunRef

	// Err is the cancel's or the divert's error.
	Err error
}

// SubscribeCancel adds a cancel subscription (EV-05, WX-14). It refuses,
// with an UnsupportedError naming CapCancelAll, a client that does not
// declare workflow.CapCancelAll and implement workflow.BulkCanceller
// (memory, sqlstore and Redis do; Hatchet does not), so a cancel is never
// emulated or dropped, and a divert on a client without CapRunQuery and
// workflow.RunQuerier (naming CapRunQuery; every driver with CapCancelAll
// has it). It returns an error wrapping ErrInvalid when s has
// no Name, Event or Label, reuses a Name, or asks for a terminal status.
//
// Publish runs cancels before it starts any run, so the runs a message
// starts are never cancelled by the same message, and Replay never runs
// them.
func (b *Bus) SubscribeCancel(s CancelSubscription) error {
	if s.Name == "" || s.Event == "" || s.Label == "" {
		return fmt.Errorf("%w: events: cancel subscription needs a Name, an Event and a Label", workflow.ErrInvalid)
	}
	if _, ok := b.client.(workflow.BulkCanceller); !ok || !b.client.Capabilities().Has(workflow.CapCancelAll) {
		return &workflow.UnsupportedError{Field: "events.CancelSubscription", Missing: workflow.CapCancelAll,
			Reason: "cancelling runs on an event needs CancelAll"}
	}
	if s.Divert != "" || s.DivertJob != nil {
		if _, ok := b.client.(workflow.RunQuerier); !ok || !b.client.Capabilities().Has(workflow.CapRunQuery) {
			return &workflow.UnsupportedError{Field: "events.CancelSubscription.Divert", Missing: workflow.CapRunQuery,
				Reason: "a divert looks up its earlier run so a republish does not cancel it"}
		}
	}
	if err := workflow.ValidateCancelQuery(cancelQuery(s, "x")); err != nil {
		return fmt.Errorf("events: cancel subscription %q: %w", s.Name, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.nameTakenLocked(s.Name) {
		return fmt.Errorf("%w: events: subscription %q already exists", workflow.ErrInvalid, s.Name)
	}
	b.cancels = append(b.cancels, s)
	return nil
}

// cancelQuery is the CancelAll query s runs for label value key.
func cancelQuery(s CancelSubscription, key string) workflow.RunQuery {
	st := s.Statuses
	if len(st) == 0 {
		st = UnfinishedStatuses
	}
	return workflow.RunQuery{Workflow: s.Workflow, Statuses: slices.Clone(st), Labels: map[string]string{s.Label: key}}
}

// runCancels runs every cancel subscription matching m, then every
// divert, and returns one Cancellation per match and the errors.
func (b *Bus) runCancels(ctx context.Context, cancels []CancelSubscription, m Message) ([]Cancellation, []error) {
	var out []Cancellation
	var errs []error
	var divert []int // indexes in out whose cancel succeeded and that divert
	var divertSubs []CancelSubscription
	for _, s := range cancels {
		if s.Event != m.Name || (s.Filter != nil && !s.Filter(m)) {
			continue
		}
		c := Cancellation{Subscription: s.Name}
		diverts := s.Divert != "" || s.DivertJob != nil
		if diverts {
			ref, found, err := b.findDivert(ctx, s, m)
			if err != nil || found {
				c.Diverted = ref
				if err != nil {
					c.Err = fmt.Errorf("events: cancel subscription %q: look for an earlier divert: %w", s.Name, err)
					errs = append(errs, c.Err)
				}
				out = append(out, c)
				continue
			}
		}
		key := m.Key
		if s.Key != nil {
			key = s.Key(m)
		}
		bc, ok := b.client.(workflow.BulkCanceller)
		switch {
		case key == "":
			c.Err = fmt.Errorf("%w: label %q value from message %q is empty", workflow.ErrInvalid, s.Label, m.ID)
		case !ok:
			c.Err = &workflow.UnsupportedError{Field: "events.CancelSubscription", Missing: workflow.CapCancelAll}
		default:
			c.Cancelled, c.Err = bc.CancelAll(ctx, cancelQuery(s, key))
		}
		if c.Err != nil {
			c.Err = fmt.Errorf("events: cancel subscription %q: %w", s.Name, c.Err)
			errs = append(errs, c.Err)
		} else if diverts {
			divert = append(divert, len(out))
			divertSubs = append(divertSubs, s)
		}
		out = append(out, c)
	}
	for i, idx := range divert {
		s := divertSubs[i]
		ref, err := b.deliver(ctx, Subscription{Name: s.Name, Event: s.Event, Workflow: s.Divert, Job: s.DivertJob}, m, "")
		if err != nil {
			err = fmt.Errorf("events: cancel subscription %q: divert: %w", s.Name, err)
			errs = append(errs, err)
		}
		out[idx].Diverted, out[idx].Err = ref, err
	}
	return out, errs
}

// findDivert looks for the divert run s started for m, in any status.
func (b *Bus) findDivert(ctx context.Context, s CancelSubscription, m Message) (workflow.RunRef, bool, error) {
	q, ok := b.client.(workflow.RunQuerier)
	if !ok {
		return workflow.RunRef{}, false, &workflow.UnsupportedError{Field: "events.CancelSubscription.Divert", Missing: workflow.CapRunQuery}
	}
	page, err := q.Runs(ctx, workflow.RunQuery{Limit: 1, Labels: map[string]string{LabelSubscription: s.Name, LabelEventID: m.ID}})
	if err != nil || len(page.Runs) == 0 {
		return workflow.RunRef{}, false, err
	}
	return page.Runs[0].Ref, true, nil
}
