package events_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
)

// waitStatus polls until ref reaches want.
func waitStatus(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	for i := 0; ; i++ {
		st, err := c.Status(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if st == want {
			return
		}
		if i > 2000 {
			t.Fatalf("run %v is %s, want %s", ref, st, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestCancelOnEvent: a message cancels every unfinished run, running ones
// included, whose label matches the message's key, across workflows; runs
// of another key, and the runs the same message starts, are untouched.
func TestCancelOnEvent(t *testing.T) {
	ctx := context.Background()
	var rec recorder
	c := started(t, holdWorkflow(t, "fulfil"), holdWorkflow(t, "remind"), rec.workflow("audit"))
	enq := func(wf, order string) workflow.RunRef {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf, Labels: map[string]string{"order": order}})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	f42, r42, f43 := enq("fulfil", "42"), enq("remind", "42"), enq("fulfil", "43")
	waitStatus(t, c, f42, workflow.StatusRunning)
	waitStatus(t, c, r42, workflow.StatusRunning)

	b, err := events.New(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SubscribeCancel(events.CancelSubscription{Name: "stop-order", Event: "order.cancelled", Label: "order"}); err != nil {
		t.Fatal(err)
	}
	// Subscribed after the cancel, and its run carries order=42 too: the
	// cancel still runs first, so this run is not cancelled.
	if err := b.Subscribe(events.Subscription{Name: "audit", Event: "order.cancelled", Workflow: "audit"}); err != nil {
		t.Fatal(err)
	}
	p, err := b.Publish(ctx, events.Message{ID: "c1", Name: "order.cancelled", Key: "42", Labels: map[string]string{"order": "42"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Cancellations) != 1 || p.Cancellations[0].Cancelled != 2 || p.Cancellations[0].Err != nil {
		t.Fatalf("cancellations = %+v; want 2 runs cancelled", p.Cancellations)
	}
	waitStatus(t, c, f42, workflow.StatusCancelled)
	waitStatus(t, c, r42, workflow.StatusCancelled)
	waitStatus(t, c, p.Deliveries[0].Ref, workflow.StatusSucceeded)
	if st, _ := c.Status(ctx, f43); st.IsTerminal() {
		t.Fatalf("order 43's run is %s; another key must be untouched", st)
	}
}

// TestCancelNarrowing: Key, Workflow and Statuses narrow what is
// cancelled; an empty key cancels nothing and reports ErrInvalid.
func TestCancelNarrowing(t *testing.T) {
	ctx := context.Background()
	c := started(t, holdWorkflow(t, "fulfil"), holdWorkflow(t, "remind"))
	enq := func(wf, order string) workflow.RunRef {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf, Labels: map[string]string{"order": order}})
		if err != nil {
			t.Fatal(err)
		}
		waitStatus(t, c, ref, workflow.StatusRunning)
		return ref
	}
	f, r := enq("fulfil", "7"), enq("remind", "7")
	b, _ := events.New(c)
	err := errors.Join(
		b.SubscribeCancel(events.CancelSubscription{Name: "only-remind", Event: "e", Label: "order", Workflow: "remind",
			Key: func(m events.Message) string { return string(m.Payload) }}),
		b.SubscribeCancel(events.CancelSubscription{Name: "only-queued", Event: "e", Label: "order", Statuses: []workflow.Status{workflow.StatusQueued},
			Key: func(m events.Message) string { return string(m.Payload) }}),
	)
	if err != nil {
		t.Fatal(err)
	}
	p, err := b.Publish(ctx, events.Message{ID: "1", Name: "e", Payload: []byte("7")})
	if err != nil {
		t.Fatal(err)
	}
	if p.Cancellations[0].Cancelled != 1 || p.Cancellations[1].Cancelled != 0 {
		t.Fatalf("cancellations = %+v; want remind only, and nothing queued", p.Cancellations)
	}
	waitStatus(t, c, r, workflow.StatusCancelled)
	if st, _ := c.Status(ctx, f); st != workflow.StatusRunning {
		t.Fatalf("fulfil is %s, want running", st)
	}

	p, err = b.Publish(ctx, events.Message{ID: "2", Name: "e"}) // empty key
	if !errors.Is(err, workflow.ErrInvalid) || !errors.Is(p.Cancellations[0].Err, workflow.ErrInvalid) || p.Cancellations[0].Cancelled != 0 {
		t.Fatalf("empty key: %+v, %v", p.Cancellations, err)
	}
	if st, _ := c.Status(ctx, f); st != workflow.StatusRunning {
		t.Fatalf("an empty key cancelled fulfil: %s", st)
	}
}

// TestDivert: the event cancels the order's runs, then starts the divert
// workflow with the message payload; the divert is not cancelled by its
// own message; a republish cancels nothing new and returns the same
// divert run.
func TestDivert(t *testing.T) {
	ctx := context.Background()
	var rec recorder
	c := started(t, holdWorkflow(t, "fulfil"), rec.workflow("refund"))
	run, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil", Labels: map[string]string{"order": "42"}})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := events.New(c)
	if err := b.SubscribeCancel(events.CancelSubscription{Name: "cancel-to-refund", Event: "order.cancelled", Label: "order", Divert: "refund"}); err != nil {
		t.Fatal(err)
	}
	msg := events.Message{ID: "c1", Name: "order.cancelled", Key: "42", Payload: []byte(`{"order":42}`), Labels: map[string]string{"order": "42"}}
	p, err := b.Publish(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	cn := p.Cancellations[0]
	if cn.Cancelled != 1 || cn.Diverted.IsZero() {
		t.Fatalf("cancellation = %+v", cn)
	}
	waitStatus(t, c, run, workflow.StatusCancelled)
	waitStatus(t, c, cn.Diverted, workflow.StatusSucceeded)
	if rec.ins[0] != `{"order":42}` || rec.runs[0].Labels[events.LabelSubscription] != "cancel-to-refund" || rec.runs[0].Labels["order"] != "42" {
		t.Fatalf("divert run got %s labels %v", rec.ins[0], rec.runs[0].Labels)
	}

	// A held divert so the republish meets its key unfinished.
	c2 := started(t, holdWorkflow(t, "refund"))
	b2, _ := events.New(c2)
	_ = b2.SubscribeCancel(events.CancelSubscription{Name: "cancel-to-refund", Event: "order.cancelled", Label: "order", Divert: "refund"})
	first, err := b2.Publish(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	again, err := b2.Publish(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	if again.Cancellations[0].Diverted != first.Cancellations[0].Diverted {
		t.Fatalf("republish diverted again: %v then %v", first.Cancellations[0].Diverted, again.Cancellations[0].Diverted)
	}
	if st, _ := c2.Status(ctx, first.Cancellations[0].Diverted); st.IsTerminal() {
		t.Fatalf("the republish cancelled the divert run: %s", st)
	}

	// After the divert finished (its key released) a republish still
	// does not divert again: the run is found by its labels.
	again, err = b.Publish(ctx, msg)
	if err != nil || again.Cancellations[0].Diverted != cn.Diverted || rec.count() != 1 {
		t.Fatalf("republish after the divert finished = %+v, %v; %d refund runs", again.Cancellations, err, rec.count())
	}
}

// TestDivertNotRunAfterFailedCancel: a cancel error leaves the divert
// unstarted, so a republish retries both.
func TestDivertNotRunAfterFailedCancel(t *testing.T) {
	var rec recorder
	c := started(t, rec.workflow("refund"))
	b, _ := events.New(failingCancel{c})
	if err := b.SubscribeCancel(events.CancelSubscription{Name: "x", Event: "e", Label: "order", Divert: "refund"}); err != nil {
		t.Fatal(err)
	}
	p, err := b.Publish(context.Background(), events.Message{Name: "e", Key: "1"})
	if err == nil || !p.Cancellations[0].Diverted.IsZero() {
		t.Fatalf("publish = %+v, %v; want the cancel error and no divert", p.Cancellations, err)
	}
}

// failingCancel's CancelAll always fails.
type failingCancel struct{ workflow.Client }

func (failingCancel) CancelAll(context.Context, workflow.RunQuery) (int, error) {
	return 0, errors.New("database down")
}

func (f failingCancel) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	return f.Client.(workflow.RunQuerier).Runs(ctx, q)
}

// cancelOnly keeps the client's BulkCanceller and hides its RunQuerier.
type cancelOnly struct{ workflow.Client }

func (c cancelOnly) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	return c.Client.(workflow.BulkCanceller).CancelAll(ctx, q)
}

// noCancelIface hides the client's BulkCanceller.
type noCancelIface struct{ workflow.Client }

// noCancelCaps declares no CapCancelAll.
type noCancelCaps struct{ workflow.Client }

func (n noCancelCaps) Capabilities() workflow.Caps {
	return n.Client.Capabilities() &^ workflow.CapCancelAll
}

func (n noCancelCaps) CancelAll(context.Context, workflow.RunQuery) (int, error) { return 0, nil }

// TestSubscribeCancelRefusals: a client without CancelAll is refused with
// UnsupportedError naming CapCancelAll; bad subscriptions and name
// collisions with ErrInvalid.
func TestSubscribeCancelRefusals(t *testing.T) {
	c := started(t)
	for name, client := range map[string]workflow.Client{"no interface": noCancelIface{c}, "no capability": noCancelCaps{c}} {
		b, _ := events.New(client)
		var ue *workflow.UnsupportedError
		err := b.SubscribeCancel(events.CancelSubscription{Name: "x", Event: "e", Label: "order"})
		if !errors.As(err, &ue) || ue.Missing != workflow.CapCancelAll {
			t.Errorf("%s: err = %v, want UnsupportedError for CapCancelAll", name, err)
		}
	}
	nq, _ := events.New(cancelOnly{c})
	var ue *workflow.UnsupportedError
	if err := nq.SubscribeCancel(events.CancelSubscription{Name: "x", Event: "e", Label: "order", Divert: "refund"}); !errors.As(err, &ue) || ue.Missing != workflow.CapRunQuery {
		t.Errorf("divert without RunQuerier: err = %v, want UnsupportedError for CapRunQuery", err)
	}
	b, _ := events.New(c)
	invalid := map[string]events.CancelSubscription{
		"no name":         {Event: "e", Label: "order"},
		"no event":        {Name: "x", Label: "order"},
		"no label":        {Name: "x", Event: "e"},
		"terminal status": {Name: "x", Event: "e", Label: "order", Statuses: []workflow.Status{workflow.StatusSucceeded}},
	}
	for name, s := range invalid {
		if err := b.SubscribeCancel(s); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if err := b.SubscribeCancel(events.CancelSubscription{Name: "x", Event: "e", Label: "order"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Subscribe(events.Subscription{Name: "x", Event: "e", Workflow: "w"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Subscribe reusing a cancel subscription's name: %v", err)
	}
	_ = b.Subscribe(events.Subscription{Name: "y", Event: "e", Workflow: "w"})
	if err := b.SubscribeCancel(events.CancelSubscription{Name: "y", Event: "e", Label: "order"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("SubscribeCancel reusing a subscription's name: %v", err)
	}
}
