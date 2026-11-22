package hatchet_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/hatchet"
)

// TestCancelSubscriptionStatuses (no engine): Hatchet has no
// CapRunStates, so a cancel subscription on it must never ask CancelAll
// for StatusRetrying. The default subscription is accepted and its publish
// gets past the status check (no engine here, so it fails later, on the
// connection, never on "missing RunStates"); one naming StatusRetrying is
// refused at SubscribeCancel, not at every publish.
func TestCancelSubscriptionStatuses(t *testing.T) {
	c := hatchet.New(hatchet.Config{})
	b, err := events.New(c)
	if err != nil {
		t.Fatal(err)
	}
	var ue *workflow.UnsupportedError
	err = b.SubscribeCancel(events.CancelSubscription{Name: "retrying", Event: "order.cancelled", Label: "order", Statuses: []workflow.Status{workflow.StatusRetrying}})
	if !errors.As(err, &ue) || ue.Missing != workflow.CapRunStates {
		t.Fatalf("SubscribeCancel naming StatusRetrying = %v, want UnsupportedError for CapRunStates", err)
	}
	if err := b.SubscribeCancel(events.CancelSubscription{Name: "stop", Event: "order.cancelled", Label: "order"}); err != nil {
		t.Fatalf("SubscribeCancel with the default statuses: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5e9)
	defer cancel()
	p, _ := b.Publish(ctx, events.Message{ID: "e1", Name: "order.cancelled", Key: "42"})
	for _, cn := range p.Cancellations {
		if errors.As(cn.Err, &ue) && ue.Missing == workflow.CapRunStates || cn.Err != nil && strings.Contains(cn.Err.Error(), "RunStates") {
			t.Fatalf("cancellation %s failed on the status filter: %v", cn.Subscription, cn.Err)
		}
	}
}

// TestEventCancelLive: on the engine, a cancel subscription with the
// default statuses cancels a running run carrying the matching label, and
// leaves a run of another key alone (Hatchet has no StatusRetrying, so
// the default must not ask for it).
func TestEventCancelLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfevcancel_" + uuid.NewString()[:8]
	hold := workflow.Workflow{Name: prefix + "_hold", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, hold)
	ctx := context.Background()
	enq := func(order string) workflow.RunRef {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: hold.Name, Labels: map[string]string{"order": order}})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	r42, r43 := enq("42"), enq("43")
	waitStatus(t, c, r42, workflow.StatusRunning)
	waitStatus(t, c, r43, workflow.StatusRunning)
	b, err := events.New(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SubscribeCancel(events.CancelSubscription{Name: "stop", Event: "order.cancelled", Label: "order", Workflow: hold.Name}); err != nil {
		t.Fatal(err)
	}
	p, err := b.Publish(ctx, events.Message{ID: "m-" + prefix, Name: "order.cancelled", Key: "42"})
	if err != nil || len(p.Cancellations) != 1 || p.Cancellations[0].Err != nil || p.Cancellations[0].Cancelled != 1 {
		t.Fatalf("publish = %+v, %v; want one run cancelled", p.Cancellations, err)
	}
	waitStatus(t, c, r42, workflow.StatusCancelled)
	if st, err := c.Status(ctx, r43); err != nil || st.IsTerminal() {
		t.Fatalf("order 43's run is %s, %v; another key must be untouched", st, err)
	}
	if err := c.Cancel(ctx, r43); err != nil {
		t.Fatal(err)
	}
}
