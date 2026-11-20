package events_test

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/workflowtest"
)

// TestSubscriptionSchedulesFromEvent proves EV-06 (schedule from event):
// a subscription's Job sets RunAt, so the event creates a future run.
func TestSubscriptionSchedulesFromEvent(t *testing.T) {
	fake := workflowtest.NewFake()
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	if err := fake.Register(workflow.Workflow{Name: "trial-reminder", Steps: []workflow.Step{{Name: "mail", Run: noop}}}); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	bus, err := events.New(fake)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 12, 1, 9, 0, 0, 0, time.UTC)
	if err := bus.Subscribe(events.Subscription{Name: "trial-ends", Event: "user.signed_up", Job: func(m events.Message) (workflow.Job, error) {
		return workflow.Job{Workflow: "trial-reminder", Payload: m.Payload, RunAt: at}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Publish(context.Background(), events.Message{ID: "e1", Name: "user.signed_up", Payload: []byte(`"u1"`)}); err != nil {
		t.Fatal(err)
	}
	jobs := fake.Jobs()
	if len(jobs) != 1 || !jobs[0].RunAt.Equal(at) || jobs[0].Workflow != "trial-reminder" {
		t.Fatalf("jobs %+v", jobs)
	}
}
