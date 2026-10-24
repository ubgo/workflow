package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/workflowtest"
)

// snippet:fake
// TestRunNowButton checks an operator action without workers: the Fake
// records the trigger, enqueues the schedule's job, refuses a second one
// while the first run is unfinished, and records a force.
func TestRunNowButton(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := fake.Register(workflow.Workflow{Name: "sync-crm", Steps: []workflow.Step{{Name: "sync", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := fake.Schedule(ctx, workflow.Schedule{Name: "crm-hourly", Workflow: "sync-crm", Cron: "0 * * * *", Location: time.UTC, Overlap: workflow.OverlapSkip})
	if err != nil {
		t.Fatal(err)
	}

	ref, err := fake.TriggerSchedule(ctx, id) // what the Run now button calls
	if err != nil {
		t.Fatal(err)
	}
	fake.AssertTriggered(t, id)
	fake.AssertEnqueued(t, "sync-crm", workflowtest.HasLabel(workflow.LabelTrigger, workflow.LabelTriggerManual))

	if _, err := fake.TriggerSchedule(ctx, id); !errors.Is(err, workflow.ErrOverlap) {
		t.Fatalf("second trigger: %v, want ErrOverlap", err)
	}
	if err := fake.ForceComplete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	fake.AssertForced(t, ref, true)
	if _, err := fake.TriggerSchedule(ctx, id); err != nil { // the previous run ended
		t.Fatal(err)
	}
}

// snippet:end
