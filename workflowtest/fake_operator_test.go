package workflowtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

func TestFakeTriggerSchedule(t *testing.T) {
	ctx := context.Background()
	f := startedFake(t, nil, oneStep("report", noopHandler))
	id, err := f.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "report", Cron: "0 3 * * *", Location: time.UTC,
		Input: []byte(`"x"`), Overlap: workflow.OverlapSkip})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := f.TriggerSchedule(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	f.AssertTriggered(t, id)
	f.AssertEnqueued(t, "report")
	if j := f.Enqueued("report"); j[0].Labels[workflow.LabelTrigger] != workflow.LabelTriggerManual || string(j[0].Payload) != `"x"` {
		t.Errorf("triggered job = %+v", j[0])
	}
	if _, err := f.TriggerSchedule(ctx, id); !errors.Is(err, workflow.ErrOverlap) {
		t.Errorf("trigger while the last run is unfinished: %v, want ErrOverlap", err)
	}
	if err := f.Finish(ref, workflow.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, err := f.TriggerSchedule(ctx, id); err != nil {
		t.Errorf("trigger after the last run finished: %v", err)
	}
	if _, err := f.TriggerSchedule(ctx, "nope"); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("unknown schedule: %v, want ErrNotFound", err)
	}
}

func TestFakeForce(t *testing.T) {
	ctx := context.Background()
	f := startedFake(t, nil, oneStep("w", noopHandler))
	a, _ := f.Enqueue(ctx, workflow.Job{Workflow: "w"})
	b, _ := f.Enqueue(ctx, workflow.Job{Workflow: "w"})
	if err := f.ForceFail(ctx, a, "bad"); err != nil {
		t.Fatal(err)
	}
	if st, _ := f.Status(ctx, a); st != workflow.StatusFailed {
		t.Errorf("after ForceFail: %q", st)
	}
	if err := f.ForceComplete(ctx, a); err != nil {
		t.Fatalf("ForceComplete of a failed run: %v", err)
	}
	if err := f.ForceComplete(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := f.ForceFail(ctx, b, "late"); !errors.Is(err, workflow.ErrWrongState) {
		t.Errorf("ForceFail of a succeeded run: %v, want ErrWrongState", err)
	}
	f.AssertForced(t, a, false)
	f.AssertForced(t, a, true)
	f.AssertForced(t, b, true)
	if len(f.Forced()) != 3 {
		t.Errorf("Forced = %+v, want 3 (the refused call is not recorded)", f.Forced())
	}
}
