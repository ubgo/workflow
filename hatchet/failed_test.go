package hatchet

import (
	"errors"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

// failedRun builds run details as the engine returns them: one task
// "only", registered with retries (nil: no budget in the config), read
// FAILED after retryCount retries with message msg, finished at finished.
func failedRun(retries any, retryCount int, msg string, finished time.Time) *rest.V1WorkflowRunDetails {
	id := openapi_types.UUID{1}
	cfg := map[string]any{"Tasks": []any{map[string]any{"ReadableId": "only", "Retries": retries}}}
	if retries == nil {
		cfg = map[string]any{}
	}
	d := &rest.V1WorkflowRunDetails{
		Run:            rest.V1WorkflowRun{Status: rest.V1TaskStatusFAILED},
		Tasks:          []rest.V1TaskSummary{{TaskExternalId: id, Status: rest.V1TaskStatusFAILED, RetryCount: &retryCount, ErrorMessage: &msg, FinishedAt: &finished}},
		WorkflowConfig: &cfg,
	}
	d.Shape = append(d.Shape, struct {
		ChildrenStepIds []openapi_types.UUID `json:"childrenStepIds"`
		StepId          openapi_types.UUID   `json:"stepId"`
		TaskExternalId  openapi_types.UUID   `json:"taskExternalId"`
		TaskName        string               `json:"taskName"`
	}{TaskExternalId: id, TaskName: "only"})
	return d
}

// TestEndStatus pins how a FAILED engine read is judged, on fake engine
// responses: a marked failure, used-up retries and a failure older than
// the settle window are final; a failure with retries left reads Queued
// (Running when a task runs), as between attempts; a run with no budget
// in its config (a run the driver did not stamp) falls back to the settle
// window; other engine statuses map as before.
func TestEndStatus(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	const settle = 5 * time.Second
	recent, old := now.Add(-50*time.Millisecond), now.Add(-time.Minute)
	marked := markedError{errors.New("perm")}.Error()
	with := func(d *rest.V1WorkflowRunDetails, f func(*rest.V1WorkflowRunDetails)) *rest.V1WorkflowRunDetails {
		f(d)
		return d
	}
	for name, tc := range map[string]struct {
		d    *rest.V1WorkflowRunDetails
		want workflow.Status
	}{
		"retries left":           {failedRun(float64(2), 1, `"boom"`, recent), workflow.StatusQueued},
		"retries left, old read": {failedRun(float64(2), 1, `"boom"`, old), workflow.StatusFailed},
		"budget reached":         {failedRun(float64(2), 2, `"boom"`, recent), workflow.StatusFailed},
		"no retries at all":      {failedRun(float64(0), 0, `"boom"`, recent), workflow.StatusFailed},
		"marker":                 {failedRun(float64(2), 0, `"`+marked+`"`, recent), workflow.StatusFailed},
		"no stamp, recent":       {failedRun(nil, 5, `"perm"`, recent), workflow.StatusQueued},
		"no stamp, old":          {failedRun(nil, 5, `"perm"`, old), workflow.StatusFailed},
		"task re-queued": {with(failedRun(float64(2), 1, `"boom"`, old), func(d *rest.V1WorkflowRunDetails) {
			d.Tasks[0].Status = rest.V1TaskStatusQUEUED
		}), workflow.StatusQueued},
		"a sibling task runs": {with(failedRun(float64(2), 1, `"boom"`, recent), func(d *rest.V1WorkflowRunDetails) {
			d.Tasks = append(d.Tasks, rest.V1TaskSummary{TaskExternalId: openapi_types.UUID{2}, Status: rest.V1TaskStatusRUNNING})
		}), workflow.StatusRunning},
		"completed": {with(failedRun(float64(2), 0, `"boom"`, recent), func(d *rest.V1WorkflowRunDetails) {
			d.Run.Status = rest.V1TaskStatusCOMPLETED
		}), workflow.StatusSucceeded},
	} {
		got, err := endStatus(tc.d, settle, now)
		if err != nil || got != tc.want {
			t.Errorf("%s: endStatus = %q, %v; want %q", name, got, err, tc.want)
		}
	}
}

// TestFinalErr pins that a driver-final error is non-retryable on the
// engine, that the marked error keeps the step's error for errors.Is (the
// SDK's wrapper has no Unwrap), and that the marker is stripped from what
// callers read back.
func TestFinalErr(t *testing.T) {
	base := errors.New("perm")
	err := finalErr(base)
	if !hatchetsdk.IsNonRetryableError(err) || !errors.Is(markedError{base}, base) {
		t.Fatalf("finalErr: retryable or lost its cause: %v", err)
	}
	if got := stripFinal(`"` + err.Error() + `"`); got != `"perm"` {
		t.Fatalf("stripFinal = %q", got)
	}
	if got := runError(workflow.StatusFailed, ptr(finalMarker+"perm")); got.Error() != "perm" {
		t.Fatalf("runError = %q", got)
	}
}

func ptr(s string) *string { return &s }
