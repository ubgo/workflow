package hatchet

import (
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

// gateRun builds run details with a start gate in gateStatus and a first
// step "a" in stepStatus (empty: not created yet), engine status runSt,
// and metaStartAt when startAt is set.
func gateRun(runSt, gateSt, stepSt rest.V1TaskStatus, startAt time.Time) *rest.V1WorkflowRunDetails {
	gateID, stepID := uuid.New(), uuid.New()
	md := map[string]any{metaWorkflow: "w"}
	if !startAt.IsZero() {
		md[metaStartAt] = strconv.FormatInt(startAt.UnixMilli(), 10)
	}
	d := &rest.V1WorkflowRunDetails{Run: rest.V1WorkflowRun{Status: runSt, AdditionalMetadata: &md}}
	d.Shape = append(d.Shape, rest.WorkflowRunShapeItemForWorkflowRunDetails{TaskName: startGateTask, TaskExternalId: gateID})
	d.Tasks = append(d.Tasks, rest.V1TaskSummary{TaskExternalId: gateID, Status: gateSt})
	if stepSt == "" {
		d.Shape = append(d.Shape, rest.WorkflowRunShapeItemForWorkflowRunDetails{TaskName: "a"})
		return d
	}
	d.Shape = append(d.Shape, rest.WorkflowRunShapeItemForWorkflowRunDetails{TaskName: "a", TaskExternalId: stepID})
	step := rest.V1TaskSummary{TaskExternalId: stepID, Status: stepSt}
	if stepSt == rest.V1TaskStatusRUNNING {
		started := time.Unix(5, 0)
		step.StartedAt = &started
	}
	d.Tasks = append(d.Tasks, step)
	return d
}

// TestGateStatus pins how a run with the start gate reads: scheduled
// while the gate holds it before its RunAt, queued while the gate runs
// without one or after it, queued after the gate until a step starts, and
// as the engine says once a step has started or the run ended. A run
// without the gate keeps the engine's status.
func TestGateStatus(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	later, earlier := now.Add(time.Hour), now.Add(-time.Second)
	const (
		R = rest.V1TaskStatusRUNNING
		Q = rest.V1TaskStatusQUEUED
		C = rest.V1TaskStatusCOMPLETED
		X = rest.V1TaskStatusCANCELLED
	)
	cases := []struct {
		name            string
		run, gate, step rest.V1TaskStatus
		startAt         time.Time
		want            workflow.Status
	}{
		{"sleeping gate before RunAt", R, R, "", later, workflow.StatusScheduled},
		{"gate queued before RunAt", Q, Q, "", later, workflow.StatusScheduled},
		{"gate running past RunAt", R, R, "", earlier, workflow.StatusQueued},
		{"gate running without RunAt", R, R, "", time.Time{}, workflow.StatusQueued},
		{"gate done, step queued", R, C, Q, later, workflow.StatusQueued},
		{"gate done, step running", R, C, R, time.Time{}, workflow.StatusRunning},
		{"cancelled while sleeping", X, X, "", later, workflow.StatusCancelled},
		{"succeeded", C, C, C, time.Time{}, workflow.StatusSucceeded},
	}
	for _, tc := range cases {
		got, err := endStatus(gateRun(tc.run, tc.gate, tc.step, tc.startAt), time.Second, now)
		if err != nil || got != tc.want {
			t.Errorf("%s: status = %s, %v; want %s", tc.name, got, err, tc.want)
		}
	}
	plain := &rest.V1WorkflowRunDetails{Run: rest.V1WorkflowRun{Status: R}, Shape: []rest.WorkflowRunShapeItemForWorkflowRunDetails{{TaskName: "a", TaskExternalId: openapi_types.UUID(uuid.New())}}}
	if got, _ := endStatus(plain, time.Second, now); got != workflow.StatusRunning {
		t.Errorf("a run without the gate: status = %s, want running", got)
	}
}

// TestStepsStartedAt pins that a gated run's StartedAt is its first step's
// start, not the gate's (the enqueue, for a delayed start).
func TestStepsStartedAt(t *testing.T) {
	d := gateRun(rest.V1TaskStatusRUNNING, rest.V1TaskStatusCOMPLETED, rest.V1TaskStatusRUNNING, time.Time{})
	gateStart := time.Unix(1, 0)
	d.Tasks[0].StartedAt = &gateStart
	id, ok := gateTaskID(d)
	if !ok {
		t.Fatal("gate not found in the shape")
	}
	if got := stepsStartedAt(d, id); !got.Equal(time.Unix(5, 0)) {
		t.Fatalf("StartedAt = %v, want the step's start", got)
	}
	if got := stepsStartedAt(gateRun(rest.V1TaskStatusRUNNING, rest.V1TaskStatusRUNNING, "", time.Time{}), id); !got.IsZero() {
		t.Fatalf("StartedAt before any step = %v, want zero", got)
	}
}
