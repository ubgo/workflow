package hatchet

import (
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

// What the rest of the driver must know about the start gate (declare.go,
// hasStartGate): it is the driver's task, not a step, so it never counts
// as one (attempts, the failed step, the result's final steps, StartedAt),
// and while it holds a run back the run has not started.

// gateStatus refines the status st of a live run (not terminal) whose
// workflow has the start gate: while the gate has not completed, the run
// is StatusScheduled until the RunAt stamped as metaStartAt and
// StatusQueued after it (or without one); once the gate has completed,
// the run is StatusQueued until one of its steps has started. The engine
// reads such a run RUNNING as soon as the gate runs, and a run held back
// by a rate limit after its gate, which read QUEUED before the gate
// existed, would read RUNNING too. A run without the gate keeps st. now
// is wall-clock, compared with the RunAt the client stamped.
func gateStatus(d *rest.V1WorkflowRunDetails, st workflow.Status, now time.Time) workflow.Status {
	if st.IsTerminal() || (st != workflow.StatusRunning && st != workflow.StatusQueued) {
		return st
	}
	gateID, ok := gateTaskID(d)
	if !ok {
		return st
	}
	gateDone := false
	for _, t := range d.Tasks {
		if t.TaskExternalId == gateID {
			gateDone = t.Status == rest.V1TaskStatusCOMPLETED
			break
		}
	}
	if !gateDone {
		if at, ok := startAtOf(metadataStrings(d.Run.AdditionalMetadata)); ok && now.Before(at) {
			return workflow.StatusScheduled
		}
		return workflow.StatusQueued
	}
	for _, t := range d.Tasks {
		if t.TaskExternalId != gateID && taskStarted(t) {
			return st
		}
	}
	return workflow.StatusQueued
}

// gateTaskID is the engine's ID of run d's start gate task, from the run's
// shape; ok is false for a run without one (its workflow has no gate, or
// it was declared before the gate existed). The shape lists a task not yet
// created with a zero ID; the gate is created with the run.
func gateTaskID(d *rest.V1WorkflowRunDetails) (openapi_types.UUID, bool) {
	for _, sh := range d.Shape {
		if sh.TaskName == startGateTask {
			return sh.TaskExternalId, true
		}
	}
	return openapi_types.UUID{}, false
}

// taskStarted reports whether a step task has begun: it is running or
// ended, it has a start time, or it is queued for a retry.
func taskStarted(t rest.V1TaskSummary) bool {
	switch t.Status {
	case rest.V1TaskStatusRUNNING, rest.V1TaskStatusCOMPLETED, rest.V1TaskStatusFAILED, rest.V1TaskStatusCANCELLED:
		return true
	}
	return t.StartedAt != nil || (t.RetryCount != nil && *t.RetryCount > 0)
}

// stepsStartedAt is RunInfo.StartedAt of a run with the start gate: the
// earliest start among its step tasks, zero while none has started. The
// engine's own start time is the gate's, which for a delayed start is the
// enqueue, not RunAt.
func stepsStartedAt(d *rest.V1WorkflowRunDetails, gateID openapi_types.UUID) time.Time {
	var first time.Time
	for _, t := range d.Tasks {
		if t.TaskExternalId == gateID || t.StartedAt == nil {
			continue
		}
		if first.IsZero() || t.StartedAt.Before(first) {
			first = *t.StartedAt
		}
	}
	return first
}
