package hatchet

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
)

// TestWorkflowVersionIsRefused (LO-11): the engine hands a run to any
// worker that registered the workflow's name, so it cannot keep a run
// started by new code away from an old worker. The driver declares no
// CapWorkflowVersion and Register refuses Version and MinVersion, before
// any engine call.
func TestWorkflowVersionIsRefused(t *testing.T) {
	if New(Config{}).Capabilities().Has(workflow.CapWorkflowVersion) {
		t.Fatal("hatchet declares CapWorkflowVersion, which its engine cannot honour")
	}
	step := []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}
	for name, wf := range map[string]workflow.Workflow{
		"Version":            {Name: "w", Steps: step, Version: 2},
		"Version+MinVersion": {Name: "w", Steps: step, Version: 2, MinVersion: 2},
	} {
		asRefusal(t, name, New(Config{}).Register(wf), "Workflow", workflow.CapWorkflowVersion)
	}
	if err := New(Config{}).Register(workflow.Workflow{Name: "w", Steps: step}); err != nil {
		t.Fatalf("an unversioned workflow: %v", err)
	}
}
