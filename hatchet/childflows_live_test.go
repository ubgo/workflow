package hatchet_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/workflowtest"
)

// TestChildFlowsLive runs the child-run helper contract on the engine:
// the cases its capabilities allow run (CancelChildren on running
// children; Hatchet has no StatusRetrying, so CancelChildren must not ask
// for it), the rest skip with the reason: no CapSnooze, and a delayed
// spawned child is refused (Spawn sets an idempotency key, which Hatchet
// refuses together with Job.RunAt).
func TestChildFlowsLive(t *testing.T) {
	skipWithoutEngine(t)
	workflowtest.RunChildFlows(t, workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
		c, err := workflow.Use(hatchet.New(hatchet.Config{WorkerName: "wfchild_" + uuid.NewString()[:8]}), workflow.Journal(workflow.NewMemoryJournal()))
		if err != nil {
			t.Fatal(err)
		}
		for _, wf := range wfs {
			if err := c.Register(wf); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return c
	}})
}
