package memory_test

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestEscalationFlows proves run-addressed signals on the memory driver:
// OwnRun waits, SignalParent, OnSignals handlers between steps and the
// RunSignalReader contract.
func TestEscalationFlows(t *testing.T) {
	workflowtest.RunEscalationFlows(t, workflowtest.EscalationHarness{
		Start: func(t *testing.T, plugins []workflow.Plugin, wfs ...workflow.Workflow) workflow.Client {
			c, err := workflow.Use(memory.New(), append([]workflow.Plugin{workflow.Journal(workflow.NewMemoryJournal())}, plugins...)...)
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
		},
	})
}
