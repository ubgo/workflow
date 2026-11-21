package memory_test

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestMachineFlows proves DefineMachine on the memory driver.
func TestMachineFlows(t *testing.T) {
	workflowtest.RunMachineFlows(t, workflowtest.MachineHarness{
		Client: func(*testing.T) workflow.Client { return memory.New() },
		Start: func(t *testing.T, c workflow.Client, wfs ...workflow.Workflow) {
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
		},
		Store: func(*testing.T, workflow.Client) workflow.JournalStore { return workflow.NewMemoryJournal() },
	})
}
