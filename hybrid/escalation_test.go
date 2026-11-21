package hybrid_test

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hybrid"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestEscalationFlows runs the run-addressed signal suite through the
// wrapper over two memory clients: SignalRun and SignalParent strip the
// wrapper's ref prefix and reach the run's own address, and OnSignals
// reads a run's signals on the backend that holds it.
func TestEscalationFlows(t *testing.T) {
	workflowtest.RunEscalationFlows(t, workflowtest.EscalationHarness{
		Start: func(t *testing.T, plugins []workflow.Plugin, wfs ...workflow.Workflow) workflow.Client {
			base := hybrid.New(memory.New(), memory.New())
			c, err := workflow.Use(base, append([]workflow.Plugin{workflow.Journal(workflow.NewMemoryJournal())}, plugins...)...)
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
