package enginetest

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/workflowtest"
)

// TestEscalationFlows proves run-addressed signals on every engine:
// OwnRun waits, SignalParent, OnSignals handlers between steps over the
// signals and consumed tables, and the RunSignalReader contract.
func TestEscalationFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			workflowtest.RunEscalationFlows(t, workflowtest.EscalationHarness{
				Start: func(t *testing.T, plugins []workflow.Plugin, wfs ...workflow.Workflow) workflow.Client {
					db, prefix := e.open(t)
					sc := newClient(t, e, db, prefix)
					c, err := workflow.Use(sc, append([]workflow.Plugin{workflow.Journal(sc.JournalStore())}, plugins...)...)
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
		})
	}
}
