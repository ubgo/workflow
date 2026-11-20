package enginetest

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/workflowtest"
)

// TestChildFlows proves the child-run helpers (joins, CancelChildren,
// MapRuns) on every engine, with the journal in the same database.
func TestChildFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			workflowtest.RunChildFlows(t, workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
				db, prefix := e.open(t)
				sc := newClient(t, e, db, prefix)
				c, err := workflow.Use(sc, workflow.Journal(sc.JournalStore()))
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
		})
	}
}
