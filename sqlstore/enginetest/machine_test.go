package enginetest

import (
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// TestMachineFlows proves state machines on every engine, each instance's
// record in the journal table.
func TestMachineFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			workflowtest.RunMachineFlows(t, workflowtest.MachineHarness{
				Client: func(t *testing.T) workflow.Client {
					db, prefix := e.open(t)
					return newClient(t, e, db, prefix)
				},
				Start: func(t *testing.T, c workflow.Client, wfs ...workflow.Workflow) {
					startWith(t, c.(*sqlstore.Client), wfs...)
				},
				Store: func(_ *testing.T, c workflow.Client) workflow.JournalStore {
					return c.(*sqlstore.Client).JournalStore()
				},
			})
		})
	}
}
