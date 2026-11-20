package enginetest

import (
	"context"
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

// TestReplayFlows proves replayed orchestration on every engine, its
// history in the journal table and its waits as snoozes.
func TestReplayFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			workflowtest.RunReplayFlows(t, workflowtest.ReplayHarness{
				Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
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
				},
				Promises: func(t *testing.T, _ workflow.Client) workflow.PromiseStore {
					db, prefix := e.open(t)
					s, err := sqlstore.NewPromiseStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
					if err != nil {
						t.Fatal(err)
					}
					if err := s.Migrate(context.Background()); err != nil {
						t.Fatal(err)
					}
					return s
				},
			})
		})
	}
}
