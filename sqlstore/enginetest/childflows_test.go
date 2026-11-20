package enginetest

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
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

// TestSignalFlows proves the signal helpers on every engine, with
// sqlstore's PromiseStore carrying update replies and promises.
func TestSignalFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			var sc *sqlstore.Client
			workflowtest.RunSignalFlows(t, workflowtest.SignalHarness{
				Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
					db, prefix := e.open(t)
					sc = newClient(t, e, db, prefix)
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
				Promises: func(*testing.T, workflow.Client) workflow.PromiseStore { return sc.PromiseStore() },
			})
		})
	}
}

// TestPromiseStore is the PromiseStore contract on every engine, through
// a standalone store (NewPromiseStore plus its Migrate).
func TestPromiseStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			s, err := sqlstore.NewPromiseStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
			workflowtest.RunPromiseStore(t, s)
		})
	}
}

// TestObjectFlows proves virtual objects on every engine, their state in
// the journal table.
func TestObjectFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			sc := newClient(t, e, db, prefix)
			workflowtest.RunObjectFlows(t, workflowtest.ObjectHarness{
				Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
					startWith(t, sc, wfs...)
					return sc
				},
				Store: func(*testing.T) workflow.JournalStore { return sc.JournalStore() },
			})
		})
	}
}

// TestResetFlows proves ResetRun on every engine.
func TestResetFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			workflowtest.RunResetFlows(t, workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
				db, prefix := e.open(t)
				sc := newClient(t, e, db, prefix)
				startWith(t, sc, wfs...)
				return sc
			}})
		})
	}
}

// TestSlotFlows proves Step.Slots on every engine.
func TestSlotFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			workflowtest.RunSlotFlows(t, workflowtest.SlotHarness{Start: func(t *testing.T, workers int, wfs ...workflow.Workflow) (workflow.Client, error) {
				db, prefix := e.open(t)
				sc := newClient(t, e, db, prefix, sqlstore.WithWorkers(workers))
				for _, wf := range wfs {
					if err := sc.Register(wf); err != nil {
						return nil, err
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				return sc, sc.Start(ctx)
			}})
		})
	}
}
