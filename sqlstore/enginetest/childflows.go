package enginetest

// The tests in this file are plain functions, not Test functions: the
// package shards/childflows wraps each in a Test function of the same name, so
// they run as a test binary of their own beside this package (see
// conformance.go for why processes, not parallel subtests).

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// ChildFlows proves the child-run helpers (joins, CancelChildren,
// MapRuns) on every engine, with the journal in the same database.
func ChildFlows(t *testing.T) {
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

// SignalFlows proves the signal helpers on every engine, with
// sqlstore's PromiseStore carrying update replies and promises.
func SignalFlows(t *testing.T) {
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

// PromiseStore is the PromiseStore contract on every engine, through
// a standalone store (NewPromiseStore plus its Migrate).
func PromiseStore(t *testing.T) {
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

// ObjectFlows proves virtual objects on every engine, their state in
// the journal table.
func ObjectFlows(t *testing.T) {
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

// ResetFlows proves ResetRun on every engine.
func ResetFlows(t *testing.T) {
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

// SlotFlows proves Step.Slots on every engine.
func SlotFlows(t *testing.T) {
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

// RedoFlows proves ResetRunWith (a succeeded run) on every engine.
func RedoFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			workflowtest.RunRedoFlows(t, workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
				db, prefix := e.open(t)
				sc := newClient(t, e, db, prefix)
				startWith(t, sc, wfs...)
				return sc
			}})
		})
	}
}

// ForkFlows proves ForkRun on every engine.
func ForkFlows(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			workflowtest.RunForkFlows(t, workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
				db, prefix := e.open(t)
				sc := newClient(t, e, db, prefix)
				startWith(t, sc, wfs...)
				return sc
			}})
		})
	}
}
