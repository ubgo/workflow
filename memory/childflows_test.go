package memory_test

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestChildFlows proves the child-run helpers (joins, CancelChildren,
// MapRuns) on the memory driver.
func TestChildFlows(t *testing.T) {
	workflowtest.RunChildFlows(t, workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
		c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()))
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

// TestSignalFlows proves the signal helpers (broadcast, SignalWithStart,
// Update, promises, ContinueAsNew, CancelSteps) on the memory driver.
func TestSignalFlows(t *testing.T) {
	start := func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
		c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()))
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
	}
	workflowtest.RunSignalFlows(t, workflowtest.SignalHarness{Start: start,
		Promises: func(*testing.T, workflow.Client) workflow.PromiseStore { return workflow.NewMemoryPromises() }})
}

func TestMemoryPromises(t *testing.T) { workflowtest.RunPromiseStore(t, workflow.NewMemoryPromises()) }

// TestObjectFlows proves virtual objects on the memory driver.
func TestObjectFlows(t *testing.T) {
	workflowtest.RunObjectFlows(t, workflowtest.ObjectHarness{
		Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
			c := memory.New()
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
		Store: func(*testing.T) workflow.JournalStore { return workflow.NewMemoryJournal() },
	})
}
