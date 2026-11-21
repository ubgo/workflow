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

// TestResetFlows proves ResetRun on the memory driver, through Use.
func TestResetFlows(t *testing.T) {
	workflowtest.RunResetFlows(t, workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
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

// TestSlotFlows proves Step.Slots on the memory driver.
func TestSlotFlows(t *testing.T) {
	workflowtest.RunSlotFlows(t, workflowtest.SlotHarness{Start: func(t *testing.T, workers int, wfs ...workflow.Workflow) (workflow.Client, error) {
		c := memory.New(memory.WithWorkers(workers))
		for _, wf := range wfs {
			if err := c.Register(wf); err != nil {
				return nil, err
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		return c, c.Start(ctx)
	}})
}

// startUsed starts a memory client behind Use with the Journal plugin.
func startUsed(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
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

// TestRedoFlows proves ResetRunWith on the memory driver, through Use.
func TestRedoFlows(t *testing.T) {
	workflowtest.RunRedoFlows(t, workflowtest.FlowHarness{Start: startUsed})
}

// TestForkFlows proves ForkRun on the memory driver, through Use.
func TestForkFlows(t *testing.T) {
	workflowtest.RunForkFlows(t, workflowtest.FlowHarness{Start: startUsed})
}
