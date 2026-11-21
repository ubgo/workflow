package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// start is where every fake clock of the suite begins.
var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestRun runs the program and checks every line it prints. The count of
// the last share follows the suite, so a group added to it later does not
// break this test.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	suite := len(workflowtest.GroupNames())
	named := len(shares[0]) + len(shares[1])
	want := "share 1: lifecycle run retry timeout onfailure\n" +
		"share 2: cron misfire schedule overlap trigger\n" +
		fmt.Sprintf("share 3: every other group (%d of %d)\n", suite-named, suite)
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// TestPlanRefusesATypo checks that the plan check catches a group the
// suite does not have.
func TestPlanRefusesATypo(t *testing.T) {
	if err := checkShares([]string{"lifecycle"}); err == nil {
		t.Error("checkShares accepted groups the suite does not list")
	}
}

// snippet:share
// TestConformance runs share 1 of the plan on the memory driver. A driver
// that splits the suite has one small test package per share, each
// calling workflowtest.Run with shareGroups(i), so go test runs the
// shares as parallel processes. This example runs only the first share
// to keep the examples' test run short.
func TestConformance(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{
		Groups: shareGroups(0),
		New: func(t *testing.T) workflowtest.Instance {
			clk := clock.NewFake(start)
			return workflowtest.Instance{Client: memory.New(memory.WithClock(clk)), Clock: clk}
		},
	})
}

// snippet:end

// snippet:start
// startUsed is the Start of FlowHarness, ReplayHarness and SignalHarness:
// a fresh client of the driver behind workflow.Use with the Journal
// plugin (over the driver's durable JournalStore where it has one), the
// workflows registered, started, and stopped when the test ends.
func startUsed(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
	c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		t.Fatal(err)
	}
	registerAndStart(t, c, wfs...)
	return c
}

// registerAndStart registers wfs on c and starts it until the test ends.
func registerAndStart(t *testing.T, c workflow.Client, wfs ...workflow.Workflow) {
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
}

// promises is the PromiseStore the run and the caller share; a durable
// driver returns its own store here.
func promises(*testing.T, workflow.Client) workflow.PromiseStore {
	return workflow.NewMemoryPromises()
}

// snippet:end

// snippet:flows
// TestCoreHelperFlows proves the core's helpers on the driver: children
// and joins, signals and promises, virtual objects, reset, redo and fork,
// state machines and replay. Each case skips what the driver cannot do.
func TestCoreHelperFlows(t *testing.T) {
	flow := workflowtest.FlowHarness{Start: startUsed}
	t.Run("child", func(t *testing.T) { workflowtest.RunChildFlows(t, flow) })
	t.Run("reset", func(t *testing.T) { workflowtest.RunResetFlows(t, flow) })
	t.Run("redo", func(t *testing.T) { workflowtest.RunRedoFlows(t, flow) })
	t.Run("fork", func(t *testing.T) { workflowtest.RunForkFlows(t, flow) })
	t.Run("signal", func(t *testing.T) {
		workflowtest.RunSignalFlows(t, workflowtest.SignalHarness{Start: startUsed, Promises: promises})
	})
	t.Run("promisestore", func(t *testing.T) { workflowtest.RunPromiseStore(t, workflow.NewMemoryPromises()) })
	t.Run("replay", func(t *testing.T) {
		workflowtest.RunReplayFlows(t, workflowtest.ReplayHarness{Start: startUsed, Promises: promises})
	})
	t.Run("object", func(t *testing.T) {
		workflowtest.RunObjectFlows(t, workflowtest.ObjectHarness{
			// Objects keep their state in Store, so the client needs no
			// Journal plugin.
			Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
				c := memory.New()
				registerAndStart(t, c, wfs...)
				return c
			},
			Store: func(*testing.T) workflow.JournalStore { return workflow.NewMemoryJournal() },
		})
	})
	t.Run("machine", func(t *testing.T) {
		workflowtest.RunMachineFlows(t, workflowtest.MachineHarness{
			// Client comes before Start, so a machine with timers can be
			// defined against the client Start will start.
			Client: func(*testing.T) workflow.Client { return memory.New() },
			Start:  registerAndStart,
			Store:  func(*testing.T, workflow.Client) workflow.JournalStore { return workflow.NewMemoryJournal() },
		})
	})
	t.Run("slot", func(t *testing.T) {
		workflowtest.RunSlotFlows(t, workflowtest.SlotHarness{
			// workers is the pool size the case asks for; a Register
			// refusal is returned, not fatal, since one case expects it.
			Start: func(t *testing.T, workers int, wfs ...workflow.Workflow) (workflow.Client, error) {
				c := memory.New(memory.WithWorkers(workers))
				for _, wf := range wfs {
					if err := c.Register(wf); err != nil {
						return nil, err
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				return c, c.Start(ctx)
			},
		})
	})
}

// snippet:end
