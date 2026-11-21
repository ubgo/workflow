package redis_test

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	wfredis "github.com/ubgo/workflow/redis"
	"github.com/ubgo/workflow/workflowtest"
)

// TestChildFlows proves the child-run helpers (joins, CancelChildren,
// MapRuns) against Redis, with the journal under the same prefix.
func TestChildFlows(t *testing.T) {
	rdb := redisClient(t)
	workflowtest.RunChildFlows(t, workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
		rc := newClient(t, rdb, testPrefix(t, rdb))
		c, err := workflow.Use(rc, workflow.Journal(rc.JournalStore()))
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

// TestSignalFlows proves the signal helpers against Redis, with the
// driver's PromiseStore carrying update replies and promises.
func TestSignalFlows(t *testing.T) {
	rdb := redisClient(t)
	var rc *wfredis.Client
	workflowtest.RunSignalFlows(t, workflowtest.SignalHarness{
		Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
			rc = newClient(t, rdb, testPrefix(t, rdb))
			c, err := workflow.Use(rc, workflow.Journal(rc.JournalStore()))
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
		Promises: func(*testing.T, workflow.Client) workflow.PromiseStore { return rc.PromiseStore() },
	})
}

// TestPromiseStore is the PromiseStore contract against Redis.
func TestPromiseStore(t *testing.T) {
	rdb := redisClient(t)
	s, err := wfredis.NewPromiseStore(rdb, testPrefix(t, rdb), 0)
	if err != nil {
		t.Fatal(err)
	}
	workflowtest.RunPromiseStore(t, s)
}

// TestObjectFlows proves virtual objects against Redis, their state in
// the driver's JournalStore.
func TestObjectFlows(t *testing.T) {
	rdb := redisClient(t)
	rc := newClient(t, rdb, testPrefix(t, rdb))
	workflowtest.RunObjectFlows(t, workflowtest.ObjectHarness{
		Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
			for _, wf := range wfs {
				if err := rc.Register(wf); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := rc.Start(ctx); err != nil {
				t.Fatal(err)
			}
			return rc
		},
		Store: func(*testing.T) workflow.JournalStore { return rc.JournalStore() },
	})
}

// TestResetFlows proves ResetRun against Redis.
func TestResetFlows(t *testing.T) {
	rdb := redisClient(t)
	workflowtest.RunResetFlows(t, workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
		rc := newClient(t, rdb, testPrefix(t, rdb))
		for _, wf := range wfs {
			if err := rc.Register(wf); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := rc.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return rc
	}})
}

// resetHarness starts a Redis client with the given workflows registered,
// for the reset, redo and fork flows.
func resetHarness(t *testing.T) workflowtest.FlowHarness {
	rdb := redisClient(t)
	return workflowtest.FlowHarness{Start: func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
		rc := newClient(t, rdb, testPrefix(t, rdb))
		for _, wf := range wfs {
			if err := rc.Register(wf); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if err := rc.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return rc
	}}
}

// TestRedoFlows proves ResetRunWith with ResetOptions.Succeeded against
// Redis: the run is reset in place, gets LabelRedo, and its chain is not
// handed on a second time.
func TestRedoFlows(t *testing.T) { workflowtest.RunRedoFlows(t, resetHarness(t)) }

// TestForkFlows proves ForkRun against Redis.
func TestForkFlows(t *testing.T) { workflowtest.RunForkFlows(t, resetHarness(t)) }
