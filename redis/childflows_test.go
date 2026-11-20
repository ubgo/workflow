package redis_test

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
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
