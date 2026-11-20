package redis_test

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	wfredis "github.com/ubgo/workflow/redis"
	"github.com/ubgo/workflow/workflowtest"
)

// TestMachineFlows proves state machines against Redis, each instance's
// record in the driver's JournalStore.
func TestMachineFlows(t *testing.T) {
	rdb := redisClient(t)
	workflowtest.RunMachineFlows(t, workflowtest.MachineHarness{
		Client: func(t *testing.T) workflow.Client { return newClient(t, rdb, testPrefix(t, rdb)) },
		Start: func(t *testing.T, c workflow.Client, wfs ...workflow.Workflow) {
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
		},
		Store: func(_ *testing.T, c workflow.Client) workflow.JournalStore { return c.(*wfredis.Client).JournalStore() },
	})
}
