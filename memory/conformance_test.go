package memory_test

import (
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestConformance runs the shared driver suite against the memory driver on
// a fake clock, so every delay, sleep and cron case runs instantly.
func TestConformance(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		return workflowtest.Instance{Client: memory.New(memory.WithClock(clk)), Clock: clk}
	}, NewWithQueues: func(t *testing.T, qs []workflow.QueueConfig) workflowtest.Instance {
		// One process is the whole cluster: no Peer, Restart or Crash.
		clk := clock.NewFake(start)
		return workflowtest.Instance{Client: memory.New(memory.WithClock(clk), memory.WithQueues(qs...)), Clock: clk}
	}})
}
