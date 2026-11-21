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

// TestConformanceScheduleHeap runs the same suite with WithScheduleHeap, so
// every cron, schedule, misfire, pause, overlap and tick case also holds
// when one scheduler goroutine and one timer serve every schedule.
func TestConformanceScheduleHeap(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		return workflowtest.Instance{Client: memory.New(memory.WithClock(clk), memory.WithScheduleHeap()), Clock: clk}
	}, NewWithQueues: func(t *testing.T, qs []workflow.QueueConfig) workflowtest.Instance {
		clk := clock.NewFake(start)
		return workflowtest.Instance{Client: memory.New(memory.WithClock(clk), memory.WithQueues(qs...), memory.WithScheduleHeap()), Clock: clk}
	}})
}
