package failover_test

import (
	"testing"

	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestConformance runs the shared driver suite over a failover client made
// of two memory drivers on one fake clock, with both backends up: every
// run lands on the primary.
func TestConformance(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		p := memory.New(memory.WithClock(clk))
		s := memory.New(memory.WithClock(clk))
		return workflowtest.Instance{Client: failover.New(p, s, failover.AllowWithoutKey()), Clock: clk}
	}})
}

// TestConformancePrimaryDown runs the suite with the primary refusing
// every enqueue as unavailable, so every run lands on the secondary and
// every Status, Cancel, Signal and run-finished event the suite checks has
// to reach it through the prefixed refs.
func TestConformancePrimaryDown(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		p := &switchable{Client: memory.New(memory.WithClock(clk))}
		p.down.Store(true)
		s := memory.New(memory.WithClock(clk))
		return workflowtest.Instance{Client: failover.New(p, s, failover.AllowWithoutKey()), Clock: clk}
	}})
}
