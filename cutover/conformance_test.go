package cutover_test

import (
	"testing"
	"time"

	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cutover"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestConformance runs the shared driver suite over a cutover client made
// of two memory drivers on one fake clock: every run lands on the new
// backend, and every Status, Cancel, Signal, run query and run-finished
// event the suite checks has to reach it through the prefixed refs.
func TestConformance(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		return workflowtest.Instance{Client: cutover.New(memory.New(memory.WithClock(clk)), memory.New(memory.WithClock(clk))), Clock: clk}
	}})
}
