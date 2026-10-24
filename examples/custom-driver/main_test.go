package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/workflowtest"
)

// TestRun runs the program and checks the lines the guide shows.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `render got "42 rows"
tiny-1: succeeded
delayed start refused: true
workflow: Job.RunAt unsupported by this driver (missing DelayedStart): job for workflow report
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}

// snippet:conformance
// TestConformance holds Tiny to the same suite as every shipped driver.
// Subtests that need a capability Tiny lacks are skipped, naming it; the
// rest must pass.
func TestConformance(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		return workflowtest.Instance{Client: NewTiny(clk), Clock: clk}
	}})
}

// snippet:end
