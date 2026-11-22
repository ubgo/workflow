package main

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow/examples/internal/capture"

	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/workflowtest"
)

// TestRun runs the program and checks every line it prints, the ones
// the guide shows.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `render got "42 rows"
tiny-1: succeeded
delayed start refused: true
workflow: Job.RunAt unsupported by this driver (missing DelayedStart): job for workflow report
reset failed: true succeeded: false succeeded with Succeeded: true
fork running: false cancelled: true redo of succeeded: true
fork without a step: workflow: invalid spec: ForkRun needs a step
chain after the attempt: [email audit] changed: true
delayed chain job: workflow: Job.RunAt unsupported by this driver (missing DelayedStart): job for workflow later
last missed tick: 12:00:00
trigger labels: 5 tick at 2026-01-05T12:30:00Z trigger manual
key enterprise weight 4 stride 262144
key free weight 1 stride 1048576
CancelAll cancels: [scheduled queued]
pause a queue needs: PauseQueue a workflow needs: PauseWorkflow
slots in use 3 of 4
too many slots: workflow: invalid spec: slot pool: 5 slots asked of 4
heartbeat export row 4000
progress export 40% "4,000 of 10,000 rows" at 09:00:00
without a reporter: workflow: ReportProgress unsupported by this driver: the driver keeps no progress, or ctx is not a step's
`
	if got := out; got != want {
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

// snippet:handshake-suite
// TestStepTimeoutHandshake runs the step-timeout cases on Tiny. Tiny
// claims no CapRunTimeout, so TestConformance skips them, but its attempts
// keep the handshake (attempt in driver.go): the harness says so, and the
// cases of attemptcause and toleratedtimeout that need it run.
func TestStepTimeoutHandshake(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	workflowtest.Run(t, workflowtest.Harness{
		StepTimeoutHandshake: true,
		Groups:               func(group string) bool { return group == "attemptcause" || group == "toleratedtimeout" },
		New: func(t *testing.T) workflowtest.Instance {
			clk := clock.NewFake(start)
			return workflowtest.Instance{Client: NewTiny(clk), Clock: clk}
		},
	})
}

// snippet:end
