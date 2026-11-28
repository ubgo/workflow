package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

// holdWorkflow is a one-step workflow for the hold-policy tests.
func holdWorkflow() workflow.Workflow {
	return workflow.Workflow{Name: "widgets", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
}

// TestRequiresHoldWhileRunning pins that only a HoldWhileRunning key asks
// for CapHoldWhileRunning: the zero value and HoldWholeRun change nothing.
func TestRequiresHoldWhileRunning(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*workflow.Workflow)
		want bool
	}{
		{"no concurrency", func(*workflow.Workflow) {}, false},
		{"zero hold", func(w *workflow.Workflow) { w.Concurrency = &workflow.Concurrency{MaxRuns: 1} }, false},
		{"whole run", func(w *workflow.Workflow) {
			w.Concurrency = &workflow.Concurrency{MaxRuns: 1, Hold: workflow.HoldWholeRun}
		}, false},
		{"concurrency while running", func(w *workflow.Workflow) {
			w.Concurrency = &workflow.Concurrency{MaxRuns: 1, Hold: workflow.HoldWhileRunning}
		}, true},
		{"limit while running", func(w *workflow.Workflow) {
			w.ConcurrencyLimits = []workflow.ConcurrencyLimit{{MaxRuns: 1}, {MaxRuns: 2, Hold: workflow.HoldWhileRunning}}
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := holdWorkflow()
			tc.set(&wf)
			if got := workflow.Requires(wf).Has(workflow.CapHoldWhileRunning); got != tc.want {
				t.Fatalf("Requires has CapHoldWhileRunning = %v, want %v", got, tc.want)
			}
			if got := workflow.HoldsWhileRunning(wf); got != tc.want {
				t.Fatalf("HoldsWhileRunning = %v, want %v", got, tc.want)
			}
			if err := workflow.Validate(wf); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

// TestValidateHold pins the combinations Register refuses, each with
// ErrInvalid naming the fields.
func TestValidateHold(t *testing.T) {
	for _, tc := range []struct {
		name   string
		set    func(*workflow.Workflow)
		fields []string
	}{
		{"unknown concurrency hold", func(w *workflow.Workflow) {
			w.Concurrency = &workflow.Concurrency{MaxRuns: 1, Hold: "sometimes"}
		}, []string{"Concurrency.Hold"}},
		{"unknown limit hold", func(w *workflow.Workflow) {
			w.ConcurrencyLimits = []workflow.ConcurrencyLimit{{MaxRuns: 1}, {MaxRuns: 1, Hold: "sometimes"}}
		}, []string{"ConcurrencyLimits[1].Hold"}},
		{"with FIFO", func(w *workflow.Workflow) {
			w.Concurrency = &workflow.Concurrency{MaxRuns: 1, FIFO: true, Hold: workflow.HoldWhileRunning}
		}, []string{"Concurrency.Hold", "Concurrency.FIFO"}},
		{"with drop newest", func(w *workflow.Workflow) {
			w.Concurrency = &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyDropNewest, Hold: workflow.HoldWhileRunning}
		}, []string{"Concurrency.Hold", "Concurrency.Strategy"}},
		{"with cancel in progress", func(w *workflow.Workflow) {
			w.Concurrency = &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyCancelInProgress, Hold: workflow.HoldWhileRunning}
		}, []string{"Concurrency.Hold", "Concurrency.Strategy"}},
		{"with round robin", func(w *workflow.Workflow) {
			w.Concurrency = &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyRoundRobin, Hold: workflow.HoldWhileRunning}
		}, []string{"Concurrency.Hold", "Concurrency.Strategy"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := holdWorkflow()
			tc.set(&wf)
			err := workflow.Validate(wf)
			if !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("Validate = %v, want ErrInvalid", err)
			}
			for _, f := range tc.fields {
				if !strings.Contains(err.Error(), f) {
					t.Fatalf("Validate = %q, want it to name %s", err, f)
				}
			}
		})
	}
	// FIFO stays allowed when only a limit gives its slot back: the FIFO
	// key itself is held for the whole run.
	wf := holdWorkflow()
	wf.Concurrency = &workflow.Concurrency{MaxRuns: 1, FIFO: true}
	wf.ConcurrencyLimits = []workflow.ConcurrencyLimit{{MaxRuns: 1, Hold: workflow.HoldWhileRunning}}
	if err := workflow.Validate(wf); err != nil {
		t.Fatalf("FIFO beside a HoldWhileRunning limit: %v", err)
	}
}
