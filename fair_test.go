package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
)

// TestFairTurn pins the start-time fair queueing step every driver shares
// (FC-05, FC-06).
func TestFairTurn(t *testing.T) {
	unit := int64(workflow.MaxKeyWeight)
	if got := workflow.FairStride(1); got != unit {
		t.Errorf("FairStride(1) = %d, want %d", got, unit)
	}
	if got := workflow.FairStride(4); got != unit/4 {
		t.Errorf("FairStride(4) = %d, want %d", got, unit/4)
	}
	for _, w := range []int{0, -3} {
		if got := workflow.FairStride(w); got != unit {
			t.Errorf("FairStride(%d) = %d, want the weight-1 stride %d", w, got, unit)
		}
	}
	if got := workflow.FairStride(workflow.MaxKeyWeight * 4); got != 1 {
		t.Errorf("FairStride above MaxKeyWeight = %d, want 1", got)
	}
	// A backlogged key advances by its stride.
	if s, n := workflow.FairTurn(5*unit, 2*unit, 1); s != 5*unit || n != 6*unit {
		t.Errorf("FairTurn(ahead) = %d, %d; want %d, %d", s, n, 5*unit, 6*unit)
	}
	// A key that was idle (pass behind the virtual time) rejoins at it.
	if s, n := workflow.FairTurn(unit, 9*unit, 2); s != 9*unit || n != 9*unit+unit/2 {
		t.Errorf("FairTurn(idle) = %d, %d; want %d, %d", s, n, 9*unit, 9*unit+unit/2)
	}
}

func TestKeyWeights(t *testing.T) {
	m := map[string]int{"gold": 3}
	w := workflow.KeyWeights(m, 2)
	m["gold"] = 9 // the map is copied
	if w("gold") != 3 || w("free") != 2 {
		t.Errorf("KeyWeights: gold %d, free %d; want 3, 2", w("gold"), w("free"))
	}
	conc := &workflow.Concurrency{Strategy: workflow.ConcurrencyRoundRobin, Weight: func(k string) int {
		return map[string]int{"neg": -1, "huge": workflow.MaxKeyWeight + 1, "five": 5}[k]
	}}
	for key, want := range map[string]int{"neg": 1, "huge": workflow.MaxKeyWeight, "five": 5} {
		if got := workflow.FairWeight(conc, key); got != want {
			t.Errorf("FairWeight(%q) = %d, want %d", key, got, want)
		}
	}
	if got := workflow.FairWeight(nil, "x"); got != 1 {
		t.Errorf("FairWeight(nil) = %d, want 1", got)
	}
	if got := workflow.FairWeight(&workflow.Concurrency{}, "x"); got != 1 {
		t.Errorf("FairWeight without Weight = %d, want 1", got)
	}
}

// TestWeightNeedsRoundRobin: Weight is refused with any other strategy,
// and asks for CapKeyWeights.
func TestWeightNeedsRoundRobin(t *testing.T) {
	step := []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}
	weight := workflow.KeyWeights(nil, 1)
	for _, s := range []workflow.ConcurrencyStrategy{"", workflow.ConcurrencyQueueNewest, workflow.ConcurrencyDropNewest, workflow.ConcurrencyCancelInProgress} {
		wf := workflow.Workflow{Name: "w", Steps: step, Concurrency: &workflow.Concurrency{MaxRuns: 1, Strategy: s, Weight: weight}}
		if err := workflow.Validate(wf); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("strategy %q with Weight: Validate = %v, want ErrInvalid", s, err)
		}
	}
	wf := workflow.Workflow{Name: "w", Steps: step, Concurrency: &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyRoundRobin, Weight: weight}}
	if err := workflow.Validate(wf); err != nil {
		t.Fatalf("round robin with Weight: %v", err)
	}
	if !workflow.Requires(wf).Has(workflow.CapKeyWeights) {
		t.Error("Requires misses CapKeyWeights")
	}
	wf.Concurrency.Weight = nil
	if workflow.Requires(wf).Has(workflow.CapKeyWeights) || workflow.Requires(wf).Has(workflow.CapRoundRobin) {
		t.Error("round robin without Weight asks for CapKeyWeights or CapRoundRobin; existing workflows must run where they ran")
	}
}
