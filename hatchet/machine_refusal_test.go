package hatchet

import (
	"errors"
	"testing"

	"github.com/ubgo/workflow"
)

// TestMachineAndReplayRefused: a state machine (and a virtual object)
// serialises a key's events with a Go key function, which Hatchet's
// expression keys cannot evaluate, so Register refuses it naming the
// field; replayed handlers suspend waits with a snooze, which this driver
// does not declare, so NewReplay refuses this client's capabilities.
func TestMachineAndReplayRefused(t *testing.T) {
	machine, err := workflow.DefineMachine(workflow.MachineSpec[struct{}]{
		Name: "m", Initial: "a", Store: workflow.NewMemoryJournal(),
		States: []workflow.MachineState[struct{}]{{Name: "a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var ue *workflow.UnsupportedError
	if err := checkSupported(machine); !errors.As(err, &ue) || ue.Field != "Workflow.Concurrency.Key.Func" {
		t.Fatalf("machine: err = %v, want an UnsupportedError naming the key function", err)
	}
	if caps.Has(workflow.CapSnooze) {
		t.Fatal("the driver declares CapSnooze: update NewReplay's driver list in the docs")
	}
}
