package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

func TestSlotPoolFIFO(t *testing.T) {
	p := workflow.NewSlotPool(4)
	ctx := context.Background()
	if err := p.Acquire(ctx, 3); err != nil {
		t.Fatal(err)
	}
	heavy := make(chan error, 1)
	go func() { heavy <- p.Acquire(ctx, 4) }()
	time.Sleep(10 * time.Millisecond) // the heavy waiter is queued
	light := make(chan error, 1)
	go func() { light <- p.Acquire(ctx, 1) }()
	select {
	case <-light:
		t.Fatal("a light attempt passed a heavy one waiting at the head")
	case <-time.After(20 * time.Millisecond):
	}
	p.Release(3)
	if err := <-heavy; err != nil {
		t.Fatal(err)
	}
	if got := p.InUse(); got != 4 {
		t.Fatalf("in use %d, want 4", got)
	}
	p.Release(4)
	if err := <-light; err != nil {
		t.Fatal(err)
	}
	if err := p.Acquire(ctx, 5); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("more than the pool: %v", err)
	}
}

func TestSlotPoolCancel(t *testing.T) {
	p := workflow.NewSlotPool(2)
	if err := p.Acquire(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Acquire(ctx, 2) }()
	time.Sleep(10 * time.Millisecond)
	waiting := make(chan error, 1)
	go func() { waiting <- p.Acquire(context.Background(), 1) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait: %v", err)
	}
	p.Release(2)
	if err := <-waiting; err != nil {
		t.Fatal(err)
	}
	if got := p.InUse(); got != 1 {
		t.Fatalf("in use %d, want 1: a cancelled waiter must hold nothing", got)
	}
}

func TestSlotsRequireAndValidate(t *testing.T) {
	run := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Slots: 2, Run: run}}}
	if !workflow.Requires(wf).Has(workflow.CapSlotCost) {
		t.Fatal("Slots 2 must require CapSlotCost")
	}
	wf.Steps[0].Slots = 1
	if workflow.Requires(wf).Has(workflow.CapSlotCost) {
		t.Fatal("Slots 1 needs nothing")
	}
	wf.Steps[0].Slots = -1
	if err := workflow.Validate(wf); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("negative Slots: %v", err)
	}
	if err := workflow.CheckSlots(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Slots: 3, Run: run}}}, 2); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("CheckSlots: %v", err)
	}
}
