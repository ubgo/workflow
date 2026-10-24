package workflowtest

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/memory"
)

func TestRecorderBatchesAndChains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewRecorder(memory.New())
	for _, wf := range []workflow.Workflow{oneStep("resize", noopHandler), chain("order", "charge", "ship")} {
		if err := r.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	m := batch.New(r, batch.NewMemoryStore(), batch.WithManualCallbackRetry())
	r.AssertNothingBatched(t)
	if _, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "resize"}, {Workflow: "resize"}}}); err != nil {
		t.Fatal(err)
	}
	r.AssertBatchCount(t, 1)
	r.AssertBatched(t, BatchSize(2), BatchHasJob("resize"))
	if errs := spyErrs(func(tb testing.TB) { r.AssertBatched(tb, BatchSize(3)) }); len(errs) != 1 {
		t.Errorf("AssertBatched(3 jobs) reported %q", errs)
	}

	if errs := spyErrs(func(tb testing.TB) { r.AssertChained(tb, "order", "charge", "ship") }); len(errs) != 1 {
		t.Errorf("a chain never enqueued reported %q", errs)
	}
	if _, err := r.Enqueue(ctx, workflow.Job{Workflow: "order"}); err != nil {
		t.Fatal(err)
	}
	r.AssertChained(t, "order", "charge", "ship")
	if errs := spyErrs(func(tb testing.TB) { r.AssertChained(tb, "order", "ship", "charge") }); len(errs) != 1 {
		t.Errorf("a wrong chain reported %q", errs)
	}

	// The standalone fake keeps definitions too.
	fake := NewRecorder(nil)
	if err := fake.Register(chain("c", "x", "y")); err != nil {
		t.Fatal(err)
	}
	if _, ok := fake.Workflow("c"); !ok {
		t.Error("NewRecorder(nil) lost a registered definition")
	}
}
