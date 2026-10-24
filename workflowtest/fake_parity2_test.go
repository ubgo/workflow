package workflowtest

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

func TestFakeRecordsChainsAndClosures(t *testing.T) {
	ctx := context.Background()
	f := startedFake(t, nil, oneStep("a", noopHandler), oneStep("b", noopHandler), oneStep("c", noopHandler))
	if _, err := f.Enqueue(ctx, workflow.Job{Workflow: "a", Chain: []workflow.Job{
		{Workflow: "b", Chain: []workflow.Job{{Workflow: "c"}}},
	}}); err != nil {
		t.Fatal(err)
	}
	f.AssertEnqueued(t, "a", HasChain("b", "c"))
	if errs := spyErrs(func(tb testing.TB) { f.AssertEnqueued(tb, "a", HasChain("c")) }); len(errs) != 1 {
		t.Fatalf("a wrong chain passed: %v", errs)
	}

	ran := false
	ref, err := f.EnqueueClosure(ctx, workflow.ClosureJob{Name: "ping", Fn: func(context.Context) error { ran = true; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := f.Status(ctx, ref); st != workflow.StatusQueued || ran {
		t.Fatalf("closure status %q, ran %v; want queued and not run", st, ran)
	}
	f.AssertClosureEnqueued(t, "ping")
	f.AssertEnqueued(t, workflow.ClosureWorkflow, HasLabel(workflow.LabelClosure, "ping"))
	if errs := spyErrs(func(tb testing.TB) { f.AssertClosureEnqueued(tb, "other") }); len(errs) != 1 || !strings.Contains(errs[0], "ping") {
		t.Fatalf("missing closure assertion = %v", errs)
	}
	if err := f.RunClosures(ctx); err != nil || !ran {
		t.Fatalf("RunClosures = %v, ran %v", err, ran)
	}
	if err := f.Finish(ref, workflow.StatusDiscarded); err != nil {
		t.Fatalf("Finish(discarded) on a Fake with run states: %v", err)
	}
}

func TestFakeRefusesWhatADurableDriverRefuses(t *testing.T) {
	ctx := context.Background()
	durable := workflow.CapDelayedStart | workflow.CapDiscard
	f := startedFake(t, []FakeOption{FakeCaps(durable)}, oneStep("a", noopHandler))
	_, err := f.Enqueue(ctx, workflow.Job{Workflow: "a", Chain: []workflow.Job{{Workflow: "a"}}})
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || ue.Field != "Job.Chain" {
		t.Fatalf("chain without CapChain = %v", err)
	}
	if _, err := workflow.EnqueueClosure(ctx, f, workflow.ClosureJob{Name: "x", Fn: func(context.Context) error { return nil }}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("closure without CapClosure = %v", err)
	}
	if _, err := f.EnqueueClosure(ctx, workflow.ClosureJob{Name: "x", Fn: func(context.Context) error { return nil }}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("direct closure without CapClosure = %v", err)
	}
	ref, err := f.Enqueue(ctx, workflow.Job{Workflow: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Finish(ref, workflow.StatusDiscarded); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Finish(discarded) without CapRunStates = %v", err)
	}
	if f.Capabilities().Has(workflow.CapWorkerScale) || NewFake(FakeCaps(^workflow.Caps(0))).Capabilities().Has(workflow.CapWorkerScale) {
		t.Fatal("the Fake claims CapWorkerScale, which it does not serve")
	}
}

func TestRunStepChainEdits(t *testing.T) {
	h := func(ctx context.Context, in []byte) ([]byte, error) {
		if err := workflow.PrependToChain(ctx, workflow.Job{Workflow: "first"}); err != nil {
			return nil, err
		}
		if err := workflow.AppendToChain(ctx, workflow.Job{Workflow: "last"}); err != nil {
			return nil, err
		}
		if string(in) == "fail" {
			return nil, errors.New("boom")
		}
		return nil, nil
	}
	o := RunStep(h, nil, StepChain(workflow.Job{Workflow: "middle"}))
	if !o.Succeeded() || !slices.Equal(o.ChainNames(), []string{"first", "middle", "last"}) {
		t.Fatalf("outcome %v, chain %v", o.Err, o.ChainNames())
	}
	o = RunStep(h, []byte("fail"), StepChain(workflow.Job{Workflow: "middle"}))
	if o.Succeeded() || !slices.Equal(o.ChainNames(), []string{"middle"}) {
		t.Fatalf("a failed attempt kept its edits: %v", o.ChainNames())
	}
	o = RunStep(h, nil, StepCaps(0))
	if !errors.Is(o.Err, workflow.ErrUnsupported) {
		t.Fatalf("chain edit without CapChain = %v", o.Err)
	}
}

func TestFakeNeverClaimsLabelPrefix(t *testing.T) {
	f := NewFake(FakeCaps(workflow.CapLabelPrefix | workflow.CapRunQuery))
	if f.Capabilities().Has(workflow.CapLabelPrefix) {
		t.Fatal("the Fake claims CapLabelPrefix but serves no run query")
	}
}
