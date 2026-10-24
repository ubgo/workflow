package otel_test

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	wfotel "github.com/ubgo/workflow/otel"
)

// TestNodeAndTenant pins the batch-5 attributes: workflow.node on every
// step span whose StepInfo names a node, workflow.tenant on step and
// enqueue spans only with WithTenant.
func TestNodeAndTenant(t *testing.T) {
	step := func(h *harness) {
		ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "w", Step: "a", Node: "n1", Tenant: "acme"})
		if _, err := h.inst.Step()(func(context.Context, []byte) ([]byte, error) { return nil, nil })(ctx, nil); err != nil {
			t.Fatal(err)
		}
		next := func(context.Context, workflow.Job) (workflow.RunRef, error) { return workflow.RunRef{RunID: "r"}, nil }
		if _, err := h.inst.Enqueue()(next)(context.Background(), workflow.Job{Workflow: "w", Tenant: "acme"}); err != nil {
			t.Fatal(err)
		}
	}
	off := newHarness(t)
	step(off)
	for _, s := range off.spans.GetSpans() {
		if _, ok := attr(s, wfotel.AttrTenant); ok {
			t.Fatalf("span %q records the tenant without WithTenant", s.Name)
		}
		if s.Name == "workflow.step w/a" {
			if v, ok := attr(s, wfotel.AttrNode); !ok || v.AsString() != "n1" {
				t.Fatalf("step span node = %v (%v), want n1", v, ok)
			}
		}
	}
	on := newHarness(t, wfotel.WithTenant())
	step(on)
	spans := on.spans.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("%d spans, want a step and an enqueue span", len(spans))
	}
	for _, s := range spans {
		if v, ok := attr(s, wfotel.AttrTenant); !ok || v.AsString() != "acme" {
			t.Fatalf("span %q tenant = %v (%v), want acme", s.Name, v, ok)
		}
	}
}
