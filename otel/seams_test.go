package otel_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"

	"github.com/ubgo/workflow"
	wfotel "github.com/ubgo/workflow/otel"
)

var noopWF = workflow.Workflow{Name: "w", Steps: []workflow.Step{
	{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
}}

func TestParenting(t *testing.T) {
	for _, tc := range []struct {
		p                  wfotel.Parenting
		wantChild, wantLnk bool
	}{
		{"", true, false}, // default unchanged
		{wfotel.ParentChild, true, false},
		{wfotel.ParentLink, false, true},
		{wfotel.ParentNewRootLink, false, true},
	} {
		t.Run(string(tc.p), func(t *testing.T) {
			var opts []wfotel.Option
			if tc.p != "" {
				opts = append(opts, wfotel.WithParenting(tc.p))
			}
			h := newHarness(t, opts...)
			caller, _ := h.run(t, noopWF, workflow.Job{Workflow: "w"})
			spans := h.spans.GetSpans()
			enq := spanNamed(t, spans, "workflow.enqueue w")[0]
			step := spanNamed(t, spans, "workflow.step w/a")[0]
			isChild := step.Parent.SpanID() == enq.SpanContext.SpanID()
			linked := len(step.Links) == 1 && step.Links[0].SpanContext.SpanID() == enq.SpanContext.SpanID()
			if isChild != tc.wantChild || linked != tc.wantLnk {
				t.Fatalf("child=%v linked=%v, want child=%v linked=%v", isChild, linked, tc.wantChild, tc.wantLnk)
			}
			sameTrace := step.SpanContext.TraceID() == caller.TraceID()
			if sameTrace != tc.wantChild {
				t.Fatalf("step in the caller's trace = %v, want %v", sameTrace, tc.wantChild)
			}
		})
	}
	if _, err := wfotel.New(wfotel.WithParenting("sideways")); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("unknown parenting: %v", err)
	}
}

// TestParentLink_KeepsCtxSpan: under ParentLink the span ctx already holds
// (an engine SDK's) stays the parent; under ParentNewRootLink it does not.
func TestParentLink_KeepsCtxSpan(t *testing.T) {
	for _, p := range []wfotel.Parenting{wfotel.ParentLink, wfotel.ParentNewRootLink} {
		h := newHarness(t, wfotel.WithParenting(p))
		_, enq := h.tp.Tracer("test").Start(context.Background(), "enqueue")
		labels := map[string]string{}
		wfotel.InjectForTest(trace.ContextWithSpan(context.Background(), enq), labels)
		ctx, engine := h.tp.Tracer("test").Start(context.Background(), "engine")
		ctx = workflow.WithStepInfo(ctx, workflow.StepInfo{Workflow: "w", Step: "a", Labels: labels})
		if _, err := h.inst.Step()(func(context.Context, []byte) ([]byte, error) { return nil, nil })(ctx, nil); err != nil {
			t.Fatal(err)
		}
		engine.End()
		enq.End()
		s := spanNamed(t, h.spans.GetSpans(), "workflow.step w/a")[0]
		underEngine := s.Parent.SpanID() == engine.SpanContext().SpanID()
		if underEngine != (p == wfotel.ParentLink) {
			t.Fatalf("%s: parent is the engine span = %v", p, underEngine)
		}
		if len(s.Links) != 1 || s.Links[0].SpanContext.SpanID() != enq.SpanContext().SpanID() {
			t.Fatalf("%s: links = %+v", p, s.Links)
		}
	}
}

func TestSpanNames(t *testing.T) {
	h := newHarness(t,
		wfotel.WithStepSpanName(func(i workflow.StepInfo) string { return "step:" + i.Step }),
		wfotel.WithEnqueueSpanName(func(j workflow.Job) string {
			if j.Workflow == "w" {
				return "" // falls back to the default
			}
			return "enq"
		}))
	h.run(t, noopWF, workflow.Job{Workflow: "w"})
	spanNamed(t, h.spans.GetSpans(), "step:a")
	spanNamed(t, h.spans.GetSpans(), "workflow.enqueue w")
}

func TestMetricLabels(t *testing.T) {
	h := newHarness(t, wfotel.WithMetricLabels("tier"))
	h.run(t, noopWF, workflow.Job{Workflow: "w", Labels: map[string]string{"tier": "gold", "user": "u1"}})
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	tierKey := attribute.Key(wfotel.LabelAttributePrefix + "tier")
	userKey := attribute.Key(wfotel.LabelAttributePrefix + "user")
	seen := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			var sets []attribute.Set
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					sets = append(sets, p.Attributes)
				}
			case metricdata.Histogram[float64]:
				for _, p := range d.DataPoints {
					sets = append(sets, p.Attributes)
				}
			}
			for _, set := range sets {
				if v, ok := set.Value(tierKey); !ok || v.AsString() != "gold" {
					t.Fatalf("%s lacks the tier attribute: %v", m.Name, set)
				}
				if set.HasValue(userKey) {
					t.Fatalf("%s recorded a label not asked for", m.Name)
				}
				seen[m.Name] = true
			}
		}
	}
	for _, name := range []string{wfotel.MetricStepDuration, wfotel.MetricStepOutcomes, wfotel.MetricEnqueueCount} {
		if !seen[name] {
			t.Fatalf("no data point for %s", name)
		}
	}
	for _, bad := range []string{"", wfotel.LabelPrefix + "traceparent"} {
		if _, err := wfotel.New(wfotel.WithMetricLabels(bad)); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("key %q accepted: %v", bad, err)
		}
	}
}
