package otel

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestTraceIDOf: the trace ID the enqueue middleware writes into a job's
// labels comes back out, and labels without a valid traceparent say so.
func TestTraceIDOf(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	spanID, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	parent := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled}))

	inst, err := New(WithTracerProvider(noop.NewTracerProvider()))
	if err != nil {
		t.Fatal(err)
	}
	var labels map[string]string
	enqueue := inst.Enqueue()(func(_ context.Context, job workflow.Job) (workflow.RunRef, error) {
		labels = job.Labels
		return workflow.RunRef{RunID: "r1"}, nil
	})
	if _, err := enqueue(parent, workflow.Job{Workflow: "w", Labels: map[string]string{"tenant": "a"}}); err != nil {
		t.Fatal(err)
	}
	if id, ok := TraceIDOf(labels); !ok || id != "0af7651916cd43dd8448eb211c80319c" {
		t.Fatalf("TraceIDOf(%v) = %q, %v", labels, id, ok)
	}
	if sc := SpanContextOf(labels, nil); sc.TraceID() != traceID {
		t.Fatalf("SpanContextOf: %v", sc)
	}

	for name, l := range map[string]map[string]string{
		"nil":        nil,
		"no otel":    {"tenant": "a"},
		"malformed":  {LabelPrefix + "traceparent": "00-nothex-b7ad6b7169203331-01"},
		"zero trace": {LabelPrefix + "traceparent": "00-00000000000000000000000000000000-b7ad6b7169203331-01"},
		"unprefixed": {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
	} {
		if id, ok := TraceIDOf(l); ok || id != "" {
			t.Errorf("%s: TraceIDOf = %q, %v; want none", name, id, ok)
		}
	}
	// Another propagator reads its own keys.
	b3ish := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	if sc := SpanContextOf(labels, b3ish); !sc.IsValid() {
		t.Error("SpanContextOf with an explicit propagator found nothing")
	}
}
