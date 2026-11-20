package sdktest

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/ubgo/workflow"
)

// TestTraceAcrossAttributes (MQ-16): the trace context the enqueue
// middleware writes into a job's labels crosses a broker hop as message
// attributes under the W3C names, and a consumer's propagator extracts
// the enqueuing span from them, on every transport profile.
func TestTraceAcrossAttributes(t *testing.T) {
	h := newHarness(t)
	var job workflow.Job
	enqueue := h.inst.Enqueue()(func(_ context.Context, j workflow.Job) (workflow.RunRef, error) {
		job = j
		return workflow.RunRef{}, nil
	})
	ctx, parent := h.tp.Tracer("test").Start(context.Background(), "producer")
	if _, err := enqueue(ctx, workflow.Job{Workflow: "w", Labels: map[string]string{"source": "shop"}}); err != nil {
		t.Fatal(err)
	}
	parent.End()
	want := parent.SpanContext().TraceID()
	for _, lim := range []workflow.AttributeLimits{workflow.SQSAttributes, workflow.PubSubAttributes, workflow.KafkaHeaders, workflow.NATSHeaders, workflow.HTTPHeaders} {
		codec := workflow.Attributes{Limits: lim}
		attrs, err := codec.Encode(job)
		if err != nil {
			t.Fatalf("%s: %v", lim.Name, err)
		}
		d, err := codec.Decode(attrs)
		if err != nil {
			t.Fatalf("%s: %v", lim.Name, err)
		}
		got := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(d.Trace)))
		if !got.IsValid() || got.TraceID() != want {
			t.Fatalf("%s: extracted %v from %v; want trace %v", lim.Name, got, d.Trace, want)
		}
		if d.Job.Labels["source"] != "shop" || len(codec.TraceLabels(d)) == 0 {
			t.Fatalf("%s: decoded labels %v, trace labels %v", lim.Name, d.Job.Labels, codec.TraceLabels(d))
		}
	}
}
