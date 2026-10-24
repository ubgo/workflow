// Command opentelemetry traces and measures an in-memory driver with the
// otel module: a span per enqueue and per step attempt, the trace context
// carried from one to the other in job labels, the three parentings, span
// names of the app's own, labels on spans and metrics, and the refusal of
// a second Enqueue middleware in one chain. Spans go to an in-memory
// exporter and metrics to a manual reader, so the program prints what a
// trace backend would receive.
//
// It is the program behind docs/observability/opentelemetry.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails
// when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	wfotel "github.com/ubgo/workflow/otel"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "opentelemetry:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "-- default: the step span is a child of the enqueue span")
	if err := traced(ctx, out); err != nil {
		return err
	}
	fmt.Fprintln(out, "-- ParentNewRootLink and span names of our own")
	// snippet:parenting
	opts := []wfotel.Option{
		wfotel.WithParenting(wfotel.ParentNewRootLink),
		wfotel.WithStepSpanName(func(info workflow.StepInfo) string { return "step " + info.Step }),
		wfotel.WithEnqueueSpanName(func(job workflow.Job) string { return "enqueue " + job.Workflow }),
	}
	// snippet:end
	return traced(ctx, out, opts...)
}

// traced runs one job through a client instrumented with opts and prints
// the spans and metrics it produced.
func traced(ctx context.Context, out io.Writer, opts ...wfotel.Option) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:providers
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	// snippet:end

	// snippet:setup
	inst, err := wfotel.New(append([]wfotel.Option{
		wfotel.WithTracerProvider(tp),
		wfotel.WithMeterProvider(mp),
		wfotel.WithLabelFilter(func(key string) bool { return key == "tier" }), // span attribute workflow.label.tier
		wfotel.WithMetricLabels("tier"),                                        // a label with few values only
	}, opts...)...)
	if err != nil {
		return err
	}
	c := memory.New(memory.WithMiddleware(inst.Step())) // first: the span covers the other middleware
	client := middleware.WrapClient(c, inst.Enqueue())
	// snippet:end

	if err := c.Register(workflow.Workflow{
		Name:  "send-email",
		Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	}); err != nil {
		return err
	}
	// snippet:run-ends
	if err := c.OnRunFinished(inst.RunFinished()); err != nil { // counts run ends by status
		return err
	}
	// snippet:end
	ends := make(chan workflow.RunEvent, 1)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:labels
	ref, err := client.Enqueue(ctx, workflow.Job{Workflow: "send-email", Labels: map[string]string{"tier": "pro", "user": "u-42"}})
	if err != nil {
		return err
	}
	// snippet:end
	<-ends

	byKind := map[trace.SpanKind]tracetest.SpanStub{}
	for _, s := range spans.GetSpans() {
		byKind[s.SpanKind] = s
	}
	enq, step := byKind[trace.SpanKindProducer], byKind[trace.SpanKindConsumer]
	for _, s := range []tracetest.SpanStub{enq, step} {
		fmt.Fprintf(out, "span %q kind=%s %s\n", s.Name, s.SpanKind, attrs(s.Attributes))
	}
	fmt.Fprintf(out, "same trace: %v, step parent is the enqueue span: %v, step links to it: %v\n",
		step.SpanContext.TraceID() == enq.SpanContext.TraceID(),
		step.Parent.SpanID() == enq.SpanContext.SpanID(),
		slices.ContainsFunc(step.Links, func(l sdktrace.Link) bool { return l.SpanContext.SpanID() == enq.SpanContext.SpanID() }))
	fmt.Fprintf(out, "run ID on the enqueue span: %v\n", slices.Contains(enq.Attributes, wfotel.AttrRunID.String(ref.RunID.String())))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		return err
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			fmt.Fprintf(out, "metric %s %s\n", m.Name, metricAttrs(m.Data))
		}
	}

	// snippet:twice
	twice := middleware.WrapClient(client, inst.Enqueue())
	_, err = twice.Enqueue(ctx, workflow.Job{Workflow: "send-email"})
	fmt.Fprintln(out, "Enqueue middleware twice:", errors.Is(err, workflow.ErrInvalid))
	// snippet:end
	return nil
}

// attrs prints span attributes sorted, without the run ID, which changes
// on every run, and without the node, which is this machine's host name and
// process ID.
func attrs(kvs []attribute.KeyValue) string {
	var parts []string
	for _, kv := range kvs {
		if kv.Key == wfotel.AttrRunID || kv.Key == wfotel.AttrNode {
			continue
		}
		parts = append(parts, string(kv.Key)+"="+kv.Value.String())
	}
	slices.Sort(parts)
	return strings.Join(parts, " ")
}

// metricAttrs prints the attribute set of a metric's single data point.
func metricAttrs(data metricdata.Aggregation) string {
	var set attribute.Set
	switch d := data.(type) {
	case metricdata.Sum[int64]:
		set = d.DataPoints[0].Attributes
	case metricdata.Histogram[float64]:
		set = d.DataPoints[0].Attributes
	}
	return attrs(set.ToSlice())
}
