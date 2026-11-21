// Command opentelemetry traces and measures an in-memory driver with the
// otel module: a span per enqueue and per step attempt, the trace context
// carried from one to the other in job labels, the three parentings, span
// names of the app's own, labels on spans and metrics, and the refusal of
// a second Enqueue middleware in one chain; then every label on spans, a
// propagator that carries W3C baggage too, the trace labels a run carries,
// and the metric instruments read by name with step durations measured on
// a fake clock. Spans go to an in-memory exporter and metrics to a manual
// reader, so the program prints what a trace backend would receive.
//
// It is the program behind docs/observability/opentelemetry.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails
// when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	wfotel "github.com/ubgo/workflow/otel"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "opentelemetry:", err)
		os.Exit(1)
	}
}

// stepTime is how long the step takes on the fake clock of the third run,
// so its duration metric reads the same on every machine.
const stepTime = 1500 * time.Millisecond

// clockStart is the fake clock's first instant; any fixed time works.
var clockStart = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// demo is one instrumented run: the otel options on top of the shared
// setup, and the fake clock the step advances (nil on real time, where the
// duration is not printed because it changes on every run).
type demo struct {
	opts []wfotel.Option
	clk  *clock.Fake
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	fmt.Println("-- default: the step span is a child of the enqueue span")
	if err := traced(ctx, demo{}); err != nil {
		return err
	}
	fmt.Println("-- ParentNewRootLink and span names of our own")
	// snippet:parenting
	parenting := wfotel.WithParenting(wfotel.ParentNewRootLink)
	// snippet:end
	// snippet:span-names
	names := []wfotel.Option{
		// A bounded set of names: the step name, never a run ID.
		wfotel.WithStepSpanName(func(info workflow.StepInfo) string { return "step " + info.Step }),
		wfotel.WithEnqueueSpanName(func(job workflow.Job) string { return "enqueue " + job.Workflow }),
	}
	// snippet:end
	if err := traced(ctx, demo{opts: append([]wfotel.Option{parenting}, names...)}); err != nil {
		return err
	}

	fmt.Println("-- every label, W3C baggage in the trace labels, durations on a fake clock")
	// snippet:propagation
	// W3C trace context plus baggage, so a value the caller put in the
	// baggage reaches the step on another machine.
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	withBaggage := wfotel.WithPropagator(prop)
	// snippet:end
	// snippet:all-labels
	// Every label but the otel. ones becomes a span attribute: only where
	// no label holds personal data. WithLabelFilter picks keys instead.
	allLabels := wfotel.WithAllLabels()
	fmt.Println("span attribute of the user label:", wfotel.LabelAttributePrefix+"user")
	// snippet:end
	// snippet:clock
	clk := clock.NewFake(clockStart)
	measured := wfotel.WithClock(clk) // step durations are measured on this clock
	// snippet:end
	return traced(ctx, demo{opts: []wfotel.Option{withBaggage, allLabels, measured}, clk: clk})
}

// traced runs one job through a client instrumented with d's options and
// prints the spans and metrics it produced.
func traced(ctx context.Context, d demo) error {
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
	}, d.opts...)...)
	if err != nil {
		return err
	}
	c := memory.New(memory.WithMiddleware(inst.Step())) // first: the span covers the other middleware
	client := middleware.WrapClient(c, inst.Enqueue())
	// snippet:end

	// The step reports what the trace context left on the run: the label
	// keys under otel.LabelPrefix and the baggage the propagator restored.
	var stepSaw string
	if err := c.Register(workflow.Workflow{
		Name: "send-email",
		Steps: []workflow.Step{{Name: "send", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			if d.clk != nil {
				stepSaw = traceLabels(ctx)
				d.clk.Advance(stepTime)
			}
			return nil, nil
		}}},
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
	if d.clk != nil {
		// The caller's baggage: a propagator with propagation.Baggage
		// carries it in the run's labels.
		plan, err := baggage.NewMember("plan", "pro")
		if err != nil {
			return err
		}
		bag, err := baggage.New(plan)
		if err != nil {
			return err
		}
		ctx = baggage.ContextWithBaggage(ctx, bag)
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
		fmt.Printf("span %q kind=%s %s\n", s.Name, s.SpanKind, attrs(s.Attributes))
	}
	fmt.Printf("same trace: %v, step parent is the enqueue span: %v, step links to it: %v\n",
		step.SpanContext.TraceID() == enq.SpanContext.TraceID(),
		step.Parent.SpanID() == enq.SpanContext.SpanID(),
		slices.ContainsFunc(step.Links, func(l sdktrace.Link) bool { return l.SpanContext.SpanID() == enq.SpanContext.SpanID() }))
	fmt.Printf("run ID on the enqueue span: %v\n", slices.Contains(enq.Attributes, wfotel.AttrRunID.String(ref.RunID.String())))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		return err
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			fmt.Printf("metric %s %s\n", m.Name, metricAttrs(m.Data))
		}
	}
	if d.clk != nil {
		fmt.Println(stepSaw)
		ok, secs := stepMetrics(rm)
		fmt.Printf("step attempts with outcome %s: %d, step time: %.1fs\n", wfotel.OutcomeOK, ok, secs)
	}

	// snippet:twice
	twice := middleware.WrapClient(client, inst.Enqueue())
	_, err = twice.Enqueue(ctx, workflow.Job{Workflow: "send-email"})
	fmt.Println("Enqueue middleware twice:", errors.Is(err, workflow.ErrInvalid))
	// snippet:end
	return nil
}

// snippet:trace-labels
// traceLabels reads, inside a step, the run's labels under otel.LabelPrefix
// (the trace context the enqueue middleware wrote) and the baggage the
// step middleware restored from them.
func traceLabels(ctx context.Context) string {
	info, _ := workflow.StepInfoFrom(ctx)
	var keys []string
	for k := range info.Labels {
		if strings.HasPrefix(k, wfotel.LabelPrefix) { // "otel.": reserved for the trace context
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	plan := baggage.FromContext(ctx).Member("plan").Value()
	return fmt.Sprintf("trace labels on the run: %s; baggage in the step: plan=%s", strings.Join(keys, " "), plan)
}

// snippet:end

// snippet:read-metrics
// stepMetrics reads two instruments by their names: the attempts that
// ended with outcome ok, and the seconds the step histogram summed.
func stepMetrics(rm metricdata.ResourceMetrics) (ok int64, seconds float64) {
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != wfotel.ScopeName {
			continue
		}
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				if m.Name != wfotel.MetricStepOutcomes {
					continue
				}
				for _, p := range data.DataPoints {
					if v, _ := p.Attributes.Value(wfotel.AttrOutcome); v.AsString() == string(wfotel.OutcomeOK) {
						ok += p.Value
					}
				}
			case metricdata.Histogram[float64]:
				if m.Name != wfotel.MetricStepDuration {
					continue
				}
				for _, p := range data.DataPoints {
					seconds += p.Sum
				}
			}
		}
	}
	return ok, seconds
}

// snippet:end

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
