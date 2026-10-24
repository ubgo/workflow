package otel_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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

type harness struct {
	spans  *tracetest.InMemoryExporter
	reader *sdkmetric.ManualReader
	tp     *sdktrace.TracerProvider
	inst   *wfotel.Instrumentation
}

func newHarness(t *testing.T, opts ...wfotel.Option) *harness {
	t.Helper()
	h := &harness{spans: tracetest.NewInMemoryExporter(), reader: sdkmetric.NewManualReader()}
	h.tp = sdktrace.NewTracerProvider(sdktrace.WithSyncer(h.spans))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(h.reader))
	inst, err := wfotel.New(append([]wfotel.Option{wfotel.WithTracerProvider(h.tp), wfotel.WithMeterProvider(mp)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	h.inst = inst
	return h
}

// run registers wf on a memory client wired with the instrumentation,
// enqueues job under a parent span and waits for the run to finish.
func (h *harness) run(t *testing.T, wf workflow.Workflow, job workflow.Job) (trace.SpanContext, workflow.RunRef) {
	t.Helper()
	c := memory.New(memory.WithMiddleware(h.inst.Step()))
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	client := middleware.WrapClient(c, h.inst.Enqueue())
	pctx, parent := h.tp.Tracer("test").Start(ctx, "caller")
	ref, err := client.Enqueue(pctx, job)
	parent.End()
	if err != nil {
		t.Fatal(err)
	}
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if st.IsTerminal() {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("run did not finish")
		case <-time.After(5 * time.Millisecond):
		}
	}
	return parent.SpanContext(), ref
}

func spanNamed(t *testing.T, spans tracetest.SpanStubs, name string) []tracetest.SpanStub {
	t.Helper()
	var out []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == name {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		t.Fatalf("no span %q in %d spans", name, len(spans))
	}
	return out
}

func attr(s tracetest.SpanStub, k attribute.Key) (attribute.Value, bool) {
	for _, a := range s.Attributes {
		if a.Key == k {
			return a.Value, true
		}
	}
	return attribute.Value{}, false
}

func TestSpansAndPropagation(t *testing.T) {
	h := newHarness(t, wfotel.WithAllLabels())
	payload := []byte(`{"secret":"s3cr3t"}`)
	callerLabels := map[string]string{"tenant": "t1"}
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) { return in, nil }},
		{Name: "b", After: []string{"a"}, Run: func(ctx context.Context, in []byte) ([]byte, error) { return nil, nil }},
	}}
	parent, ref := h.run(t, wf, workflow.Job{Workflow: "w", Payload: payload, Labels: callerLabels})

	if len(callerLabels) != 1 {
		t.Fatalf("caller's labels were modified: %v", callerLabels)
	}
	spans := h.spans.GetSpans()
	enq := spanNamed(t, spans, "workflow.enqueue w")[0]
	if enq.Parent.SpanID() != parent.SpanID() || enq.SpanKind != trace.SpanKindProducer {
		t.Fatalf("enqueue span parent/kind wrong: %+v", enq)
	}
	if v, _ := attr(enq, wfotel.AttrRunID); v.AsString() != ref.RunID.String() {
		t.Fatalf("enqueue run id = %q, want %q", v.AsString(), ref.RunID)
	}
	for _, name := range []string{"workflow.step w/a", "workflow.step w/b"} {
		s := spanNamed(t, spans, name)[0]
		if s.Parent.SpanID() != enq.SpanContext.SpanID() || s.SpanContext.TraceID() != parent.TraceID() {
			t.Fatalf("%s is not a child of the enqueue span", name)
		}
		if v, _ := attr(s, wfotel.AttrRunID); v.AsString() != ref.RunID.String() {
			t.Fatalf("%s run id = %q", name, v.AsString())
		}
		if v, _ := attr(s, wfotel.AttrAttempt); v.AsInt64() != 1 {
			t.Fatalf("%s attempt = %d", name, v.AsInt64())
		}
		if v, _ := attr(s, wfotel.LabelAttributePrefix+"tenant"); v.AsString() != "t1" {
			t.Fatalf("%s tenant label = %q", name, v.AsString())
		}
		if _, ok := attr(s, wfotel.LabelAttributePrefix+wfotel.LabelPrefix+"traceparent"); ok {
			t.Fatalf("%s records the reserved trace label", name)
		}
		for _, a := range s.Attributes {
			if a.Value.AsString() == string(payload) {
				t.Fatalf("%s records the payload under %s", name, a.Key)
			}
		}
	}
}

func TestOutcomesAndMetrics(t *testing.T) {
	h := newHarness(t)
	boom := errors.New("boom")
	wf := workflow.Workflow{Name: "w", Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{
		{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			if info.Attempt == 1 {
				return nil, boom
			}
			return nil, workflow.Permanent(boom)
		}},
	}}
	h.run(t, wf, workflow.Job{Workflow: "w"})

	steps := spanNamed(t, h.spans.GetSpans(), "workflow.step w/a")
	if len(steps) != 2 {
		t.Fatalf("got %d step spans, want 2 (one per attempt)", len(steps))
	}
	for _, s := range steps {
		if s.Status.Code != codes.Error {
			t.Fatalf("failed attempt span status = %v", s.Status.Code)
		}
	}

	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	counts := map[string]map[string]int64{}
	var histCount uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					o, _ := p.Attributes.Value(wfotel.AttrOutcome)
					if counts[m.Name] == nil {
						counts[m.Name] = map[string]int64{}
					}
					counts[m.Name][o.AsString()] += p.Value
				}
			case metricdata.Histogram[float64]:
				if m.Name != wfotel.MetricStepDuration {
					continue
				}
				for _, p := range d.DataPoints {
					histCount += p.Count
				}
			}
		}
	}
	if got := counts[wfotel.MetricStepOutcomes]; got["error"] != 1 || got["permanent"] != 1 {
		t.Fatalf("step outcomes = %v", got)
	}
	if got := counts[wfotel.MetricEnqueueCount]; got["ok"] != 1 {
		t.Fatalf("enqueue count = %v", got)
	}
	if histCount != 2 {
		t.Fatalf("duration histogram count = %d, want 2", histCount)
	}
}

func TestLabelFilter(t *testing.T) {
	h := newHarness(t, wfotel.WithLabelFilter(func(k string) bool { return k == "tenant" }))
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) { return nil, nil }},
	}}
	h.run(t, wf, workflow.Job{Workflow: "w", Labels: map[string]string{"tenant": "t1", "email": "a@b.c"}})
	for _, s := range h.spans.GetSpans() {
		if _, ok := attr(s, wfotel.LabelAttributePrefix+"email"); ok {
			t.Fatalf("span %q records a filtered label", s.Name)
		}
	}
	if _, ok := attr(spanNamed(t, h.spans.GetSpans(), "workflow.step w/a")[0], wfotel.LabelAttributePrefix+"tenant"); !ok {
		t.Fatal("kept label missing")
	}
}

// Labels are closed by default: without an opt-in no span records one.
func TestLabelsOffByDefault(t *testing.T) {
	h := newHarness(t)
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) { return nil, nil }},
	}}
	h.run(t, wf, workflow.Job{Workflow: "w", Labels: map[string]string{"tenant": "t1"}})
	for _, s := range h.spans.GetSpans() {
		for _, a := range s.Attributes {
			if strings.HasPrefix(string(a.Key), wfotel.LabelAttributePrefix) {
				t.Fatalf("span %q records label %s without an opt-in", s.Name, a.Key)
			}
		}
	}
}

// A caller-supplied label under the reserved prefix is refused with
// ErrInvalid, so it can never choose the trace the steps join, and the job
// does not reach the driver.
func TestReservedLabelRefused(t *testing.T) {
	h := newHarness(t)
	forged := "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	reached := false
	enq := h.inst.Enqueue()(func(context.Context, workflow.Job) (workflow.RunRef, error) {
		reached = true
		return workflow.RunRef{}, nil
	})
	for _, k := range []string{wfotel.LabelPrefix + "traceparent", wfotel.LabelPrefix + "anything"} {
		_, err := enq(context.Background(), workflow.Job{Workflow: "w", Labels: map[string]string{k: forged}})
		if !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("label %q: err = %v, want ErrInvalid", k, err)
		}
	}
	if reached {
		t.Fatal("a job with a reserved label reached the driver")
	}
	if _, err := enq(context.Background(), workflow.Job{Workflow: "w", Labels: map[string]string{"otelx": "ok"}}); err != nil || !reached {
		t.Fatalf("an app label outside the prefix: err=%v reached=%v", err, reached)
	}
}

// With no labels and no enqueue middleware, the step span takes its parent
// from ctx and still carries the step attributes.
func TestStepWithoutPropagation(t *testing.T) {
	h := newHarness(t)
	ctx, parent := h.tp.Tracer("test").Start(context.Background(), "worker")
	ctx = workflow.WithStepInfo(ctx, workflow.StepInfo{Workflow: "w", Step: "a", RunID: "r1", Attempt: 3})
	if _, err := h.inst.Step()(func(context.Context, []byte) ([]byte, error) { return nil, nil })(ctx, nil); err != nil {
		t.Fatal(err)
	}
	parent.End()
	s := spanNamed(t, h.spans.GetSpans(), "workflow.step w/a")[0]
	if s.Parent.SpanID() != parent.SpanContext().SpanID() {
		t.Fatal("step span is not a child of the ctx span")
	}
	if v, _ := attr(s, wfotel.AttrAttempt); v.AsInt64() != 3 {
		t.Fatalf("attempt = %d", v.AsInt64())
	}
	if v, _ := attr(s, wfotel.AttrOutcome); v.AsString() != string(wfotel.OutcomeOK) {
		t.Fatalf("outcome = %q", v.AsString())
	}
}
