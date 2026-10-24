package otel_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	wfotel "github.com/ubgo/workflow/otel"
)

func collect(t *testing.T, h *harness) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

func metricNamed(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// TestQueueWait: the wait histogram records attempt start minus
// StepInfo.ReadyAt, with the queue as an attribute, and records nothing
// when the driver leaves ReadyAt zero.
func TestQueueWait(t *testing.T) {
	t0 := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	clk := clock.NewFake(t0)
	h := newHarness(t, wfotel.WithClock(clk))
	step := h.inst.Step()(func(context.Context, []byte) ([]byte, error) { return nil, nil })

	ready := workflow.StepInfo{Workflow: "w", Step: "a", Queue: "emails", Attempt: 1, ReadyAt: t0.Add(-1500 * time.Millisecond)}
	if _, err := step(workflow.WithStepInfo(context.Background(), ready), nil); err != nil {
		t.Fatal(err)
	}
	unknown := ready
	unknown.ReadyAt = time.Time{}
	if _, err := step(workflow.WithStepInfo(context.Background(), unknown), nil); err != nil {
		t.Fatal(err)
	}
	// A ReadyAt after the start (clock skew between nodes) records zero,
	// never a negative wait.
	skewed := ready
	skewed.ReadyAt = t0.Add(time.Minute)
	if _, err := step(workflow.WithStepInfo(context.Background(), skewed), nil); err != nil {
		t.Fatal(err)
	}

	m, ok := metricNamed(collect(t, h), wfotel.MetricQueueWait)
	if !ok {
		t.Fatal("no wait histogram")
	}
	pts := m.Data.(metricdata.Histogram[float64]).DataPoints
	if len(pts) != 1 || pts[0].Count != 2 || pts[0].Sum != 1.5 {
		t.Fatalf("wait points = %+v, want one series with 2 samples summing 1.5s", pts)
	}
	if q, _ := pts[0].Attributes.Value(wfotel.AttrQueue); q.AsString() != "emails" {
		t.Errorf("queue attribute = %q", q.AsString())
	}

	spans := h.spans.GetSpans()
	if v, ok := attr(spans[0], wfotel.AttrQueue); !ok || v.AsString() != "emails" {
		t.Errorf("step span queue = %v %v", v, ok)
	}
	dur, _ := metricNamed(collect(t, h), wfotel.MetricStepDuration)
	if q, _ := dur.Data.(metricdata.Histogram[float64]).DataPoints[0].Attributes.Value(wfotel.AttrQueue); q.AsString() != "emails" {
		t.Errorf("duration queue attribute = %q", q.AsString())
	}
}

// TestQueueWait_Memory: the memory driver sets ReadyAt and Queue, so a
// run records one wait sample per step attempt on its queue.
func TestQueueWait_Memory(t *testing.T) {
	h := newHarness(t)
	wf := noopWF
	wf.Queue = "emails"
	h.run(t, wf, workflow.Job{Workflow: "w"})
	m, ok := metricNamed(collect(t, h), wfotel.MetricQueueWait)
	if !ok {
		t.Fatal("no wait histogram from the memory driver")
	}
	pts := m.Data.(metricdata.Histogram[float64]).DataPoints
	if len(pts) != 1 || pts[0].Count != 1 {
		t.Fatalf("wait points = %+v", pts)
	}
	if q, _ := pts[0].Attributes.Value(wfotel.AttrQueue); q.AsString() != "emails" {
		t.Errorf("queue = %q", q.AsString())
	}
}

func TestQueueDepthGauge(t *testing.T) {
	h := newHarness(t)
	if _, err := h.inst.ObserveQueueDepth(nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil fn: %v", err)
	}
	fail := false
	reg, err := h.inst.ObserveQueueDepth(func(context.Context) (map[string]int64, error) {
		if fail {
			return nil, errors.New("db down")
		}
		return map[string]int64{"emails": 7, "default": 2}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := metricNamed(collect(t, h), wfotel.MetricQueueDepth)
	if !ok {
		t.Fatal("no depth gauge")
	}
	got := map[string]int64{}
	for _, p := range m.Data.(metricdata.Gauge[int64]).DataPoints {
		q, _ := p.Attributes.Value(wfotel.AttrQueue)
		got[q.AsString()] = p.Value
	}
	if len(got) != 2 || got["emails"] != 7 || got["default"] != 2 {
		t.Fatalf("depths = %v", got)
	}

	// A failed read reports no points and surfaces the error to the
	// collection, which the SDK hands to the OpenTelemetry error handler.
	fail = true
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("collect with a failing read: %v", err)
	}
	if m, ok := metricNamed(rm, wfotel.MetricQueueDepth); ok && len(m.Data.(metricdata.Gauge[int64]).DataPoints) != 0 {
		t.Fatalf("a failed read still reported points: %+v", m)
	}
	fail = false
	if err := reg.Unregister(); err != nil {
		t.Fatal(err)
	}
	if m, ok := metricNamed(collect(t, h), wfotel.MetricQueueDepth); ok && len(m.Data.(metricdata.Gauge[int64]).DataPoints) != 0 {
		t.Fatalf("unregistered gauge still reported: %+v", m)
	}
}

// TestEnqueueQueueAttribute: the enqueue span and counter carry the queue
// the job asked for, and none when it left it to the workflow.
func TestEnqueueQueueAttribute(t *testing.T) {
	h := newHarness(t)
	wf := noopWF
	wf.Queue = "emails"
	h.run(t, wf, workflow.Job{Workflow: "w", Queue: "emails"})
	enq := spanNamed(t, h.spans.GetSpans(), "workflow.enqueue w")[0]
	if v, _ := attr(enq, wfotel.AttrQueue); v.AsString() != "emails" {
		t.Errorf("enqueue span queue = %q", v.AsString())
	}
	m, _ := metricNamed(collect(t, h), wfotel.MetricEnqueueCount)
	pts := m.Data.(metricdata.Sum[int64]).DataPoints
	if len(pts) != 1 || !pts[0].Attributes.HasValue(wfotel.AttrQueue) {
		t.Errorf("enqueue count points = %+v", pts)
	}

	h2 := newHarness(t)
	h2.run(t, noopWF, workflow.Job{Workflow: "w"})
	if v, ok := attr(spanNamed(t, h2.spans.GetSpans(), "workflow.enqueue w")[0], wfotel.AttrQueue); ok {
		t.Errorf("enqueue span without Job.Queue has queue %q", v.AsString())
	}
	m, _ = metricNamed(collect(t, h2), wfotel.MetricEnqueueCount)
	if pts := m.Data.(metricdata.Sum[int64]).DataPoints; pts[0].Attributes.HasValue(wfotel.AttrQueue) {
		t.Errorf("enqueue count without Job.Queue has a queue: %+v", pts)
	}
}
