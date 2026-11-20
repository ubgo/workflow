package workflow_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/ubgo/workflow"
)

// eventLog is a StepEventSink that keeps every event it sees.
type eventLog struct {
	mu     sync.Mutex
	events []workflow.StepEvent
}

func (l *eventLog) RecordStepEvent(_ context.Context, e workflow.StepEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func TestRecordEventWithoutSinkIsANoOp(t *testing.T) {
	ctx := context.Background()
	if workflow.HasStepEventSink(ctx) {
		t.Fatal("HasStepEventSink on a bare ctx")
	}
	workflow.RecordEvent(ctx, "nobody listens", slog.Int("n", 1)) // must not panic
	if got := workflow.WithStepEventSink(ctx, nil); got != ctx {
		t.Fatal("WithStepEventSink(nil) changed ctx")
	}
}

func TestRecordEventFansOutInBindingOrder(t *testing.T) {
	var order []string
	first := workflow.StepEventFunc(func(context.Context, workflow.StepEvent) { order = append(order, "first") })
	second := &eventLog{}
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "w", Step: "s", RunID: "r1", Attempt: 2})
	ctx = workflow.WithStepEventSink(ctx, first)
	ctx = workflow.WithStepEventSink(ctx, workflow.StepEventFunc(func(ctx context.Context, e workflow.StepEvent) {
		order = append(order, "second")
		second.RecordStepEvent(ctx, e)
	}))
	if !workflow.HasStepEventSink(ctx) {
		t.Fatal("HasStepEventSink false with two sinks")
	}
	workflow.RecordEventLevel(ctx, slog.LevelWarn, "rows", slog.Int("n", 4000), slog.String("src", "shop"))
	if strings.Join(order, ",") != "first,second" {
		t.Fatalf("order = %v", order)
	}
	e := second.events[0]
	if e.Message != "rows" || e.Level != slog.LevelWarn || !e.At.IsZero() {
		t.Fatalf("event = %+v", e)
	}
	if e.Step.RunID != "r1" || e.Step.Step != "s" || e.Step.Attempt != 2 {
		t.Fatalf("step info = %+v", e.Step)
	}
	want := []workflow.EventAttr{{Key: "n", Value: "4000"}, {Key: "src", Value: "shop"}}
	if len(e.Attrs) != 2 || e.Attrs[0] != want[0] || e.Attrs[1] != want[1] {
		t.Fatalf("attrs = %v", e.Attrs)
	}
}

func TestRecordEventDefaultsToInfoAndBindingIsScoped(t *testing.T) {
	log := &eventLog{}
	base := context.Background()
	ctx := workflow.WithStepEventSink(base, log)
	workflow.RecordEvent(ctx, "hello")
	if log.events[0].Level != slog.LevelInfo || log.events[0].Attrs != nil {
		t.Fatalf("event = %+v", log.events[0])
	}
	// The parent ctx is unchanged by a child binding.
	if workflow.HasStepEventSink(base) {
		t.Fatal("binding leaked to the parent ctx")
	}
}

func TestRecordEventBounds(t *testing.T) {
	log := &eventLog{}
	ctx := workflow.WithStepEventSink(context.Background(), log)
	attrs := make([]slog.Attr, workflow.MaxStepEventAttrs+5)
	for i := range attrs {
		attrs[i] = slog.String("k", strings.Repeat("v", workflow.MaxStepEventValue+10))
	}
	// A multi-byte rune straddling the cut must not be split.
	msg := strings.Repeat("a", workflow.MaxStepEventMessage-1) + "é" + "tail"
	workflow.RecordEvent(ctx, msg, attrs...)
	e := log.events[0]
	if len(e.Message) > workflow.MaxStepEventMessage || !utf8.ValidString(e.Message) {
		t.Fatalf("message not cut cleanly: len %d valid %v", len(e.Message), utf8.ValidString(e.Message))
	}
	if len(e.Attrs) != workflow.MaxStepEventAttrs {
		t.Fatalf("attrs = %d, want %d", len(e.Attrs), workflow.MaxStepEventAttrs)
	}
	if len(e.Attrs[0].Value) != workflow.MaxStepEventValue {
		t.Fatalf("value len = %d", len(e.Attrs[0].Value))
	}
}

func TestRecordEventResolvesLogValuer(t *testing.T) {
	log := &eventLog{}
	ctx := workflow.WithStepEventSink(context.Background(), log)
	workflow.RecordEvent(ctx, "m", slog.Any("v", valuer{}))
	if got := log.events[0].Attrs[0].Value; got != "resolved" {
		t.Fatalf("value = %q, want resolved", got)
	}
}

// valuer is a slog.LogValuer, which RecordEvent must resolve before it
// renders the value.
type valuer struct{}

func (valuer) LogValue() slog.Value { return slog.StringValue("resolved") }
