package workflow

import (
	"context"
	"log/slog"
	"time"
)

// Bounds of a step event, so a step that records events in a loop cannot
// grow what a sink stores without limit. RecordEvent cuts a longer message
// or value and keeps at most MaxStepEventAttrs attributes.
const (
	// MaxStepEventMessage is the longest StepEvent.Message, in bytes.
	//
	//ds:def id=maxstepeventmessage-yfsr3k3h owner=@khanakia stability=stable
	MaxStepEventMessage = 1 << 10

	// MaxStepEventAttrs is the most attributes one StepEvent keeps.
	//
	//ds:def id=maxstepeventattrs-das9dx82 owner=@khanakia stability=stable
	MaxStepEventAttrs = 32

	// MaxStepEventValue is the longest attribute value, in bytes.
	//
	//ds:def id=maxstepeventvalue-ukkmnhkc owner=@khanakia stability=stable
	MaxStepEventValue = 1 << 10
)

// StepEvent is one line of a step's timeline ("downloaded 12 MB, 4,000
// rows"), recorded from inside a step with RecordEvent and handed to every
// StepEventSink bound on the step's ctx: a run recorder stores it with the
// step, a tracer adds it as a span event, a log capture writes it.
//
//ds:def id=stepevent-f7gyzzpj owner=@khanakia stability=stable
type StepEvent struct {
	// Message is a short note for a person.
	Message string

	// Attrs are the event's key/value pairs, values already rendered as
	// strings so every sink stores the same text. Order is the caller's.
	Attrs []EventAttr

	// Level is the slog level the caller gave (slog.LevelInfo by default
	// with RecordEvent, any level with RecordEventLevel).
	Level slog.Level

	// At is when it was recorded. RecordEvent leaves it zero, because the
	// core reads no wall clock on a step's path (a fake clock in a test
	// must see the same time a sink stores); each sink stamps it from its
	// own clock when zero.
	At time.Time

	// Step is the StepInfo of the attempt that recorded it; zero outside
	// a driver-run step.
	Step StepInfo
}

// EventAttr is one key/value pair of a StepEvent.
//
//ds:def id=eventattr-ruqhyudh owner=@khanakia stability=stable
type EventAttr struct {
	Key   string
	Value string
}

// StepEventSink receives the events a step records. A sink must be safe
// for concurrent use (steps of one run may run in parallel) and must not
// block: RecordEvent calls it on the step's goroutine.
//
//ds:def id=stepeventsink-f4enzkvf owner=@khanakia stability=stable
type StepEventSink interface {
	RecordStepEvent(ctx context.Context, e StepEvent)
}

// StepEventFunc adapts a function to StepEventSink.
//
//ds:def id=stepeventfunc-473b6uwj owner=@khanakia stability=stable
type StepEventFunc func(ctx context.Context, e StepEvent)

// RecordStepEvent calls f.
func (f StepEventFunc) RecordStepEvent(ctx context.Context, e StepEvent) { f(ctx, e) }

type stepEventKey struct{}

// WithStepEventSink returns a ctx carrying s in addition to the sinks ctx
// already carries, so a recorder, a tracer and a log capture each bind
// their own and every one sees every event, in binding order. Plugins and
// middleware call it on the step ctx before the handler runs. A nil s
// returns ctx unchanged.
//
//ds:def id=withstepeventsink-ehcpt8xf owner=@khanakia stability=stable
func WithStepEventSink(ctx context.Context, s StepEventSink) context.Context {
	if s == nil {
		return ctx
	}
	old, _ := ctx.Value(stepEventKey{}).([]StepEventSink)
	sinks := make([]StepEventSink, 0, len(old)+1)
	sinks = append(sinks, old...)
	sinks = append(sinks, s)
	return context.WithValue(ctx, stepEventKey{}, sinks)
}

// HasStepEventSink reports whether any sink is bound on ctx, so a step can
// skip building an expensive message nobody will read.
//
//ds:def id=hasstepeventsink-95trpj6t owner=@khanakia stability=stable
func HasStepEventSink(ctx context.Context) bool {
	sinks, _ := ctx.Value(stepEventKey{}).([]StepEventSink)
	return len(sinks) > 0
}

// RecordEvent records msg with attrs on the current step's timeline, at
// slog.LevelInfo. With no sink bound it does nothing: an event is a log
// line, not data the caller asked to keep (contrast ReportProgress, which
// errors), so a step records events the same way with or without a
// recorder. Values are rendered with slog.Value.String after Resolve.
//
//ds:def id=recordevent-k3aqhnwy owner=@khanakia stability=stable
func RecordEvent(ctx context.Context, msg string, attrs ...slog.Attr) {
	RecordEventLevel(ctx, slog.LevelInfo, msg, attrs...)
}

// RecordEventLevel is RecordEvent at a chosen level (a warning in a
// timeline that a sink may colour or alert on).
//
//ds:def id=recordeventlevel-b9kyju7j owner=@khanakia stability=stable
func RecordEventLevel(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	sinks, _ := ctx.Value(stepEventKey{}).([]StepEventSink)
	if len(sinks) == 0 {
		return
	}
	e := StepEvent{Message: cutString(msg, MaxStepEventMessage), Level: level}
	e.Step, _ = StepInfoFrom(ctx)
	n := min(len(attrs), MaxStepEventAttrs)
	if n > 0 {
		e.Attrs = make([]EventAttr, 0, n)
		for _, a := range attrs[:n] {
			v := a.Value.Resolve().String()
			e.Attrs = append(e.Attrs, EventAttr{Key: cutString(a.Key, MaxStepEventValue), Value: cutString(v, MaxStepEventValue)})
		}
	}
	for _, s := range sinks {
		s.RecordStepEvent(ctx, e)
	}
}

// cutString returns s cut to at most n bytes without splitting a UTF-8
// sequence, so a stored event is always valid text.
func cutString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	i := n
	// Step back over continuation bytes (10xxxxxx) to a rune start.
	for i > 0 && s[i]&0xC0 == 0x80 {
		i--
	}
	return s[:i]
}
