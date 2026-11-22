package otel

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TraceIDOf returns the trace ID (32 lowercase hex digits) the enqueue
// middleware carried in a run's labels, so an app can link a run to its
// trace: an admin trace link (admin.WithTraceLink), a log line, a support
// ticket. It reads the W3C traceparent the default propagator writes
// under LabelPrefix; it returns false when the labels hold none, or one
// that is malformed or all zeros. With a different propagator
// (WithPropagator), use SpanContextOf with that propagator.
//
//ds:def id=otel-traceidof-qmje4yr5 owner=@khanakia stability=stable
func TraceIDOf(labels map[string]string) (string, bool) {
	sc := SpanContextOf(labels, propagation.TraceContext{})
	if !sc.TraceID().IsValid() {
		return "", false
	}
	return sc.TraceID().String(), true
}

// SpanContextOf extracts the enqueue span's context from a run's labels
// with prop (the propagator given to WithPropagator; nil means W3C trace
// context). The result is invalid (IsValid false) when the labels carry
// none.
//
//ds:def id=otel-spancontextof-xxyvu2wh owner=@khanakia stability=stable
func SpanContextOf(labels map[string]string, prop propagation.TextMapPropagator) trace.SpanContext {
	if len(labels) == 0 {
		return trace.SpanContext{}
	}
	if prop == nil {
		prop = propagation.TraceContext{}
	}
	return trace.SpanContextFromContext(prop.Extract(context.Background(), labelCarrier{labels: labels}))
}
