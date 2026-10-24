package otel

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
)

// InjectForTest writes ctx's trace context into labels the way Enqueue
// does, with the default propagator.
func InjectForTest(ctx context.Context, labels map[string]string) {
	propagation.TraceContext{}.Inject(ctx, labelCarrier{labels: labels})
}
