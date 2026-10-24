package workflow

import (
	"context"
	"time"
)

// StepInfo describes the step attempt a Handler is running, so middleware
// (logging, tracing, metrics) and handlers can tell runs and attempts apart
// without a driver-specific context. Every driver sets it on the ctx it
// passes to a step handler, with WithStepInfo, before the Middleware chain
// runs.
type StepInfo struct {
	// Workflow is the workflow name.
	Workflow string

	// Step is the step name.
	Step string

	// RunID is the run the step belongs to.
	RunID RunID

	// Attempt is 1 for the first attempt and grows by one per retry.
	Attempt int

	// Labels are the run's Job.Labels. Drivers pass the run's own map;
	// handlers and middleware must not modify it.
	Labels map[string]string

	// Queue is the run's queue (EffectiveQueue). Drivers without
	// CapQueue set DefaultQueue or leave it empty.
	Queue string

	// ReadyAt is when this attempt became ready to claim, on the
	// driver's clock: the run's admission for a first step, its parents'
	// end for a later one, the backoff's end for a retry. Now minus
	// ReadyAt is how long the attempt waited for a worker (the queue wait
	// a metric reports). Zero when the driver does not know it.
	ReadyAt time.Time

	// Tenant is the run's Job.Tenant; empty without one or on a driver
	// without CapTenant.
	Tenant string

	// Node is the identity of the node running this attempt (the
	// driver's node-ID option, DefaultNodeID when unset); empty on a
	// driver without CapNodeIdentity.
	Node string
}

// stepInfoKey is the unexported context key for StepInfo, so no other
// package can overwrite or read it except through the functions below.
type stepInfoKey struct{}

// WithStepInfo returns a ctx carrying info. Drivers call it before
// invoking a step handler.
func WithStepInfo(ctx context.Context, info StepInfo) context.Context {
	return context.WithValue(ctx, stepInfoKey{}, info)
}

// StepInfoFrom returns the StepInfo a driver set on ctx, and false when
// ctx is not a step's context (for example in an enqueue middleware).
func StepInfoFrom(ctx context.Context) (StepInfo, bool) {
	info, ok := ctx.Value(stepInfoKey{}).(StepInfo)
	return info, ok
}
