// Package otel adds OpenTelemetry tracing and metrics to any workflow
// driver, as one step middleware and one enqueue middleware:
//
//	inst, err := otel.New(otel.WithTracerProvider(tp), otel.WithMeterProvider(mp))
//	c := memory.New(memory.WithMiddleware(inst.Step()))
//	client := middleware.WrapClient(c, inst.Enqueue())
//
// # Spans
//
// Enqueue opens a span named "workflow.enqueue <workflow>" (kind
// Producer) around every Client.Enqueue, and Step opens one named
// "workflow.step <workflow>/<step>" (kind Consumer) around every step
// attempt. Both carry workflow.name and workflow.queue (see Metrics for
// the enqueue side); the step span
// also carries workflow.step, workflow.run_id and workflow.attempt, and
// the enqueue span workflow.run_id once the driver returns it. Job labels are not
// recorded by default, because they can carry tenant or user ids into a
// third-party trace backend; WithLabelFilter (chosen keys) or
// WithAllLabels opts in, recording them as workflow.label.<key>. Payloads
// and step outputs are never recorded: they can hold personal data, and a
// codec may not have run yet. WithStepSpanName and WithEnqueueSpanName
// replace the span names; keep a name to a bounded set of values.
//
// # Trace context propagation
//
// The enqueue middleware injects the caller's trace context into the job's
// labels and the step middleware extracts it, so a step span is a child of
// the span that enqueued its run even when the step runs on another
// machine. WithParenting changes that relation: ParentLink keeps the span
// ctx already holds (an engine SDK's) as the parent and links the enqueue
// span, as the OTel messaging conventions do; ParentNewRootLink starts each
// step span as a new trace linked to the enqueue span, so a fan-out of
// thousands of jobs does not build one trace too large to keep. The carrier prefixes every propagator key with LabelPrefix, so
// with the default W3C TraceContext propagator the labels are
//
//	otel.traceparent
//	otel.tracestate (only when the trace has state)
//
// The prefix is reserved for this package (it is the core's
// workflow.LabelPrefixOTel): Enqueue refuses a job that already sets a
// label under it with workflow.ErrInvalid, so use Enqueue once per chain. It is chosen so every driver accepts
// it: the core validates no label keys, and the Hatchet driver reserves
// only "wf__", "hatchet__" and the bare "traceparent" its SDK writes
// itself. The caller's Labels map is never modified; the middleware copies
// it before adding keys. When a step's labels carry no trace context, the
// step span's parent is whatever span ctx already holds.
//
// # Metrics
//
//   - workflow.step.duration (histogram, seconds): one per step attempt,
//     with workflow.name, workflow.step, workflow.queue and
//     workflow.outcome.
//
//   - workflow.step.outcomes (counter): one per step attempt, same
//     attributes.
//
//   - workflow.enqueue.count (counter): one per Enqueue, with
//     workflow.name, workflow.outcome and, when Job.Queue is set,
//     workflow.queue.
//
//   - workflow.queue.wait (histogram, seconds, QueueWaitBuckets): one per
//     step attempt whose StepInfo.ReadyAt the driver set: attempt start
//     minus ReadyAt, so for a run's first step it is enqueue (or RunAt) to
//     start. With workflow.name, workflow.step and workflow.queue. A
//     driver that leaves ReadyAt zero records no sample; a ReadyAt after
//     the start (clock skew between nodes) records zero.
//
//   - workflow.queue.depth (gauge): runs waiting per queue, one point per
//     queue with workflow.queue, read through ObserveQueueDepth's
//     QueueDepthFunc at every collection; admin.Admin.QueueDepths fits it.
//
//   - workflow.runs.finished (counter): one per run end delivered to the
//     observer RunFinished returns (register it with the driver's
//     OnRunFinished), with workflow.name and workflow.run.status:
//     "succeeded", "failed", "cancelled", or "discarded" on a driver with
//     CapRunStates. Delivery is at least once, so a redelivered end is
//     counted again. "retrying" never appears: it is not an end.
//
// Step spans and step metrics carry workflow.queue (StepInfo.Queue); the
// enqueue span and counter carry the queue the job asked for (Job.Queue),
// and no workflow.queue when the job left the choice to the workflow,
// whose queue only the driver resolves. workflow.outcome is
// "ok", "error" (retryable), "permanent", "cancelled" or "discarded" (the
// step returned workflow.Discard). Run ids never become metric attributes, and labels do only
// when named one by one with WithMetricLabels (recorded as
// workflow.label.<key>): each named key multiplies the series by its
// number of values, so name only labels with a small, bounded set of
// values, such as a tier or a region.
//
// Step spans also carry workflow.node (StepInfo.Node) when the driver
// names the process that ran the attempt. Job.Tenant is recorded as
// workflow.tenant on step and enqueue spans only with WithTenant, since a
// tenant ID can identify a customer, and is never a metric attribute.
//
// # Providers
//
// No global is read unless asked for: a nil or absent TracerProvider or
// MeterProvider means the otel global one (otel.GetTracerProvider,
// otel.GetMeterProvider), which is a no-op until the app installs one. The
// propagator defaults to W3C TraceContext, not the global propagator,
// whose default propagates nothing; WithPropagator replaces it.
package otel
