# opentelemetry

OpenTelemetry tracing and metrics for the in-memory driver with the `otel` module. Spans go to the SDK's in-memory exporter and metrics to a manual reader, so the program prints what a trace backend would receive: the enqueue and step spans with their names, kinds and attributes, whether the step span is a child of the enqueue span or a new root linked to it (`WithParenting(ParentNewRootLink)`), span names of the app's own (`WithStepSpanName`, `WithEnqueueSpanName`), one label recorded on spans and metrics by name (`WithLabelFilter`, `WithMetricLabels`) while another is left out, the metrics, and the refusal of a second `Enqueue` middleware in one chain. A third run records every label on spans (`WithAllLabels`), carries W3C baggage beside the trace context with a propagator of its own (`WithPropagator`), shows the `otel.` labels a step finds on its run, and reads the step metrics by their constant names with durations measured on a fake clock (`WithClock`).

```sh
cd examples
go run ./opentelemetry
```

```text
-- default: the step span is a child of the enqueue span
span "workflow.enqueue send-email" kind=producer workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok
span "workflow.step send-email/send" kind=consumer workflow.attempt=1 workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok workflow.queue=default workflow.step=send
same trace: true, step parent is the enqueue span: true, step links to it: false
run ID on the enqueue span: true
metric workflow.step.duration workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok workflow.queue=default workflow.step=send
metric workflow.step.outcomes workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok workflow.queue=default workflow.step=send
metric workflow.enqueue.count workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok
metric workflow.queue.wait workflow.label.tier=pro workflow.name=send-email workflow.queue=default workflow.step=send
metric workflow.runs.finished workflow.name=send-email workflow.run.status=succeeded
Enqueue middleware twice: true
-- ParentNewRootLink and span names of our own
span "enqueue send-email" kind=producer workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok
span "step send" kind=consumer workflow.attempt=1 workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok workflow.queue=default workflow.step=send
same trace: false, step parent is the enqueue span: false, step links to it: true
run ID on the enqueue span: true
metric workflow.step.duration workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok workflow.queue=default workflow.step=send
metric workflow.step.outcomes workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok workflow.queue=default workflow.step=send
metric workflow.enqueue.count workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok
metric workflow.queue.wait workflow.label.tier=pro workflow.name=send-email workflow.queue=default workflow.step=send
metric workflow.runs.finished workflow.name=send-email workflow.run.status=succeeded
Enqueue middleware twice: true
-- every label, W3C baggage in the trace labels, durations on a fake clock
span attribute of the user label: workflow.label.user
span "workflow.enqueue send-email" kind=producer workflow.label.tier=pro workflow.label.user=u-42 workflow.name=send-email workflow.outcome=ok
span "workflow.step send-email/send" kind=consumer workflow.attempt=1 workflow.label.tier=pro workflow.label.user=u-42 workflow.name=send-email workflow.outcome=ok workflow.queue=default workflow.step=send
same trace: true, step parent is the enqueue span: true, step links to it: false
run ID on the enqueue span: true
metric workflow.step.duration workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok workflow.queue=default workflow.step=send
metric workflow.step.outcomes workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok workflow.queue=default workflow.step=send
metric workflow.enqueue.count workflow.label.tier=pro workflow.name=send-email workflow.outcome=ok
metric workflow.queue.wait workflow.label.tier=pro workflow.name=send-email workflow.queue=default workflow.step=send
metric workflow.runs.finished workflow.name=send-email workflow.run.status=succeeded
trace labels on the run: otel.baggage otel.traceparent; baggage in the step: plan=pro
step attempts with outcome ok: 1, step time: 1.5s
Enqueue middleware twice: true
```

Attributes are printed sorted and without the run ID, which changes on every run; the step time is printed only on the third run, whose clock is fake. The guide that copies this program is [OpenTelemetry](../../docs/observability/opentelemetry.md). `go test ./opentelemetry` runs it and checks every line.
