# opentelemetry

OpenTelemetry tracing and metrics for the in-memory driver with the `otel` module. Spans go to the SDK's in-memory exporter and metrics to a manual reader, so the program prints what a trace backend would receive: the enqueue and step spans with their names, kinds and attributes, whether the step span is a child of the enqueue span or a new root linked to it (`WithParenting(ParentNewRootLink)`), span names of the app's own (`WithStepSpanName`, `WithEnqueueSpanName`), one label recorded on spans and metrics by name (`WithLabelFilter`, `WithMetricLabels`) while another is left out, the three metrics, and the refusal of a second `Enqueue` middleware in one chain.

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
```

Attributes are printed sorted and without the run ID, which changes on every run. The guide that copies this program is [OpenTelemetry](../../docs/observability/opentelemetry.md). `go test ./opentelemetry` runs it and checks every line.
