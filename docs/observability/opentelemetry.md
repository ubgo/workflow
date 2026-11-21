# OpenTelemetry

The `otel` module adds OpenTelemetry tracing and metrics to any driver as one step middleware and one enqueue middleware: a span per `Enqueue` and per step attempt, the caller's trace carried to the step even when it runs on another machine, and duration and outcome metrics. It reads no global state unless asked, records no payloads, and records labels only when you name them.

## Before you start

- `go get github.com/ubgo/workflow/otel@latest`. It is its own module (Go 1.25) and requires only the OpenTelemetry API, so it never becomes a dependency of the core and never brings an SDK: your app uses the SDK and exporters it already runs.
- A `TracerProvider` and a `MeterProvider` from your app's OpenTelemetry setup. The example uses the SDK's in-memory span exporter and a manual metric reader, so it prints what a backend would receive.
- The runnable program for this page is [examples/opentelemetry](../../examples/opentelemetry/).

## Add tracing and metrics

`otel.New(opts...)` builds an `Instrumentation` once; share it. `Instrumentation.Step()` is the step middleware and goes first in the driver's chain, so the span covers the other middleware and sees a recovered panic as an error. `Instrumentation.Enqueue()` is the enqueue middleware, applied with `middleware.WrapClient`. `otel.WithTracerProvider` and `otel.WithMeterProvider` say where spans and metrics go; nil or absent means the OpenTelemetry global ones, which are no-ops until the app installs its own.

<!-- snippet: examples/opentelemetry/main.go#providers -->
```go
spans := tracetest.NewInMemoryExporter()
tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
reader := sdkmetric.NewManualReader()
mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
```

<!-- snippet: examples/opentelemetry/main.go#setup -->
```go
inst, err := wfotel.New(append([]wfotel.Option{
	wfotel.WithTracerProvider(tp),
	wfotel.WithMeterProvider(mp),
	wfotel.WithLabelFilter(func(key string) bool { return key == "tier" }), // span attribute workflow.label.tier
	wfotel.WithMetricLabels("tier"),                                        // a label with few values only
}, d.opts...)...)
if err != nil {
	return err
}
c := memory.New(memory.WithMiddleware(inst.Step())) // first: the span covers the other middleware
client := middleware.WrapClient(c, inst.Enqueue())
```

The enqueue span is named `workflow.enqueue <workflow>`, kind Producer, with `workflow.name`, and `workflow.run_id` once the driver returns it. The step span is named `workflow.step <workflow>/<step>`, kind Consumer, with `workflow.name`, `workflow.step`, `workflow.run_id` and `workflow.attempt`. Both get `workflow.outcome` when they end, and a failed one records the error. On Hatchet the step middleware goes in `hatchet.Config.Middleware`, on the SQL driver in `sqlstore.WithMiddleware`.

## Trace context from enqueue to step

The enqueue middleware writes the enqueue span's trace context into a copy of the job's labels, and the step middleware reads it back from the run's labels, so the step span joins the caller's trace across processes and machines. Every key is prefixed with `otel.LabelPrefix` (`otel.`), so with the default W3C TraceContext propagator the labels are `otel.traceparent` and, when the trace has state, `otel.tracestate`. The prefix is reserved for this package and every driver accepts it.

`otel.WithPropagator(p)` replaces the propagator. The default is W3C TraceContext, not the OpenTelemetry global propagator, whose default propagates nothing. When a step's labels carry no trace context, its span's parent is whatever span the context already holds. The example's third run hands it a composite propagator that carries W3C baggage as well, so a value the caller put in its context's baggage reaches the step:

<!-- snippet: examples/opentelemetry/main.go#propagation -->
```go
// W3C trace context plus baggage, so a value the caller put in the
// baggage reaches the step on another machine.
prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
withBaggage := wfotel.WithPropagator(prop)
```

Inside a step, the run's labels show what the enqueue middleware wrote, and the context holds what the step middleware restored. A step never needs to read these labels itself; this is how to look when a trace does not join up:

<!-- snippet: examples/opentelemetry/main.go#trace-labels -->
```go
// traceLabels reads, inside a step, the run's labels under otel.LabelPrefix
// (the trace context the enqueue middleware wrote) and the baggage the
// step middleware restored from them.
func traceLabels(ctx context.Context) string {
	info, _ := workflow.StepInfoFrom(ctx)
	var keys []string
	for k := range info.Labels {
		if strings.HasPrefix(k, wfotel.LabelPrefix) { // "otel.": reserved for the trace context
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	plan := baggage.FromContext(ctx).Member("plan").Value()
	return fmt.Sprintf("trace labels on the run: %s; baggage in the step: plan=%s", strings.Join(keys, " "), plan)
}
```

```text
trace labels on the run: otel.baggage otel.traceparent; baggage in the step: plan=pro
```

Outside a step (an admin link, a log line), `otel.TraceIDOf(labels)` returns the trace ID from a run's labels under the default propagator, and `otel.SpanContextOf(labels, prop)` does the same with the propagator you passed to `WithPropagator`.

## Across a broker or HTTP hop

Inside the library the trace context rides in job labels. When a job leaves the library's own store (a message on SQS, SNS or Pub/Sub, a Kafka or NATS record, an HTTP request to another service), its metadata and trace context must travel as that transport's message attributes or headers. `workflow.Attributes` is a codec for that hop: `Encode(job)` returns a flat string map and `Decode(attrs)` reads it back. It writes `ubgo-workflow`, `ubgo-tenant`, `ubgo-queue`, `ubgo-idempotency-key`, `ubgo-priority` and `ubgo-run-at` when set, the trace labels under the W3C names other tracers read (`traceparent`, `tracestate`, `baggage`), and the other labels as one `ubgo-l-<key>` attribute each, or packed into one JSON attribute `ubgo-labels` when a key is not a name the transport accepts, the count would pass its limit, or the transport folds case. `Limits` says what the transport accepts; the library ships `SQSAttributes` (at most 10, AWS name rules), `PubSubAttributes` (at most 100, values up to 1024 bytes, no `goog` keys), `KafkaHeaders`, `NATSHeaders` and `HTTPHeaders` (case-insensitive, no control characters in values). A job that does not fit is refused with `ErrInvalid`, never cut. `Prefix`, `Trace` (label key to attribute name) and `PackLabels` change the names; an app whose consumers expect other names implements `workflow.AttributeCodec` itself.

On the producer side, run the otel enqueue middleware in front of the function that sends, so the trace labels are on the job when it is encoded:

<!-- snippet: examples/message-attributes/main.go#produce -->
```go
codec := workflow.Attributes{Limits: workflow.HTTPHeaders}
send := func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	attrs, err := codec.Encode(job) // refuses what the transport cannot carry
	if err != nil {
		return workflow.RunRef{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, bytes.NewReader(job.Payload))
	if err != nil {
		return workflow.RunRef{}, err
	}
	workflow.SetHeader(req.Header, attrs)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return workflow.RunRef{}, err
	}
	resp.Body.Close()
	return workflow.RunRef{}, nil
}
producer := inst.Enqueue()(send)
```

On the consumer side, decode, put the trace back as labels with `TraceLabels`, and enqueue on the local client. The trace labels are reserved, so enqueue under `AllowReservedPrefix(ctx, workflow.LabelPrefixOTel)` and not through the otel enqueue middleware, which writes its own; the step span then continues the producer's trace. `Decoded.Trace` also works directly with a propagator's map carrier, for consumer code that is not a step:

<!-- snippet: examples/message-attributes/main.go#consume -->
```go
codec := workflow.Attributes{Limits: workflow.HTTPHeaders}
d, err := codec.Decode(workflow.HeaderAttributes(r.Header))
if err != nil {
	http.Error(w, "bad attributes", http.StatusBadRequest)
	return
}
job := d.Job
job.Payload = body
job.Labels = map[string]string{}
maps.Copy(job.Labels, d.Job.Labels)
maps.Copy(job.Labels, codec.TraceLabels(d)) // the steps continue the sender's trace
ref, err := consumer.Enqueue(workflow.AllowReservedPrefix(r.Context(), workflow.LabelPrefixOTel), job)
```

`workflow.SetHeader` and `workflow.HeaderAttributes` move attributes in and out of an `http.Header`. For a broker, pass the map to its client's message attributes:

<!-- snippet: examples/message-attributes/main.go#sqs -->
```go
attrs, err := workflow.Attributes{Limits: workflow.SQSAttributes}.Encode(traced)
```

## Parent or link

`otel.WithParenting` decides how a step span relates to the enqueue span it reads from the labels; `New` refuses any other value with `workflow.ErrInvalid`:

- `otel.ParentChild` (the default): the step span is a child of the enqueue span, so the whole run is one trace with its caller.
- `otel.ParentLink`: the step keeps whatever span the context already holds as its parent (an engine SDK's span, or none) and links the enqueue span, as the OpenTelemetry messaging conventions do for asynchronous consumers.
- `otel.ParentNewRootLink`: every step span starts a trace of its own, linked to the enqueue span, so a fan-out of thousands of jobs does not build one trace too large for the backend to keep.

<!-- snippet: examples/opentelemetry/main.go#parenting -->
```go
parenting := wfotel.WithParenting(wfotel.ParentNewRootLink)
```

With the default the example prints `same trace: true, step parent is the enqueue span: true`; with `ParentNewRootLink` it prints `same trace: false, ... step links to it: true`.

## Span names

`otel.WithStepSpanName(fn)` names step spans from the driver's `StepInfo` (zero when the context has none), and `otel.WithEnqueueSpanName(fn)` names enqueue spans from the job. An empty result falls back to the default name. Backends index span names, so keep a name to a bounded set of values: never a run ID, a payload field or a label with unbounded values.

<!-- snippet: examples/opentelemetry/main.go#span-names -->
```go
names := []wfotel.Option{
	// A bounded set of names: the step name, never a run ID.
	wfotel.WithStepSpanName(func(info workflow.StepInfo) string { return "step " + info.Step }),
	wfotel.WithEnqueueSpanName(func(job workflow.Job) string { return "enqueue " + job.Workflow }),
}
```

The example's second run passes these with `ParentNewRootLink`, and its spans are named `enqueue send-email` and `step send` instead of `workflow.enqueue send-email` and `workflow.step send-email/send` ([Try it](#try-it)).

## Job labels on spans

Job labels are not recorded by default, because they can carry tenant or user IDs into a third-party trace backend. `otel.WithLabelFilter(keep)` records the labels for which `keep(key)` reports true, and `otel.WithAllLabels()` records every one; either way they become attributes named `otel.LabelAttributePrefix` plus the key (`workflow.label.tier`). The package's own `otel.` labels are never recorded, and payloads and step outputs never are. In the example the job carries `tier` and `user` labels, and only `workflow.label.tier=pro` reaches the spans:

<!-- snippet: examples/opentelemetry/main.go#labels -->
```go
ref, err := client.Enqueue(ctx, workflow.Job{Workflow: "send-email", Labels: map[string]string{"tier": "pro", "user": "u-42"}})
if err != nil {
	return err
}
```

`WithAllLabels` records every label instead; the example's third run uses it, and its spans carry `workflow.label.user=u-42` as well. Use it only where no label holds personal data or secrets:

<!-- snippet: examples/opentelemetry/main.go#all-labels -->
```go
// Every label but the otel. ones becomes a span attribute: only where
// no label holds personal data. WithLabelFilter picks keys instead.
allLabels := wfotel.WithAllLabels()
fmt.Println("span attribute of the user label:", wfotel.LabelAttributePrefix+"user")
```

The enqueue middleware refuses a job that already carries a label under `otel.` with `workflow.ErrInvalid`, before any span or count, rather than letting a caller choose the trace a run joins. So put `Enqueue` in a chain once:

<!-- snippet: examples/opentelemetry/main.go#twice -->
```go
twice := middleware.WrapClient(client, inst.Enqueue())
_, err = twice.Enqueue(ctx, workflow.Job{Workflow: "send-email"})
fmt.Println("Enqueue middleware twice:", errors.Is(err, workflow.ErrInvalid))
```

## Metrics

Five instruments, under the scope `otel.ScopeName` (`github.com/ubgo/workflow/otel`):

| Name | Constant | Kind | One per | Attributes |
|---|---|---|---|---|
| `workflow.step.duration` | `MetricStepDuration` | histogram, seconds | step attempt | `workflow.name`, `workflow.step`, `workflow.queue`, `workflow.outcome` |
| `workflow.step.outcomes` | `MetricStepOutcomes` | counter | step attempt | `workflow.name`, `workflow.step`, `workflow.queue`, `workflow.outcome` |
| `workflow.enqueue.count` | `MetricEnqueueCount` | counter | `Enqueue` | `workflow.name`, `workflow.outcome`, and `workflow.queue` when `Job.Queue` is set |
| `workflow.queue.wait` | `MetricQueueWait` | histogram, seconds (`QueueWaitBuckets()`, 5ms to 1h) | step attempt the driver gave a `ReadyAt` | `workflow.name`, `workflow.step`, `workflow.queue` |
| `workflow.queue.depth` | `MetricQueueDepth` | gauge | queue, at each collection | `workflow.queue` |

`workflow.outcome` is an `otel.Outcome`: `ok`, `error` (retryable), `permanent`, `cancelled` or `discarded` (the step returned `workflow.Discard`). Run IDs never become metric attributes. `otel.WithMetricLabels(keys...)` adds the named job labels to every metric as `workflow.label.<key>`, for per-tenant or per-tier SLOs; a job without the label records `""`, so every series has the same keys. `New` refuses an empty key or one under `otel.` with `workflow.ErrInvalid`.

`otel.WithClock` sets the clock step durations are measured on (nil means the real clock). A test passes a fake clock and moves it inside the step, so the duration it reads is exact; the example's step advances it by 1.5 seconds:

<!-- snippet: examples/opentelemetry/main.go#clock -->
```go
clk := clock.NewFake(clockStart)
measured := wfotel.WithClock(clk) // step durations are measured on this clock
```

Read an instrument by its constant rather than a string you retype, and an outcome by `otel.AttrOutcome` and an `otel.Outcome` value. This is how a test asserts on metrics with the SDK's manual reader:

<!-- snippet: examples/opentelemetry/main.go#read-metrics -->
```go
// stepMetrics reads two instruments by their names: the attempts that
// ended with outcome ok, and the seconds the step histogram summed.
func stepMetrics(rm metricdata.ResourceMetrics) (ok int64, seconds float64) {
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != wfotel.ScopeName {
			continue
		}
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				if m.Name != wfotel.MetricStepOutcomes {
					continue
				}
				for _, p := range data.DataPoints {
					if v, _ := p.Attributes.Value(wfotel.AttrOutcome); v.AsString() == string(wfotel.OutcomeOK) {
						ok += p.Value
					}
				}
			case metricdata.Histogram[float64]:
				if m.Name != wfotel.MetricStepDuration {
					continue
				}
				for _, p := range data.DataPoints {
					seconds += p.Sum
				}
			}
		}
	}
	return ok, seconds
}
```

```text
step attempts with outcome ok: 1, step time: 1.5s
```

## Count run ends

Step metrics count attempts; `Instrumentation.RunFinished()` counts runs. It returns a run-finished observer that adds one to `workflow.runs.finished` per run end, with `workflow.name` and `workflow.run.status` (`succeeded`, `failed`, `cancelled`, or `discarded` on a driver with `CapRunStates`). Register it before `Start`:

<!-- snippet: examples/opentelemetry/main.go#run-ends -->
```go
if err := c.OnRunFinished(inst.RunFinished()); err != nil { // counts run ends by status
	return err
}
```

Delivery is at least once, so a run end delivered twice counts twice: read it as a rate, not a ledger. A run waiting on its retry backoff (`retrying`) is not an end and never appears here; read it from the [run listing](../jobs/run-states.md#retrying). Step attempts a step dropped with `workflow.Discard` carry the outcome `discarded` on the step metrics and span.

## Queue wait and queue depth

Step spans and step metrics carry `workflow.queue` (`otel.AttrQueue`), the run's queue from `StepInfo.Queue`. The enqueue span and counter carry it only when the job names a queue (`Job.Queue`): the workflow's own queue is resolved by the driver, after the enqueue middleware has run, so an unnamed one records no attribute rather than a guessed one.

`workflow.queue.wait` is how long a step attempt waited for a worker: the attempt's start minus `StepInfo.ReadyAt`, the moment the driver made it claimable. For a run's first step that is enqueue to start (or `RunAt` to start for a delayed run); for a retry it starts when the backoff ends; for a later step when the steps before it finished. A driver that leaves `ReadyAt` zero records no sample, and a `ReadyAt` after the start (clock skew between nodes) records zero rather than a negative wait. The buckets run from 5 ms to an hour, because the SDK's default buckets are sized for milliseconds; an SDK View replaces them.

`workflow.queue.depth` is a gauge of the runs waiting in each queue. The instrumentation does not count them itself: `Instrumentation.ObserveQueueDepth(fn)` registers an `otel.QueueDepthFunc`, `func(ctx) (map[string]int64, error)`, that the SDK calls at every metric collection, with no poller of its own. [`admin.Admin.QueueDepths`](../admin/operations.md#queue-depth-and-oldest-wait) has that signature and works on any driver with `CapRunQuery`; an app that can count with one SQL query passes its own function. An error from it skips that collection's points and reaches the OpenTelemetry error handler.

<!-- snippet: examples/admin/main.go#gauge -->
```go
reader := sdkmetric.NewManualReader()
mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
inst, err := wfotel.New(wfotel.WithMeterProvider(mp))
if err != nil {
	return err
}
reg, err := inst.ObserveQueueDepth(adm.QueueDepths) // read at every collection
if err != nil {
	return err
}
defer func() { err = errors.Join(err, reg.Unregister()) }()
```

Unregister the returned registration when the client stops. Alerts on a depth or wait threshold are the [admin Monitor](../admin/alerts.md), which needs no metrics backend.

## Try it

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

The example prints span attributes sorted and without the run ID, which changes on every run, and prints the step time only on the third run, whose clock is fake. `go test ./opentelemetry` checks every line.

## Pitfalls

- **Every metric label multiplies the series.** A key named in `WithMetricLabels` multiplies the number of time series by its number of distinct values; name only labels with a small, bounded set (a tier, a region, a few hundred tenants at most), never a user or order ID.
- **Nothing is exported until the app installs providers.** Without `WithTracerProvider`/`WithMeterProvider`, the global providers are used, and they are no-ops until the app sets them.
- **`Step` must be outermost.** Put inside `Recover`, the step span ends by the panic unwinding past it and never records the recovered error.
- **A label filter sees every key.** `WithAllLabels` records whatever the app put on the job, including tenant or user IDs; prefer `WithLabelFilter` with an explicit list.
- **The error text is recorded on failed spans.** Steps that put secrets in error strings leak them to the trace backend, as they already do to logs and run status.
- **Message attributes have small limits.** SQS takes 10 attributes per message, so a job with many labels has them packed into one; a consumer that reads attributes by name should decode with the same codec. Over a limit, `Encode` refuses the job rather than dropping a label.
- **Baggage crosses only with a propagator that carries it.** The default propagator is W3C TraceContext alone: baggage set before `Enqueue` is dropped unless `WithPropagator` includes `propagation.Baggage{}`, and the same propagator must be used on the enqueue and the worker side, since both read it from the one `Instrumentation`.
- **A trace label set by hand is refused.** Labels under `otel.LabelPrefix` belong to the enqueue middleware; the consumer side of a broker hop restores them under `AllowReservedPrefix`, as shown above, and nothing else should write them.
- **`WithClock` should be the driver's clock.** Step durations and queue wait are read on it (span start and end times come from the tracer provider), and queue wait is the step's start on that clock minus the driver's `ReadyAt`. A clock that is not the driver's, as in the example's third run, records a queue wait of zero, because a negative wait is clamped.
- **Parenting depends on the context the driver passes.** `ParentLink` keeps the span the driver's context already holds; on a driver that starts no span of its own, the step span has no parent and only the link.

## Related

- [Step middleware](../middleware/step-middleware.md) and [enqueue middleware](../middleware/enqueue-middleware.md): where the two halves sit.
- [Logging](logging.md): log lines per attempt.
- [Admin operations](../admin/operations.md) and [queue alerts](../admin/alerts.md): the depth source and threshold alerts.
- [Reserved names](../reference/names.md#metrics): metric names and reserved labels.
- Examples: [examples/opentelemetry](../../examples/opentelemetry/), [examples/message-attributes](../../examples/message-attributes/).
