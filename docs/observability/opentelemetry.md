# OpenTelemetry

The `otel` module adds OpenTelemetry tracing and metrics to any driver as one step middleware ([`Instrumentation.Step`](ds:block?id=otel-instrumentation-step-3trz8uhf)) and one enqueue middleware ([`Instrumentation.Enqueue`](ds:block?id=otel-instrumentation-enqueue-9865m6xu)): a span per `Enqueue` and per step attempt, the caller's trace carried to the step even when it runs on another machine, and duration and outcome metrics. It reads no global state unless asked, records no payloads, and records labels only when you name them ([`WithLabelFilter`](ds:block?id=otel-withlabelfilter-8xrwxdpa), [`TestLabelsOffByDefault`](ds:block?id=sdktest-testlabelsoffbydefault-8x287kzb)).

## Before you start

- `go get github.com/ubgo/workflow/otel@latest`. It is its own module (Go 1.25) and requires only the OpenTelemetry API, so it never becomes a dependency of the core and never brings an SDK: your app uses the SDK and exporters it already runs.
- A `TracerProvider` and a `MeterProvider` from your app's OpenTelemetry setup. The example uses the SDK's in-memory span exporter and a manual metric reader, so it prints what a backend would receive.
- The runnable program for this page is [examples/opentelemetry](../../examples/opentelemetry/).

## Add tracing and metrics

[`otel.New(opts...)`](ds:block?id=otel-new-7u57f8wb) builds an `Instrumentation` once; share it. [`Instrumentation.Step()`](ds:block?id=otel-instrumentation-step-3trz8uhf) is the step middleware and goes first in the driver's chain, so the span covers the other middleware and sees a recovered panic as an error. [`Instrumentation.Enqueue()`](ds:block?id=otel-instrumentation-enqueue-9865m6xu) is the enqueue middleware, applied with `middleware.WrapClient`. [`otel.WithTracerProvider`](ds:block?id=otel-withtracerprovider-ueajjzzj) and [`otel.WithMeterProvider`](ds:block?id=otel-withmeterprovider-a998ufwa) say where spans and metrics go; nil or absent means the OpenTelemetry global ones ([`New`](ds:block?id=otel-new-7u57f8wb)), which are no-ops until the app installs its own.

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

The [enqueue span](ds:block?id=otel-instrumentation-enqueue-9865m6xu) is named `workflow.enqueue <workflow>`, kind Producer, with `workflow.name` ([`AttrWorkflow`](ds:block?id=otel-attrworkflow-m4cwsvz8)), and `workflow.run_id` once the driver returns it. The [step span](ds:block?id=otel-instrumentation-step-3trz8uhf) is named `workflow.step <workflow>/<step>`, kind Consumer, with `workflow.name`, `workflow.step`, `workflow.run_id`, `workflow.attempt` and `workflow.queue`, plus `workflow.node` ([`AttrNode`](ds:block?id=otel-attrnode-dv4zd9dw)) when the driver names the node that ran it. Both get `workflow.outcome` when they end, and a failed one records the error ([`recordErr`](ds:block?id=otel-recorderr-ezczmja6)); [`otel.WithTenant()`](ds:block?id=otel-withtenant-w9822jna) adds `Job.Tenant` to both as `workflow.tenant` ([`AttrTenant`](ds:block?id=otel-attrtenant-5dkn6ckw), see [Tenants](../jobs/tenants.md)). On Hatchet the step middleware goes in [`hatchet.Config.Middleware`](ds:block?id=hatchet-config-wdky3m9f), on the SQL driver in [`sqlstore.WithMiddleware`](ds:block?id=sql-withmiddleware-hkqu69ab).

## Trace context from enqueue to step

The [enqueue middleware](ds:block?id=otel-instrumentation-enqueue-9865m6xu) writes the enqueue span's trace context into a copy of the job's labels, and the [step middleware](ds:block?id=otel-instrumentation-step-3trz8uhf) reads it back from the run's labels, so the step span joins the caller's trace across processes and machines. Every key is prefixed with [`otel.LabelPrefix`](ds:block?id=otel-labelprefix-hb4wagdf) (`otel.`, through [`labelCarrier`](ds:block?id=otel-labelcarrier-dhprd6f7)), so with the default W3C TraceContext propagator the labels are `otel.traceparent` and, when the trace has state, `otel.tracestate`. The prefix is reserved for this package ([`workflow.LabelPrefixOTel`](ds:block?id=labelprefixotel-8avw4pw8)), and every driver accepts it from the enqueue middleware, which allows it on the context it passes down.

[`otel.WithPropagator(p)`](ds:block?id=otel-withpropagator-sgqm46gx) replaces the propagator. The default is W3C TraceContext ([`New`](ds:block?id=otel-new-7u57f8wb)), not the OpenTelemetry global propagator, whose default propagates nothing. When a step's labels carry no trace context, its span's parent is whatever span the context already holds ([`Instrumentation.Step`](ds:block?id=otel-instrumentation-step-3trz8uhf)). The example's third run hands it a composite propagator that carries W3C baggage as well, so a value the caller put in its context's baggage reaches the step:

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

Outside a step (an admin link, a log line), [`otel.TraceIDOf(labels)`](ds:block?id=otel-traceidof-qmje4yr5) returns the trace ID from a run's labels under the default propagator, and [`otel.SpanContextOf(labels, prop)`](ds:block?id=otel-spancontextof-xxyvu2wh) does the same with the propagator you passed to `WithPropagator`.

## Across a broker or HTTP hop

Inside the library the trace context rides in job labels. When a job leaves the library's own store (a message on SQS, SNS or Pub/Sub, a Kafka or NATS record, an HTTP request to another service), its metadata and trace context must travel as that transport's message attributes or headers. [`workflow.Attributes`](ds:block?id=attributes-7zyw3bfr) is a codec for that hop: [`Encode(job)`](ds:block?id=attributes-encode-zx9435tw) returns a flat string map and `Decode(attrs)` reads it back. It writes ([`DefaultAttributePrefix`](ds:block?id=defaultattributeprefix-9hnzab3c) `ubgo-`) `ubgo-workflow`, `ubgo-tenant`, `ubgo-queue`, `ubgo-idempotency-key`, `ubgo-priority` and `ubgo-run-at` when set, the trace labels under the W3C names other tracers read (`traceparent`, `tracestate`, `baggage`: [`DefaultTraceAttributes`](ds:block?id=defaulttraceattributes-nam377yv)), and the other labels as one `ubgo-l-<key>` attribute each, or packed into one JSON attribute `ubgo-labels` when a key is not a name the transport accepts, the count would pass its limit, or the transport folds case. `Limits` says what the transport accepts; the library ships [`SQSAttributes`](ds:block?id=sqsattributes-7ak56yuy) (at most 10, AWS name rules), [`PubSubAttributes`](ds:block?id=pubsubattributes-b9vtxyjr) (at most 100, values up to 1024 bytes, no `goog` keys), `KafkaHeaders`, `NATSHeaders` and [`HTTPHeaders`](ds:block?id=httpheaders-33x53fpr) (case-insensitive, no control characters in values). A job that does not fit is refused with `ErrInvalid`, never cut ([`Encode`](ds:block?id=attributes-encode-zx9435tw)). `Prefix`, `Trace` (label key to attribute name) and `PackLabels` change the names; an app whose consumers expect other names implements [`workflow.AttributeCodec`](ds:block?id=attributecodec-b4ta84hp) itself.

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

On the consumer side, decode, put the trace back as labels with [`TraceLabels`](ds:block?id=attributes-tracelabels-zagv8x95), and enqueue on the local client. The trace labels are reserved, so enqueue under `AllowReservedPrefix(ctx, workflow.LabelPrefixOTel)` and not through the otel enqueue middleware, which [refuses a job that carries them](ds:block?id=otel-instrumentation-enqueue-9865m6xu) and writes its own; the step span then continues the producer's trace. `Decoded.Trace` also works directly with a propagator's map carrier, for consumer code that is not a step:

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

[`workflow.SetHeader`](ds:block?id=setheader-e5daj8aj) and [`workflow.HeaderAttributes`](ds:block?id=headerattributes-zm9xyp7h) move attributes in and out of an `http.Header`. For a broker, pass the map to its client's message attributes:

<!-- snippet: examples/message-attributes/main.go#sqs -->
```go
attrs, err := workflow.Attributes{Limits: workflow.SQSAttributes}.Encode(traced)
```

## Parent or link

[`otel.WithParenting`](ds:block?id=otel-withparenting-6nfcu2q3) decides how a step span relates to the enqueue span it reads from the labels; [`New`](ds:block?id=otel-new-7u57f8wb) refuses any other value with `workflow.ErrInvalid`:

- [`otel.ParentChild`](ds:block?id=otel-parentchild-5bcxkepy) (the default): the step span is a child of the enqueue span, so the whole run is one trace with its caller.
- [`otel.ParentLink`](ds:block?id=otel-parentlink-uh7dzcaw): the step keeps whatever span the context already holds as its parent (an engine SDK's span, or none) and links the enqueue span, as the OpenTelemetry messaging conventions do for asynchronous consumers.
- [`otel.ParentNewRootLink`](ds:block?id=otel-parentnewrootlink-xr8w9g8v): every step span starts a trace of its own, linked to the enqueue span, so a fan-out of thousands of jobs does not build one trace too large for the backend to keep.

<!-- snippet: examples/opentelemetry/main.go#parenting -->
```go
parenting := wfotel.WithParenting(wfotel.ParentNewRootLink)
```

With the default the example prints `same trace: true, step parent is the enqueue span: true`; with `ParentNewRootLink` it prints `same trace: false, ... step links to it: true`.

## Span names

[`otel.WithStepSpanName(fn)`](ds:block?id=otel-withstepspanname-pykjm6rf) names step spans from the driver's `StepInfo` (zero when the context has none), and [`otel.WithEnqueueSpanName(fn)`](ds:block?id=otel-withenqueuespanname-u75brhk8) names enqueue spans from the job. An empty result falls back to the default name ([`Step`](ds:block?id=otel-instrumentation-step-3trz8uhf), [`Enqueue`](ds:block?id=otel-instrumentation-enqueue-9865m6xu)). Backends index span names, so keep a name to a bounded set of values: never a run ID, a payload field or a label with unbounded values.

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

Job labels are not recorded by default ([`TestLabelsOffByDefault`](ds:block?id=sdktest-testlabelsoffbydefault-8x287kzb)), because they can carry tenant or user IDs into a third-party trace backend. [`otel.WithLabelFilter(keep)`](ds:block?id=otel-withlabelfilter-8xrwxdpa) records the labels for which `keep(key)` reports true, and [`otel.WithAllLabels()`](ds:block?id=otel-withalllabels-82h5srqg) records every one; either way they become attributes named [`otel.LabelAttributePrefix`](ds:block?id=otel-labelattributeprefix-td9mmuw2) plus the key (`workflow.label.tier`). The package's own `otel.` labels are never recorded ([`labelAttrs`](ds:block?id=otel-instrumentation-labelattrs-4u4wxgpe)), and payloads and step outputs never are. In the example the job carries `tier` and `user` labels, and only `workflow.label.tier=pro` reaches the spans:

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

The [enqueue middleware](ds:block?id=otel-instrumentation-enqueue-9865m6xu) refuses a job that already carries a label under `otel.` with `workflow.ErrInvalid`, before any span or count ([`TestReservedLabelRefused`](ds:block?id=sdktest-testreservedlabelrefused-fuyarhwh)), rather than letting a caller choose the trace a run joins. So put `Enqueue` in a chain once:

<!-- snippet: examples/opentelemetry/main.go#twice -->
```go
twice := middleware.WrapClient(client, inst.Enqueue())
_, err = twice.Enqueue(ctx, workflow.Job{Workflow: "send-email"})
fmt.Println("Enqueue middleware twice:", errors.Is(err, workflow.ErrInvalid))
```

## Metrics

Six instruments ([`New`](ds:block?id=otel-new-7u57f8wb) creates them), under the scope [`otel.ScopeName`](ds:block?id=otel-scopename-5xfp7mp2) (`github.com/ubgo/workflow/otel`):

| Name | Constant | Kind | One per | Attributes |
|---|---|---|---|---|
| `workflow.step.duration` | [`MetricStepDuration`](ds:block?id=otel-metricstepduration-hedu2w5b) | histogram, seconds | step attempt | `workflow.name`, `workflow.step`, `workflow.queue`, `workflow.outcome` |
| `workflow.step.outcomes` | [`MetricStepOutcomes`](ds:block?id=otel-metricstepoutcomes-hxy5z37a) | counter | step attempt | `workflow.name`, `workflow.step`, `workflow.queue`, `workflow.outcome` |
| `workflow.enqueue.count` | [`MetricEnqueueCount`](ds:block?id=otel-metricenqueuecount-xg9zbms4) | counter | `Enqueue` | `workflow.name`, `workflow.outcome`, and `workflow.queue` when `Job.Queue` is set |
| `workflow.queue.wait` | [`MetricQueueWait`](ds:block?id=otel-metricqueuewait-cvnnku6c) | histogram, seconds ([`QueueWaitBuckets()`](ds:block?id=otel-queuewaitbuckets-m9t9px3s), 5ms to 1h) | step attempt the driver gave a `ReadyAt` | `workflow.name`, `workflow.step`, `workflow.queue` |
| `workflow.queue.depth` | [`MetricQueueDepth`](ds:block?id=otel-metricqueuedepth-fgtkqkfz) | gauge | queue, at each collection | `workflow.queue` |
| `workflow.runs.finished` | [`MetricRunsFinished`](ds:block?id=otel-metricrunsfinished-kencama7) | counter | run end ([Count run ends](#count-run-ends)) | `workflow.name`, `workflow.run.status` |

`workflow.outcome` ([`AttrOutcome`](ds:block?id=otel-attroutcome-dq7f9wcq)) is an [`otel.Outcome`](ds:block?id=otel-outcome-dxhyabc3): `ok`, `error` (retryable), `permanent`, `cancelled` or `discarded` (the step returned `workflow.Discard`; [`outcomeOf`](ds:block?id=otel-outcomeof-sg88bt3a) classifies). Run IDs never become metric attributes. [`otel.WithMetricLabels(keys...)`](ds:block?id=otel-withmetriclabels-4zzn4wqj) adds the named job labels to every metric as `workflow.label.<key>`, for per-tenant or per-tier SLOs; a job without the label records `""` ([`metricLabelAttrs`](ds:block?id=otel-instrumentation-metriclabelattrs-rdczpnjf)), so every series has the same keys. `New` refuses an empty key or one under `otel.` with `workflow.ErrInvalid` ([`TestMetricLabels`](ds:block?id=sdktest-testmetriclabels-guusuxq6)).

[`otel.WithClock`](ds:block?id=otel-withclock-qtagzrcf) sets the clock step durations are measured on (nil means the real clock). A test passes a fake clock and moves it inside the step, so the duration it reads is exact; the example's step advances it by 1.5 seconds:

<!-- snippet: examples/opentelemetry/main.go#clock -->
```go
clk := clock.NewFake(clockStart)
measured := wfotel.WithClock(clk) // step durations are measured on this clock
```

Read an instrument by its constant rather than a string you retype, and an outcome by [`otel.AttrOutcome`](ds:block?id=otel-attroutcome-dq7f9wcq) and an `otel.Outcome` value. This is how a test asserts on metrics with the SDK's manual reader:

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

Step metrics count attempts; [`Instrumentation.RunFinished()`](ds:block?id=otel-instrumentation-runfinished-pcpk5bes) counts runs. It returns a run-finished observer that adds one to `workflow.runs.finished` ([`MetricRunsFinished`](ds:block?id=otel-metricrunsfinished-kencama7)) per run end, with `workflow.name` and `workflow.run.status` ([`AttrRunStatus`](ds:block?id=otel-attrrunstatus-5nbeuysf): `succeeded`, `failed`, `cancelled`, or `discarded` on a driver with `CapRunStates`). Register it before `Start`:

<!-- snippet: examples/opentelemetry/main.go#run-ends -->
```go
if err := c.OnRunFinished(inst.RunFinished()); err != nil { // counts run ends by status
	return err
}
```

Delivery is at least once ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)), so a run end delivered twice counts twice: read it as a rate, not a ledger. A run waiting on its retry backoff (`retrying`) is not an end and never appears here ([`RunFinished`](ds:block?id=otel-instrumentation-runfinished-pcpk5bes)); read it from the [run listing](../jobs/run-states.md#retrying). Step attempts a step dropped with `workflow.Discard` carry the outcome `discarded` ([`OutcomeDiscarded`](ds:block?id=otel-outcomediscarded-c7rj4ujx)) on the step metrics and span.

## Queue wait and queue depth

Step spans and step metrics carry `workflow.queue` ([`otel.AttrQueue`](ds:block?id=otel-attrqueue-b5u3yxt9)), the run's queue from `StepInfo.Queue` ([`Step`](ds:block?id=otel-instrumentation-step-3trz8uhf)). The [enqueue span and counter](ds:block?id=otel-instrumentation-enqueue-9865m6xu) carry it only when the job names a queue (`Job.Queue`): the workflow's own queue is resolved by the driver, after the enqueue middleware has run, so an unnamed one records no attribute rather than a guessed one.

`workflow.queue.wait` ([`MetricQueueWait`](ds:block?id=otel-metricqueuewait-cvnnku6c)) is how long a step attempt waited for a worker: the attempt's start minus `StepInfo.ReadyAt`, the moment the driver made it claimable. For a run's first step that is enqueue to start (or `RunAt` to start for a delayed run); for a retry it starts when the backoff ends; for a later step when the steps before it finished. A driver that leaves `ReadyAt` zero records no sample, and a `ReadyAt` after the start (clock skew between nodes) records zero rather than a negative wait ([`TestQueueWait`](ds:block?id=sdktest-testqueuewait-ufty8c7b)). The buckets run from 5 ms to an hour ([`QueueWaitBuckets`](ds:block?id=otel-queuewaitbuckets-m9t9px3s)), because the SDK's default buckets are sized for milliseconds; an SDK View replaces them.

`workflow.queue.depth` ([`MetricQueueDepth`](ds:block?id=otel-metricqueuedepth-fgtkqkfz)) is a gauge of the runs waiting in each queue. The instrumentation does not count them itself: [`Instrumentation.ObserveQueueDepth(fn)`](ds:block?id=otel-instrumentation-observequeuedepth-sed5czpp) registers an [`otel.QueueDepthFunc`](ds:block?id=otel-queuedepthfunc-bczzu4r9), `func(ctx) (map[string]int64, error)`, that the SDK calls at every metric collection, with no poller of its own. [`admin.Admin.QueueDepths`](../admin/operations.md#queue-depth-and-oldest-wait) has that signature and works on any driver with `CapRunQuery`; an app that can count with one SQL query passes its own function. An error from it skips that collection's points and reaches the OpenTelemetry error handler ([`ObserveQueueDepth`](ds:block?id=otel-instrumentation-observequeuedepth-sed5czpp)).

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

- **Every metric label multiplies the series.** A key named in [`WithMetricLabels`](ds:block?id=otel-withmetriclabels-4zzn4wqj) multiplies the number of time series by its number of distinct values; name only labels with a small, bounded set (a tier, a region, a few hundred tenants at most), never a user or order ID.
- **Nothing is exported until the app installs providers.** Without `WithTracerProvider`/`WithMeterProvider`, the global providers are used ([`New`](ds:block?id=otel-new-7u57f8wb)), and they are no-ops until the app sets them.
- **`Step` must be outermost.** Put inside `Recover`, the step span ends by the panic unwinding past it and never records the recovered error ([`Step`](ds:block?id=otel-instrumentation-step-3trz8uhf) records the error only when the handler returns).
- **A label filter sees every key.** [`WithAllLabels`](ds:block?id=otel-withalllabels-82h5srqg) records whatever the app put on the job, including tenant or user IDs; prefer `WithLabelFilter` with an explicit list.
- **The error text is recorded on failed spans** ([`recordErr`](ds:block?id=otel-recorderr-ezczmja6)). Steps that put secrets in error strings leak them to the trace backend, as they already do to logs and run status.
- **Message attributes have small limits.** SQS takes 10 attributes per message ([`SQSAttributes`](ds:block?id=sqsattributes-7ak56yuy)), so a job with many labels has them packed into one; a consumer that reads attributes by name should decode with the same codec. Over a limit, [`Encode`](ds:block?id=attributes-encode-zx9435tw) refuses the job rather than dropping a label.
- **Baggage crosses only with a propagator that carries it.** The default propagator is W3C TraceContext alone ([`WithPropagator`](ds:block?id=otel-withpropagator-sgqm46gx)): baggage set before `Enqueue` is dropped unless `WithPropagator` includes `propagation.Baggage{}`, and the same propagator must be used on the enqueue and the worker side, since both read it from the one `Instrumentation`.
- **A trace label set by hand is refused** ([`TestReservedLabelRefused`](ds:block?id=sdktest-testreservedlabelrefused-fuyarhwh)). Labels under `otel.LabelPrefix` belong to the enqueue middleware; the consumer side of a broker hop restores them under `AllowReservedPrefix`, as shown above, and nothing else should write them.
- **`WithClock` should be the driver's clock.** Step durations and queue wait are read on it ([`WithClock`](ds:block?id=otel-withclock-qtagzrcf); span start and end times come from the tracer provider), and queue wait is the step's start on that clock minus the driver's `ReadyAt` ([`Step`](ds:block?id=otel-instrumentation-step-3trz8uhf)). A clock that is not the driver's, as in the example's third run, records a queue wait of zero, because a negative wait is clamped.
- **Parenting depends on the context the driver passes.** [`ParentLink`](ds:block?id=otel-parentlink-uh7dzcaw) keeps the span the driver's context already holds ([`TestParentLink_KeepsCtxSpan`](ds:block?id=sdktest-testparentlink-keepsctxspan-4rsc6p34)); on a driver that starts no span of its own, the step span has no parent and only the link.

## Related

- [Step middleware](../middleware/step-middleware.md) and [enqueue middleware](../middleware/enqueue-middleware.md): where the two halves sit.
- [Logging](logging.md): log lines per attempt.
- [Admin operations](../admin/operations.md) and [queue alerts](../admin/alerts.md): the depth source and threshold alerts.
- [Reserved names](../reference/names.md#metrics): metric names and reserved labels.
- Examples: [examples/opentelemetry](../../examples/opentelemetry/), [examples/message-attributes](../../examples/message-attributes/).
