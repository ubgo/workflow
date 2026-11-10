# Labels

`Job.Labels` is a `map[string]string` of app metadata that travels with a run: a tenant, a source, a request ID. It reaches every step through `StepInfo.Labels` and every run-finished observer through `RunEvent.Labels`, so middleware can require one or log it without trusting a context convention. A few prefixes are kept for the library's own packages, which write labels onto the jobs they pass down; this page says which, and how to keep app labels out of them.

## Before you start

- You have enqueued a job with labels ([Your first workflow](../start/first-workflow.md#step-4--enqueue-and-read-the-status)).

## Reserved prefixes

| Prefix | Constant | Owned by | What it carries |
|---|---|---|---|
| `ubgo.workflow/` | [`workflow.LabelPrefixInternal`](ds:block?id=labelprefixinternal-mvjqh2yy) | `middleware` | Unique locks (`middleware.UniqueKeyLabel`, `UniqueOwnerLabel`) and the claim-check token (`middleware.ClaimCheckLabel`) |
| `workflow.batch.` | [`workflow.LabelPrefixBatch`](ds:block?id=labelprefixbatch-vq6yb5d4) | `batch` | A job's batch ID and its index in the batch |
| `otel.` | [`workflow.LabelPrefixOTel`](ds:block?id=labelprefixotel-8avw4pw8) | `otel` | Trace context carried from `Enqueue` to the step |

An app label under one of them would collide with, or be removed by, the package that owns it. The three spellings differ for historical reasons and are kept as they are, because runs in flight carry them. [`workflow.ReservedLabelPrefixes()`](ds:block?id=reservedlabelprefixes-s6pnbs6s) returns all three as a fresh slice, [`workflow.ReservedLabel(key)`](ds:block?id=reservedlabel-be47d7qr) reports whether a key falls under one and which, and [`workflow.ValidateLabels(labels)`](ds:block?id=validatelabels-henfu882) refuses the first reserved key (in sorted order, so the error is the same on every call) with an error wrapping `ErrInvalid`:

<!-- snippet: examples/capabilities/main.go#labels -->
```go
fmt.Fprintln(out, "reserved:", workflow.ReservedLabelPrefixes())
prefix, ok := workflow.ReservedLabel("otel.traceparent")
fmt.Fprintln(out, "otel.traceparent reserved:", ok, prefix)

err := workflow.ValidateLabels(map[string]string{"tenant": "acme", "workflow.batch.id": "b1"})
fmt.Fprintln(out, "ValidateLabels:", err)
```

```text
reserved: [ubgo.workflow/ workflow.batch. otel.]
otel.traceparent reserved: true otel.
ValidateLabels: workflow: invalid spec: label "workflow.batch.id" uses the reserved prefix "workflow.batch."
```

Where the check runs:

- `workflow.ValidateSchedule` calls it, so `Schedule.Labels` under a reserved prefix are refused by every driver (schedules are written only by apps).
- Each owning package refuses incoming labels under its own prefix: `middleware.Unique` and `ClaimCheck.Enqueue` refuse a job that already carries their labels (so a caller cannot release someone else's lock or point a run at another run's blobs), `batch` refuses reserved batch labels on `Batch.Labels`, and the `otel` enqueue middleware refuses incoming `otel.` labels.
- Every driver's public `Enqueue` (and `EnqueueTx`, `EnqueueMany` and group dispatch) calls [`workflow.CheckJobLabels(ctx, job)`](ds:block?id=checkjoblabels-p6m28b9h), which refuses a reserved label on the job or a chained job with `workflow.ErrInvalid` unless the package that owns it allowed that key on the context ([`workflow.AllowReservedLabels`](ds:block?id=allowreservedlabels-4czg8eyz), [`AllowReservedPrefix`](ds:block?id=allowreservedprefix-f884j92u)). A label typed by the app or copied from a request is refused; a label the library stamped passes. It is a guard against accidents and untrusted input, not a sandbox: only the owning package should call the allow functions.

## Try it

```sh
cd examples
go run ./capabilities
```

The last three lines are the ones above.

## Source labels

Two keys under `ubgo.workflow/` are reserved yet app-settable: [`workflow.LabelSource`](ds:block?id=labelsource-m6wjahhr) and [`workflow.LabelSourceRef`](ds:block?id=labelsourceref-qfv7bbhp), the run's trigger source and a reference within it. Set them with [`workflow.WithSource(job, source, ref)`](ds:block?id=withsource-g5xwt2m5); `CheckJobLabels` and `ValidateLabels` let them through when the source passes [`workflow.ValidateSource`](ds:block?id=validatesource-73nzwq5k) (lower-case letters, digits and `. _ -`, at most [64 bytes](ds:block?id=maxsourcelen-sx2rxxmg)) and the reference is at most [256 bytes](ds:block?id=maxsourcereflen-gygg9xx4). [`workflow.SourceOf(labels)`](ds:block?id=sourceof-7mrryr2k) reads them, or derives a source from the labels the library stamps (a schedule fire, a manual trigger, a spawned child, a batch member, an event), and [`middleware.RequireSource()`](ds:block?id=mw-requiresource-3tsntxp2) refuses a job whose source cannot be told. A source is provenance, never an authorization input. The whole model is on [Trigger source](trigger-source.md); a run recorder filters its history by it ([Record run history](../observability/run-recorder.md#trigger-source)). Two other kinds of run metadata ride on the step's context instead of labels, because only a step produces them: [step events](step-events.md) ([`workflow.RecordEvent`](ds:block?id=recordevent-k3aqhnwy)) and the [run outcome](run-outcome.md) ([`workflow.SetOutcome`](ds:block?id=setoutcome-p5kkstu4)).

## Pitfalls

- A job enqueued straight on the driver, bypassing the owning middleware, is not checked. Do not copy labels from one run's `RunEvent.Labels` onto a new job: they include the reserved ones the middleware stamped.
- Labels are not secret and not small-payload storage: they are written next to the run in the database or the engine and may be shown in a dashboard. Put data in the payload, and encrypt it when it is personal ([Encrypt payloads](../middleware/codecs.md#encrypt-payloads)).
- Labels are recorded on spans only on opt-in, so a tenant ID does not reach your tracing backend by accident ([Job labels on spans](../observability/opentelemetry.md#job-labels-on-spans)).
- Drivers pass the run's own map to steps and observers; treat `StepInfo.Labels` and `RunEvent.Labels` as read-only.

## Related

- [Names reference](../reference/names.md#labels)
- [Unique jobs](../reliability/unique-jobs.md#unique-for-a-time), [labels on a batch](../batches/batches.md#labels-on-a-batch), [trace context from enqueue to step](../observability/opentelemetry.md#trace-context-from-enqueue-to-step)
- [Trigger source](trigger-source.md), [Step events](step-events.md), [Run outcome](run-outcome.md)
- Example: [capabilities](../../examples/capabilities/), [trigger-source](../../examples/trigger-source/)
