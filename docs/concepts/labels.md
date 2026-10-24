# Labels

`Job.Labels` is a `map[string]string` of app metadata that travels with a run: a tenant, a source, a request ID. It reaches every step through `StepInfo.Labels` and every run-finished observer through `RunEvent.Labels`, so middleware can require one or log it without trusting a context convention. A few prefixes are kept for the library's own packages, which write labels onto the jobs they pass down; this page says which, and how to keep app labels out of them.

## Before you start

- You have enqueued a job with labels ([Your first workflow](../start/first-workflow.md#step-4--enqueue-and-read-the-status)).

## Reserved prefixes

| Prefix | Constant | Owned by | What it carries |
|---|---|---|---|
| `ubgo.workflow/` | `workflow.LabelPrefixInternal` | `middleware` | Unique locks (`middleware.UniqueKeyLabel`, `UniqueOwnerLabel`) and the claim-check token (`middleware.ClaimCheckLabel`) |
| `workflow.batch.` | `workflow.LabelPrefixBatch` | `batch` | A job's batch ID and its index in the batch |
| `otel.` | `workflow.LabelPrefixOTel` | `otel` | Trace context carried from `Enqueue` to the step |

An app label under one of them would collide with, or be removed by, the package that owns it. The three spellings differ for historical reasons and are kept as they are, because runs in flight carry them. `workflow.ReservedLabelPrefixes()` returns all three as a fresh slice, `workflow.ReservedLabel(key)` reports whether a key falls under one and which, and `workflow.ValidateLabels(labels)` refuses the first reserved key (in sorted order, so the error is the same on every call) with an error wrapping `ErrInvalid`:

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
- Every driver's public `Enqueue` (and `EnqueueTx`, `EnqueueMany` and group dispatch) calls `workflow.CheckJobLabels(ctx, job)`, which refuses a reserved label on the job or a chained job with `workflow.ErrInvalid` unless the package that owns it allowed that key on the context (`workflow.AllowReservedLabels`, `AllowReservedPrefix`). A label typed by the app or copied from a request is refused; a label the library stamped passes. It is a guard against accidents and untrusted input, not a sandbox: only the owning package should call the allow functions.

## Try it

```sh
cd examples
go run ./capabilities
```

The last three lines are the ones above.

## Pitfalls

- A job enqueued straight on the driver, bypassing the owning middleware, is not checked. Do not copy labels from one run's `RunEvent.Labels` onto a new job: they include the reserved ones the middleware stamped.
- Labels are not secret and not small-payload storage: they are written next to the run in the database or the engine and may be shown in a dashboard. Put data in the payload, and encrypt it when it is personal ([Encrypt payloads](../middleware/codecs.md#encrypt-payloads)).
- Labels are recorded on spans only on opt-in, so a tenant ID does not reach your tracing backend by accident ([Job labels on spans](../observability/opentelemetry.md#job-labels-on-spans)).
- Drivers pass the run's own map to steps and observers; treat `StepInfo.Labels` and `RunEvent.Labels` as read-only.

## Related

- [Names reference](../reference/names.md#labels)
- [Unique jobs](../reliability/unique-jobs.md#unique-for-a-time), [labels on a batch](../batches/batches.md#labels-on-a-batch), [trace context from enqueue to step](../observability/opentelemetry.md#trace-context-from-enqueue-to-step)
- Example: [capabilities](../../examples/capabilities/)
