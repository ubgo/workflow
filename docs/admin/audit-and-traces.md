# Audit log and trace links

Two additions to the [admin JSON API](http-api.md) and its [web UI](ui.md) for teams that operate it together. An auditor asks who retried the failed charge last Tuesday, and who tried to delete runs without the right to: [`admin.WithAudit`](ds:block?id=admin-withaudit-qj2q5pss) records every write action with who did it, what, to which resource, how it ended and when. An engineer looking at a failed run wants its trace: [`admin.WithTraceLink`](ds:block?id=admin-withtracelink-rwcnu3e6) puts a link to the run's trace in the run detail and on the run page.

## Before you start

- Both are options of [`admin.NewHandler`](ds:block?id=admin-newhandler-np68g63j) and work over every driver the admin handler serves.
- The trace link reads the trace ID the [OpenTelemetry middleware](../observability/opentelemetry.md) carried in the run's labels, with [`otel.TraceIDOf`](ds:block?id=otel-traceidof-qmje4yr5) from the otel module. Any other source of a URL works too: the option takes a function of the run.
- The example is [`examples/admin-audit`](../../examples/admin-audit). It runs on the memory driver with a fake clock for the audit times. Run it with `go run ./admin-audit` from `examples/`.

## Audit every write action

[`admin.WithAudit(sink)`](ds:block?id=admin-withaudit-qj2q5pss) calls `sink.Record(ctx, entry)` once for every write request that reached `Authorize`, after the action ran or was refused ([`finishAudit`](ds:block?id=admin-handler-finishaudit-p5za5fer)). A write is any action whose [`ReadOnly()`](ds:block?id=admin-action-readonly-fgxr6xzq) is false: retry, delete, cancel, cancel-all, pause and resume, schedule pause, resume and trigger, force complete and fail, the bulk retries and deletes (dry runs included, with `Resource.DryRun` set), reschedule, edit, cancel-step, reset (with `Resource.Step` and `Resource.Reset`), fork (with `Resource.Step` and `Resource.Fork`; the entry names the original run, not the fork) and signal-waiting (with `Resource.Event`); [`TestAudit_EveryWriteRoute`](ds:block?id=admin-testaudit-everywriteroute-srkcs2a4) walks each one, and [`admin.Resource`](ds:block?id=admin-resource-6fktgbm8) holds the fields. [`admin.MemoryAudit`](ds:block?id=admin-memoryaudit-9mh2u7tn) keeps entries in memory for tests; an app implements [`AuditSink`](ds:block?id=admin-auditsink-wxp7v9x2) (or wraps a function with [`admin.AuditFunc`](ds:block?id=admin-auditfunc-ghwx8jf2)) to write to its own audit table.

<!-- snippet: examples/admin-audit/main.go#handler -->
```go
audit := &admin.MemoryAudit{} // an app writes to its own audit table
h := admin.NewHandler(admin.New(c, admin.WithClock(clk)), auth,
	admin.WithAudit(audit),
	admin.WithTraceLink(traceLink))
```

Each [`admin.AuditEntry`](ds:block?id=admin-auditentry-d4n3jycr) carries:

| Field | What it holds |
|---|---|
| `Who` | the principal `Identify` returned, as an `any` (type-assert it to your own type) |
| `Action`, `Resource` | what `Authorize` was asked about: the run, the filter, the schedule, the pause target, the force-fail reason |
| `Allowed` | false when `Authorize` refused: the operation never ran, and the attempt is still recorded ([`noteAuthorize`](ds:block?id=admin-noteauthorize-2wqmbyjs)) |
| `Status` | the HTTP status answered |
| `Err` | why the action did not succeed, nil on success; the operation's full error (it matches the core sentinels such as `workflow.ErrNotFound`), even when the response only said "internal error" ([`TestAudit_ResourceAndOutcome`](ds:block?id=admin-testaudit-resourceandoutcome-nfb42tq6)) |
| `At` | when the action ended, on the Admin's clock ([`admin.WithClock`](ds:block?id=admin-withclock-635ed787); [`finishAudit`](ds:block?id=admin-handler-finishaudit-p5za5fer)) |

<!-- snippet: examples/admin-audit/main.go#entries -->
```go
fmt.Println("audit log:")
for _, e := range audit.Entries() {
	who := e.Who.(user) // the handler's own principal type
	outcome := "ok"
	if errors.Is(e.Err, workflow.ErrNotFound) {
		outcome = "not found"
	} else if e.Err != nil {
		outcome = e.Err.Error()
	}
	run := strings.Replace(e.Resource.Run.RunID.String(), ref.RunID.String(), "<run>", 1)
	fmt.Printf("  %s %s %s %s allowed=%v %d %s\n", e.At.Format(time.TimeOnly), who.Name, e.Action, run, e.Allowed, e.Status, outcome)
}
```

## Link each run to its trace

[`admin.WithTraceLink(fn)`](ds:block?id=admin-withtracelink-rwcnu3e6) adds `trace_url` to `GET /runs/{id}` and an "Open trace" link to the run page. `fn` gets the run's `RunInfo`, labels included; [`otel.TraceIDOf(labels)`](ds:block?id=otel-traceidof-qmje4yr5) returns the 32-hex-digit trace ID the enqueue middleware carried in them (the W3C `traceparent` under the `otel.` prefix), and false when they hold none. The example's handler above passes this function:

<!-- snippet: examples/admin-audit/main.go#tracelink -->
```go
// traceLink builds the run page's "Open trace" link from the trace ID the
// otel enqueue middleware carried in the run's labels. A run enqueued
// without that middleware carries none, and gets no link.
func traceLink(r workflow.RunInfo) string {
	id, ok := wfotel.TraceIDOf(r.Labels) // 32 hex digits from the W3C traceparent
	if !ok {
		return ""
	}
	return "https://tracing.example.com/trace/" + id
}
```

[`otel.SpanContextOf(labels, propagator)`](ds:block?id=otel-spancontextof-xxyvu2wh) returns the whole span context instead, for code that starts a span linked to the enqueue, and reads labels written by another propagator (the one given to the otel module's `WithPropagator`; nil is W3C trace context). It is invalid (`IsValid()` false) when the labels carry none:

<!-- snippet: examples/admin-audit/main.go#spancontext -->
```go
// The whole span context, for code that starts a span linked to the
// enqueue (nil: the W3C propagator, the otel module's default).
sc := wfotel.SpanContextOf(page.Runs[0].Labels, nil)
fmt.Printf("  span context: span %s, sampled %v\n", sc.SpanID(), sc.IsSampled())
```

Only an absolute `http` or `https` URL with a host is shown ([`safeLink`](ds:block?id=admin-safelink-g9znk3p8)). Anything else (a `javascript:` or `data:` URL, a relative path, a control character) is dropped and logged ([`traceURL`](ds:block?id=admin-handler-traceurl-x89t3656), [`TestTraceLink`](ds:block?id=admin-testtracelink-nqdpbxrk)), and the UI checks the scheme again before it builds the link with the DOM. The page keeps its Content-Security-Policy; the link opens in a new tab with `rel="noopener noreferrer"`. Lists do not carry the link, only the run detail.

## Try it

```sh
cd examples
go run ./admin-audit
```

```text
actions:
  vic POST /admin/runs/<run>/retry: 403
  ann POST /admin/runs/<run>/retry: 200
  ann GET /admin/runs/<run>: 200
  ann DELETE /admin/runs/no-such-run: 404
audit log:
  09:30:00 vic runs.retry <run> allowed=false 403 admin: not allowed: runs.retry
  09:30:00 ann runs.retry <run> allowed=true 200 ok
  09:30:00 ann runs.delete no-such-run allowed=true 404 not found
run detail:
  status succeeded
  trace_url https://tracing.example.com/trace/4bf92f3577b34da6a3ce929d0e0e4736
  span context: span 00f067aa0ba902b7, sampled true
```

The viewer's refused retry is in the audit log with `allowed=false`; the read is not. The delete of a run that does not exist is recorded with its 404 and the full `workflow.ErrNotFound` error.

## Pitfalls

- **Requests refused before `Authorize` are not audited.** An anonymous request (401), a write without a JSON content type (415) or a malformed body (400) asked for no action ([`TestAudit_NotBeforeAuthorize`](ds:block?id=admin-testaudit-notbeforeauthorize-m6tfbmqp)). Log those at the HTTP layer if you need them.
- **The sink runs on the request.** `Record` runs after the response is written, on the request's goroutine, so a slow sink holds the connection a little longer. A sink error or panic is logged (`admin.WithLogger`; [`finishAudit`](ds:block?id=admin-handler-finishaudit-p5za5fer)) and never changes the response ([`TestAudit_SinkFailureNeverChangesTheResponse`](ds:block?id=admin-testaudit-sinkfailureneverchangestheresponse-k96rsu8g)), which has already been sent; buffer and retry in the sink if no entry may be lost.
- **`Err` holds the full error.** It can name hosts or data the response hid. Keep the audit store as private as your logs.
- **[`MemoryAudit`](ds:block?id=admin-memoryaudit-9mh2u7tn) keeps everything.** It is for tests and short-lived tools, not a long-running server.
- **No trace label, no link.** Runs enqueued without the otel enqueue middleware, or enqueued by a scheduler before it was added, carry no trace context; [`TraceIDOf`](ds:block?id=otel-traceidof-qmje4yr5) returns false and the function should return "".
- **Only the Go API is not audited.** [`WithAudit`](ds:block?id=admin-withaudit-qj2q5pss) belongs to the HTTP handler, which knows who the caller is. Code that calls `Admin.Retry` directly records its own audit entry.

## Related

- [Admin JSON API](http-api.md): the routes and actions being audited.
- [Admin web UI](ui.md): the run page that shows the trace link.
- [OpenTelemetry](../observability/opentelemetry.md): the middleware that carries the trace in labels.
- Example: [examples/admin-audit](../../examples/admin-audit/).
