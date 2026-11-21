# admin-audit

The admin handler's audit log and trace links, on the in-memory driver: `admin.WithAudit` records every write action (a viewer's refused retry included) with who, what, the run, the status and the time on a fake clock, and `admin.WithTraceLink` with `otel.TraceIDOf` gives the run detail a `trace_url` built from the trace the OpenTelemetry enqueue middleware carried in the run's labels; `otel.SpanContextOf` reads the whole span context from the same labels.

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

The guide that copies this program is [audit log and trace links](../../docs/admin/audit-and-traces.md). `go test ./admin-audit` checks every line.
