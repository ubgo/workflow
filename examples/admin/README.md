# admin

The operations side on the memory driver. A `send-email` workflow on the `emails` queue fails for one recipient while a fake SMTP server is down. The admin JSON API, mounted at `/admin/` behind a token login with an `admin` and a read-only `viewer` role, then shows:

- an anonymous request refused before routing;
- the failed run listed with the step that failed and its error, and a viewer's retry refused;
- an admin's retry, once SMTP is back, re-running only the failed step;
- the queue paused while three more runs wait, a `Monitor` alert on its depth, and the same depth as the `workflow.queue.depth` OpenTelemetry gauge;
- the queue resumed, the runs drained and the alert resolved.

```sh
cd examples
go run ./admin
```

```text
anonymous GET /failures: 401
viewer GET /failures: 200, 1 failed run, step "send": memory: step "send" failed after 1 attempt(s): smtp: connection refused
viewer POST retry: 403 {"error":{"code":"forbidden","message":"not allowed: runs.retry"}}
admin POST retry: 200, run now succeeded
admin POST /pause emails: 200
ALERT queue_depth queue=emails depth=3 (max 2)
workflow.queue.depth{workflow.queue=emails} = 3
admin POST /resume emails: 200
resolved queue_depth queue=emails depth=0 (max 2)
```

## The code

- `auth`: `admin.Auth[user]` with `Identify` (token to user) and `Authorize` (admins do anything, viewers only `Action.ReadOnly()` actions).
- `mount`: `admin.New` and `admin.NewHandler` under a prefix with `http.StripPrefix`.
- `monitor`: `admin.NewMonitor` with a per-queue and a default `Threshold`, an `AlerterFunc` and `Check`.
- `gauge`: `otel.Instrumentation.ObserveQueueDepth(adm.QueueDepths)`.

`main_test.go` runs the program and checks every line. The guides [docs/admin/operations.md](../../docs/admin/operations.md), [docs/admin/http-api.md](../../docs/admin/http-api.md), [docs/admin/alerts.md](../../docs/admin/alerts.md) and [docs/observability/opentelemetry.md](../../docs/observability/opentelemetry.md) copy their code from the regions marked `snippet:`.
