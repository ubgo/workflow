# step-events

A step that records two timeline events with [`workflow.RecordEvent`](ds:block?id=recordevent-k3aqhnwy) and [`workflow.RecordEventLevel`](ds:block?id=recordeventlevel-b9kyju7j) and sets a partial outcome with [`workflow.SetOutcome`](ds:block?id=setoutcome-p5kkstu4), run twice. On a plain memory client, step middleware of the app's own binds a sink with [`workflow.WithStepEventSink`](ds:block?id=withstepeventsink-ehcpt8xf) and prints each event as it is recorded, [`middleware.CaptureOutput`](ds:block?id=mw-captureoutput-fk79ecqu) keeps the same events with the step's captured log, and `SetOutcome` returns an `UnsupportedError` because nothing keeps an outcome. With a run recorder applied ([`recorder.NewMemoryStore`](ds:block?id=rec-newmemorystore-5yrbgb94), `workflow.Use`), the events are stored with the step and the outcome with the run; a second workflow that sets no outcome gets the default outcome of its status.

```sh
cd examples
go run ./step-events
```

```text
-- without a recorder
event sync/fetch INFO "fetched changes" [{rows 100}]
event sync/fetch WARN "rows rejected" [{bad 3} {first row-17}]
outcome kept: false unsupported: true
== 2026-01-05T02:00:00Z sync/fetch attempt 1 [run-2] ok
2026-01-05T02:00:00Z INFO fetched changes rows=100
2026-01-05T02:00:00Z WARN rows rejected bad=3 first=row-17
-- with a recorder
outcome kept: true unsupported: false
run succeeded outcome=partial (3 of 100 rows rejected)
  fetch INFO "fetched changes" [{rows 100}]
  fetch WARN "rows rejected" [{bad 3} {first row-17}]
cleanup succeeded outcome=success (set by a step: false)
```

The captured block is printed after the attempt ends, so it follows the line the step printed. `go test ./step-events` runs the program and checks every line.

## Guides

- [Step events](../../docs/concepts/step-events.md)
- [Run outcome](../../docs/concepts/run-outcome.md)
- [Step middleware › Capture step events](../../docs/middleware/step-middleware.md#capture-step-events)
