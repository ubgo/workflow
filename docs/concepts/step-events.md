# Step events

A step event is one line of a step's timeline: "fetched 4,000 rows", "retrying the upload", "3 rows rejected". A step records it with [`workflow.RecordEvent`](ds:block?id=recordevent-k3aqhnwy), and the library hands it to every sink bound on the step's context: a [run recorder](../observability/run-recorder.md) stores it with the step attempt, [`middleware.CaptureOutput`](../schedules/output.md) keeps it with the step's captured log, and your own middleware can print it, add it to a span or count it. A step records events the same way whether anything listens or not.

## Before you start

- You have written a step ([Steps](../workflows/steps.md)) and know [step middleware](../middleware/step-middleware.md).
- The runnable program for this page is [examples/step-events](../../examples/step-events/): the same step on a plain memory client and on one with a run recorder.

## Step 1: record events from a step

`workflow.RecordEvent(ctx, msg, attrs...)` records `msg` with `slog` attributes at `slog.LevelInfo`; [`workflow.RecordEventLevel(ctx, level, msg, attrs...)`](ds:block?id=recordeventlevel-b9kyju7j) takes a level, so a sink can colour or alert on a warning:

<!-- snippet: examples/step-events/main.go#step -->
```go
return func(ctx context.Context, _ []byte) ([]byte, error) {
	workflow.RecordEvent(ctx, "fetched changes", slog.Int("rows", 100))
	workflow.RecordEventLevel(ctx, slog.LevelWarn, "rows rejected", slog.Int("bad", 3), slog.String("first", "row-17"))
	err := workflow.SetOutcome(ctx, workflow.OutcomePartial, "3 of 100 rows rejected")
	fmt.Println("outcome kept:", err == nil, "unsupported:", errors.Is(err, workflow.ErrUnsupported))
	return nil, nil
}
```

([`SetOutcome`](ds:block?id=setoutcome-p5kkstu4) is the subject of [Run outcome](run-outcome.md).) What a sink receives is a [`workflow.StepEvent`](ds:block?id=stepevent-f7gyzzpj):

| Field | What it holds |
|---|---|
| `Message` | The text, cut to [`workflow.MaxStepEventMessage`](ds:block?id=maxstepeventmessage-yfsr3k3h) (1 KiB) without splitting a character |
| `Attrs` | `[]workflow.EventAttr`, key and value as strings (each value rendered with `slog.Value.String` after `Resolve`), in the caller's order; at most [`workflow.MaxStepEventAttrs`](ds:block?id=maxstepeventattrs-das9dx82) (32), each key and value cut to [`workflow.MaxStepEventValue`](ds:block?id=maxstepeventvalue-ukkmnhkc) (1 KiB) |
| `Level` | The `slog.Level` given; Info with `RecordEvent` |
| `At` | Zero: the core reads no wall clock on a step's path, so each sink stamps the time from its own clock |
| `Step` | The `StepInfo` of the attempt that recorded it (workflow, step, run ID, attempt, labels); zero outside a driver-run step |

With no sink bound, `RecordEvent` does nothing and returns nothing: an event is a log line, not data the caller asked to keep. [`workflow.HasStepEventSink(ctx)`](ds:block?id=hasstepeventsink-95trpj6t) reports whether any sink is bound, so a step can skip building an expensive message nobody will read.

## Step 2: bind a sink of your own

A sink is a [`workflow.StepEventSink`](ds:block?id=stepeventsink-f4enzkvf), one method `RecordStepEvent(ctx, StepEvent)`; [`workflow.StepEventFunc`](ds:block?id=stepeventfunc-473b6uwj) adapts a function. Step middleware binds it with [`workflow.WithStepEventSink(ctx, sink)`](ds:block?id=withstepeventsink-ehcpt8xf) before calling the handler:

<!-- snippet: examples/step-events/main.go#sink -->
```go
// Step middleware that binds a sink of its own: every event the step
// records reaches it, next to any other sink bound on the context.
printEvents := func(next workflow.Handler) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		sink := workflow.StepEventFunc(func(_ context.Context, e workflow.StepEvent) {
			fmt.Printf("event %s/%s %s %q %v\n", e.Step.Workflow, e.Step.Step, e.Level, e.Message, e.Attrs)
		})
		return next(workflow.WithStepEventSink(ctx, sink), in)
	}
}
```

`WithStepEventSink` adds to the sinks the context already carries, so a recorder, a tracer and a log capture each bind their own and every one sees every event, in binding order: the outermost middleware's sink first. A nil sink leaves the context unchanged.

A sink must be safe for concurrent use, since steps of one run may run in parallel, and must not block: `RecordEvent` calls it on the step's goroutine.

## Step 3: let the stock sinks keep them

Two sinks ship with the library:

- **The run recorder** ([`recorder.Recorder`](ds:block?id=rec-recorder-dss7wy23)'s step middleware) stores each event on the step attempt that recorded it, with its level and attributes, after redaction. `RunRecord.Steps[i].Events` reads them back ([Record run history](../observability/run-recorder.md#step-events-and-outcome)).
- **[`middleware.CaptureOutput`](ds:block?id=mw-captureoutput-fk79ecqu)** turns each event into a captured log record at the event's level, under the capture level like any record, so it reaches the output sink with the lines the step logged through [`OutputLogger`](ds:block?id=mw-outputlogger-r8mzkh57) ([Step middleware › Capture step events](../middleware/step-middleware.md#capture-step-events)).

<!-- snippet: examples/step-events/main.go#capture -->
```go
// CaptureOutput is a sink too: events join the step's captured log.
// The fake clock only makes the printed times fixed.
stamp := clock.NewFake(time.Date(2026, 1, 5, 2, 0, 0, 0, time.UTC))
capture := middleware.CaptureOutput(middleware.WriterSink(os.Stdout), middleware.CaptureClock(stamp))
plain := memory.New(memory.WithMiddleware(printEvents, capture))
```

## Try it

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

The `event` lines come from the app's sink as each event is recorded; the block starting `==` is `CaptureOutput` delivering after the attempt; the indented lines are the recorder's stored events. `go test ./step-events` checks every line.

## Pitfalls

- **Messages are not redacted; attributes are.** A recorder's redactor matches attribute keys ([`recorder.JSONPaths`](ds:block?id=rec-jsonpaths-66ph5w5h)), never the message text, so put values in attributes, not in the message.
- **Events are bounded, silently.** A longer message or value is cut and attributes past the [32nd](ds:block?id=maxstepeventattrs-das9dx82) are dropped, with no error: keep events short and put bulk data in the step's output.
- **A slow sink slows the step.** Every sink runs on the step's goroutine; a recorder write is bounded by its [`WithWriteTimeout`](ds:block?id=rec-withwritetimeout-rn35n8vw) ([5 seconds](ds:block?id=rec-defaultwritetimeout-wbngg5vx) by default), a sink of your own by nothing. Hand slow work to a goroutine or a queue.
- **Outside a driver there is no `Step`.** A handler called directly in a test gets events with a zero `StepInfo`, and the recorder records nothing for a context without one.
- **Events are not logs.** `RecordEvent` does not write to `slog.Default()` or to the [`CaptureTee`](ds:block?id=mw-capturetee-zz3fu929) handler; log through your logger as well when the line must reach the app's log.

## Related

- [Run outcome](run-outcome.md) and [Trigger source](trigger-source.md): the other run metadata a recorder keeps.
- [Record run history](../observability/run-recorder.md): where a recorder stores events.
- [Task output](../schedules/output.md): `CaptureOutput` and `OutputLogger`.
- [Write step middleware](../extending/writing-middleware.md#write-step-middleware): binding sinks from your own middleware.
- Example: [step-events](../../examples/step-events/).
