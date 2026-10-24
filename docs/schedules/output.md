# Task output

A nightly export, a weekly cleanup or a report job writes a few log lines and ends. When it fails at 02:00, someone wants to read those lines in the morning, from a file or a mail, without searching the app's log stream. `middleware.CaptureOutput` collects what a step logs and how it ended and hands the result, after each attempt, to a sink you choose: a file the output replaces or extends, any `io.Writer`, or your own function that mails it, posts it to a chat or stores it. It is step middleware, so it works on every driver and for any job, not only scheduled ones.

## Before you start

- Steps whose output you want to capture log through `middleware.OutputLogger(ctx, fallback)` instead of a logger they hold. Outside a capture it returns `fallback` (or `slog.Default()` when nil), so the same step logs normally when capture is off.
- The example is [`examples/task-output`](../../examples/task-output). Run it with `go run ./task-output` from `examples/`.

## Step 1: log through OutputLogger

<!-- snippet: examples/task-output/main.go#step -->
```go
// report is the nightly task. It logs through middleware.OutputLogger, so
// its lines are captured when CaptureOutput wraps it and go to the app's
// logger otherwise.
func report(night *int) workflow.Handler {
	return func(ctx context.Context, _ []byte) ([]byte, error) {
		log := middleware.OutputLogger(ctx, nil)
		*night++
		log.Info("exporting orders", "night", *night)
		if *night == 2 {
			log.Error("warehouse API refused the export", "status", 503)
			return nil, workflow.Permanent(errors.New("export refused"))
		}
		log.Debug("not captured: below the default Info level")
		return []byte(`{"rows":42}`), nil
	}
}
```

## Step 2: choose a sink and wrap the steps

A sink is an `OutputSink`, one method `Deliver(ctx, Output) error`. The built-ins:

| Sink | What it does |
|---|---|
| `FileSink(path, FileReplace)` | Writes each attempt's output to the file, replacing what was there, so it holds the latest run only |
| `FileSink(path, FileAppend)` | Appends each attempt's output to the file |
| `WriterSink(w)` | Writes each attempt's output to any `io.Writer` |
| `OutputSinkFunc(fn)` | Your function: mail, chat, object storage, a database row |

The file sinks create the file with mode 0600, since output holds whatever a step logged. Every built-in writes the text of `middleware.FormatOutput`, which your own sink can reuse. The example appends every night to a file and mails the failures; mail is the app's own function, since the library sends none:

<!-- snippet: examples/task-output/main.go#sink -->
```go
appendToFile := middleware.FileSink(path, middleware.FileAppend)
mailOnFailure := middleware.OutputSinkFunc(func(ctx context.Context, o middleware.Output) error {
	if o.Err == nil {
		return nil
	}
	// Your mailer here: the body is the same text the file gets.
	fmt.Fprintf(&mailbox, "To: ops@example.com\nSubject: %s failed\n\n", o.Info.Workflow)
	return middleware.FormatOutput(&mailbox, o)
})
capture := middleware.CaptureOutput(
	middleware.OutputSinkFunc(func(ctx context.Context, o middleware.Output) error {
		return errors.Join(appendToFile.Deliver(ctx, o), mailOnFailure.Deliver(ctx, o))
	}),
	middleware.CaptureResult(),   // include the step's result; off by default
	middleware.CaptureClock(clk), // record times from the driver's clock
)
c := memory.New(memory.WithClock(clk), memory.WithMiddleware(capture))
```

`CaptureOutput` is added like any step middleware: `memory.WithMiddleware`, `sqlstore.WithMiddleware`, or the Hatchet driver's `Config.Middleware`.

## Step 3: read the output

Each delivery is one `middleware.Output`: the attempt's `StepInfo` (workflow, step, run ID, attempt), `Started` and `Finished`, the `Records` logged at or above the capture level, `Err`, and `Result` only with `CaptureResult`. In text it reads:

```text
--- nightly-report.log
== 2026-01-05T02:00:00Z nightly-report/export attempt 1 [run-2] ok
2026-01-05T02:00:00Z INFO exporting orders night=1
result: {"rows":42}
== 2026-01-06T02:00:00Z nightly-report/export attempt 1 [run-5] error: export refused
2026-01-06T02:00:00Z INFO exporting orders night=2
2026-01-06T02:00:00Z ERROR warehouse API refused the export status=503
--- mail
To: ops@example.com
Subject: nightly-report failed

== 2026-01-06T02:00:00Z nightly-report/export attempt 1 [run-5] error: export refused
2026-01-06T02:00:00Z INFO exporting orders night=2
2026-01-06T02:00:00Z ERROR warehouse API refused the export status=503
```

## Keep output in the database

A file or a mail reaches one place; an operator looking at a run on another machine, or in the [admin UI](../admin/ui.md), needs the output stored with the run. `sqlstore.OutputStore` is an `OutputSink` that keeps each attempt's output in `<prefix>outputs`, shared by every process on the database. Build it from a client with `Client.OutputStore(limits)` (`Client.Migrate` creates its table), or alone with `sqlstore.NewOutputStore(db, dialect, limits)` and its own `Migrate`, which is how an app on Hatchet with a database uses it. The program is [`examples/output-store`](../../examples/output-store):

<!-- snippet: examples/output-store/main.go#store -->
```go
// A client only to build the store from; the worker below gets its
// middleware. Client.Migrate creates <prefix>outputs too.
setup, err := sqlstore.New(db, sqlb.SQLiteDialect{})
if err != nil {
	return err
}
if err := setup.Migrate(ctx); err != nil {
	return err
}
outputs := setup.OutputStore(sqlstore.OutputLimits{}) // 20 attempts per run, 64 KiB each
c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
	sqlstore.WithPollInterval(20*time.Millisecond),
	sqlstore.WithMiddleware(middleware.CaptureOutput(outputs)),
)
```

`RunOutput(ctx, runID)` reads a run's attempts back, oldest first, as `StoredOutput` rows (step, attempt, start and finish, the text, whether it was cut, and the error):

<!-- snippet: examples/output-store/main.go#read -->
```go
attempts, err := outputs.RunOutput(ctx, ref.RunID) // oldest first
if err != nil {
	return err
}
for _, a := range attempts {
	fmt.Fprintf(out, "%s attempt %d error=%q logged fetching feed: %t\n", a.Step, a.Attempt, a.Error, strings.Contains(a.Text, "fetching feed"))
}
```

The admin reads output through an `admin.OutputSource`; sqlstore does not import admin, so `admin.OutputSourceFunc` maps the rows, field for field:

<!-- snippet: examples/output-store/main.go#admin -->
```go
// sqlstore does not import admin: map its rows to admin's, one for one.
src := admin.OutputSourceFunc(func(ctx context.Context, id workflow.RunID) ([]admin.AttemptOutput, error) {
	rows, err := outputs.RunOutput(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]admin.AttemptOutput, len(rows))
	for i, r := range rows {
		out[i] = admin.AttemptOutput(r) // the same fields, types and order
	}
	return out, nil
})
adm := admin.New(c, admin.WithOutputs(src))
```

```text
run succeeded
fetch attempt 1 error="feed timed out" logged fetching feed: true
fetch attempt 2 error="" logged fetching feed: true
admin sees 2 attempts
```

`OutputLimits{PerRun, Bytes}` keeps the newest 20 attempts per run and 64 KiB of text per attempt by default (`DefaultOutputPerRun`, `DefaultOutputBytes`), the same bounds as the admin's in-memory store; longer text is cut on a character boundary and marked `Truncated`. A client deletes a run's output with the run (`Delete`, retention); a store used without a client bounds its table with `PruneBefore(ctx, t)`. Output captured outside a driver, with no run to file it under, is refused with `sqlstore.ErrOutputNoRun`.

## Options

| Option | Effect |
|---|---|
| `CaptureResult()` | Adds the step's result bytes to `Output.Result`. Off by default, because a result may hold payload data you want kept out of files and mail |
| `CaptureOnlyFailures()` | Delivers only attempts that returned an error |
| `CaptureLevel(l)` | Keeps records at or above `l`; the default is Info |
| `CaptureTee(h)` | Also passes every record to your own `slog.Handler`, so the lines still reach the app's log |
| `CaptureOnly(fn)` | Captures only the attempts `fn(StepInfo)` returns true for, such as one workflow's steps |
| `CaptureOnSinkError(fn)` | Receives the sink's errors; the default drops them |
| `CaptureClock(clk)` | The clock that stamps `Started`, `Finished` and record times |

## Pitfalls

- **One delivery per attempt.** A step retried three times delivers three outputs. A scheduled task with one step, or a [function by name](../jobs/functions.md), delivers one per run attempt; a workflow with several steps delivers one per step attempt, so filter with `CaptureOnly` or group by `Info.RunID` in your sink.
- **A slow sink holds the worker.** `Deliver` runs on the step's goroutine after the attempt returns and before the driver records the result. A sink that sends mail or calls a network service should hand the output to its own goroutine or queue.
- **A sink error never fails the step.** It goes to `CaptureOnSinkError`, which drops it by default; pass a function that logs it.
- **Only `OutputLogger` lines are captured.** A step that logs through a logger it captured at start-up, or through `slog.Default()` directly, writes past the capture. `CaptureTee` sends captured lines on to your handler, so you do not lose them from the main log.
- **One capture per step.** If two `CaptureOutput` middlewares wrap the same step, the inner one receives the records and the outer one delivers an output with none. Use one capture with a sink that fans out, as the example does.
- **Two file sinks on one path are not serialised.** Deliveries through one `FileSink` value are; build one sink per file and share it.
- **Stored output is as sensitive as the payloads.** It holds whatever a step logged; only what you wire to the table reads it, and the admin shows it to everyone who may open a run.

## Related
