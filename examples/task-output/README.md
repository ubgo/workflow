# task-output

A scheduled task's output captured and sent on after each run, the way a scheduler sends a task's output to a file or by mail. `middleware.CaptureOutput` wraps the step, the step logs through `middleware.OutputLogger`, and after each attempt the captured records, the outcome and (with `CaptureResult`) the step's result go to an `OutputSink`: here one that appends to a file with `FileSink(path, FileAppend)` and, when the run failed, hands the same text to a mail function. The schedule runs nightly at 02:00 on the in-memory driver with a fake clock, so two nights pass at once; the second fails.

```sh
cd examples
go run ./task-output
```

```text
night 1: succeeded
night 2: failed
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

The Debug line the step logs is not in the output, because the default capture level is Info (`CaptureLevel` changes it). The mail function is the app's own: the library sends no mail, it hands each `Output` to the sink.

The guide that copies this program is [Task output](../../docs/schedules/output.md). `go test ./task-output` checks every line.
