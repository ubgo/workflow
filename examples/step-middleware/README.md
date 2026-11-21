# step-middleware

The four small step middleware of the `middleware` package, one at a time, each on a client of its own on the in-memory driver: `Recover` turns a panic into a failed attempt (and a retry policy refuses to retry `ErrPanic`), `Timeout` stops a step that would wait an hour, `FailOn` fails a declined card at once while a busy gateway keeps its retries, and `Skip` completes a step without calling it.

```sh
cd examples
go run ./step-middleware
```

```text
-- recover
level=ERROR msg="workflow step panicked" panic="assignment to entry in nil map"
run: failed after 1 attempt(s): memory: step "parse" failed after 1 attempt(s): workflow/middleware: step panicked: assignment to entry in nil map
history: attempt 1 "workflow/middleware: step panicked: assignment to entry in nil map", stack kept: true
-- timeout
run: failed: memory: step "download" failed after 1 attempt(s): context deadline exceeded
stopped well before the hour: true
-- fail on
declined: failed after 1 attempt(s)
busy: failed after 3 attempt(s)
-- skip
{"order":1}: succeeded, notify called 1 time(s)
{"order":2,"quiet":true}: succeeded, notify called 0 time(s)
```

The guide that copies this program is [step middleware](../../docs/middleware/step-middleware.md#recover-from-panics).
