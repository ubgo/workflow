# middleware-stack

Step and enqueue middleware on the in-memory driver: the order `middleware.Chain` runs in, `Logging` with a level per attempt (`WithLogLevel`), `Recover`, `Timeout`, `Skip`, `ThrottleErrors` with a `CircuitStore` of the app's own, `FailOn`, then `WrapClient` with an enqueue middleware of the app's own, `Route` and `Unique`, and the refusal when `Unique` is put in one chain twice.

```sh
cd examples
go run ./middleware-stack
```

The log lines carry no time or run ID and durations read `0s` (Logging measures on a fake clock), so the output is the same on every run:

```text
a before
b before
handler
b after
a after
-- step middleware
level=WARN msg="workflow step failed" workflow=charge step=run attempt=1 duration=0s permanent=false error="payments api down"
run charge: succeeded, 2 handler call(s)
level=ERROR msg="workflow step panicked" panic="nil map"
level=ERROR msg="workflow step failed" workflow=panics step=run attempt=1 duration=0s permanent=false error="workflow/middleware: step panicked: nil map"
run panics: failed (memory: step "run" failed after 1 attempt(s): workflow/middleware: step panicked: nil map), 1 handler call(s)
level=ERROR msg="workflow step failed" workflow=validate step=run attempt=1 duration=0s permanent=true error="field amount: bad input"
run validate: failed (memory: step "run" failed after 1 attempt(s): field amount: bad input), 1 handler call(s)
level=ERROR msg="workflow step failed" workflow=slow step=run attempt=1 duration=0s permanent=false error="context deadline exceeded"
run slow: failed (memory: step "run" failed after 1 attempt(s): context deadline exceeded), 1 handler call(s)
run charge: succeeded, 0 handler call(s)
-- throttle
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="payments api down"
run call-api: failed (memory: step "run" failed after 1 attempt(s): payments api down), 1 handler call(s)
circuit "call-api" open for 5m0s
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="payments api down"
run call-api: failed (memory: step "run" failed after 1 attempt(s): payments api down), 1 handler call(s)
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="workflow/middleware: step throttled after repeated errors (key \"call-api\")"
run call-api: failed (memory: step "run" failed after 1 attempt(s): workflow/middleware: step throttled after repeated errors (key "call-api")), 0 handler call(s)
level=ERROR msg="workflow step failed" workflow=call-api step=run attempt=1 duration=0s permanent=false error="payments api down"
run call-api: failed (memory: step "run" failed after 1 attempt(s): payments api down), 1 handler call(s)
-- enqueue middleware
enqueue charge tier="bulk" queue="bulk" err=<nil>
run charge: succeeded, 0 handler call(s)
enqueue charge tier="" queue="" err=workflow: duplicate idempotency key: unique key "o-7" is held
same order again: true
unique twice: true workflow: invalid spec: Unique: label "ubgo.workflow/unique-key" uses the reserved prefix "ubgo.workflow/"
```

What each part shows:

- `charge` fails once and is retried; `WithLogLevel` logs the retried attempt at Warn.
- `panics` panics; `Recover` turns it into an error wrapping `middleware.ErrPanic`, and `Logging`, outside it, logs the failed attempt.
- `validate` has two retries but `FailOn` makes its `bad input` error permanent, so it runs once.
- `slow` blocks on its context until `Timeout` ends it.
- A `"skip"` payload completes without a handler call.
- `call-api` fails twice, its circuit opens, the third run is throttled without a handler call, and after the cooldown the step runs again.
- `Route` adds `queue=bulk` to a `tier=bulk` job, `Unique` refuses a second job for the same order, and two `Unique` layers refuse every job with `workflow.ErrInvalid`.

The guides that copy this program: [step middleware](../../docs/middleware/step-middleware.md), [enqueue middleware](../../docs/middleware/enqueue-middleware.md) and [logging](../../docs/observability/logging.md). `go test ./middleware-stack` runs it and checks every line.
