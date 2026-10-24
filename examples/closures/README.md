# closures

A Go function run as a job on the in-memory driver, for small in-process work that does not deserve a registered workflow: a closure that fails once and is retried, how its run appears to `Status` and to run-finished observers, a delayed closure cancelled before it starts, validation, and the refusal on the SQL and Hatchet drivers.

```sh
cd examples
go run ./closures
```

```text
closure warm-price-cache: succeeded after 2 attempts, workflow "ubgo.workflow.closure", label "ubgo.workflow/closure"
Status: succeeded, step "closure"
closure nudge: cancelled before it started
closure without Fn: ErrInvalid true
sqlstore refuses: missing Closure, ErrUnsupported true
hatchet refuses: missing Closure, ErrUnsupported true
```

A closure run is a run of the workflow `workflow.ClosureWorkflow` with one step, `workflow.ClosureStep`, and the closure's name in the `workflow.LabelClosure` label, so listings, observers and middleware see it like any run. A closure lives only in the memory of the process that enqueued it: a restart loses it, and it never runs on another machine. The SQL and Hatchet drivers cannot store or send a function, so `workflow.EnqueueClosure` refuses there with an `UnsupportedError` naming `CapClosure`; register a workflow for anything that must survive.

The guide that copies this program is [closures](../../docs/jobs/closures.md). `go test ./closures` checks every line.
