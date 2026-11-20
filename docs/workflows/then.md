# Completion triggers

When an import succeeds, its index should be rebuilt; when it fails, someone should be alerted. Enqueuing the index job from import's last step couples import to everything that follows it and misses the failure case, since a failed step never reaches that code. `workflow.Then` is a plugin that watches run ends and starts the next workflow per rule, so "after import, index" lives beside the workflows and a failed or cancelled end can trigger its own follow-up.

## Before you start

- `Then` observes run ends, so it needs a client with `workflow.RunNotifier`; `workflow.Use` refuses one without it with an `*workflow.UnsupportedError` naming `CapRunFinished`.
- With `workflow.CapRunFinished` (memory, SQL, Redis) every run end is delivered, including after a restart on the durable drivers. On Hatchet delivery is best effort, so a next job can be missed.
- The example is [`examples/events-more`](../../examples/events-more). Run it with `go run ./events-more` from `examples/`.

## Start a workflow when another finishes

`workflow.Then(rules, opts...)` returns the plugin; pass it to `Use` before `Register` and `Start`:

<!-- snippet: examples/events-more/main.go#then -->
```go
then, err := workflow.Then([]workflow.ThenRule{
	{When: "import", Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
		return workflow.Job{Workflow: "index", Payload: []byte(ev.Labels["file"])}, true, nil
	}},
	{Name: "import-failed", When: "import", On: []workflow.Status{workflow.StatusFailed},
		Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
			return workflow.Job{Workflow: "alert", Payload: []byte(ev.Labels["file"])}, true, nil // ev.Err says why
		}},
})
if err != nil {
	return err
}
c, err := workflow.Use(memory.New(), then) // before Register and Start
if err != nil {
	return err
}
```

```text
then:
  import ok.csv succeeded -> index ran with ok.csv
  import bad.csv failed -> alert ran with bad.csv
```

A `ThenRule` fires when a run of `When` ends in one of `On` (default: `StatusSucceeded` only). `Start` gets the `RunEvent` (the run's ref, status, error, labels, failed step) and returns the next job, or `false` to start nothing. The output is not in the event; a next job that needs it reads it with `workflow.ResultReader` or gets the finished run's ref. Every matching rule starts its own job, in rule order.

Each next job carries the label `workflow.LabelThenFrom` with the finished run's ID, and the idempotency key `then/<rule>/<finished run ID>` unless `Start` set one. The rule's name is `Name`, or `When>` plus the next job's workflow when `Name` is empty; set `Name` when two rules for one workflow start the same next workflow. A run-finished event delivered twice therefore starts the next job once while that job is unfinished.

`Then` enqueues through the client `Use` returns, below the `Then` plugin itself: put `Then` first in `Use` so the other plugins' enqueue middleware (unique jobs, tracing) applies to the next job. One `Then` value may be used with several clients; each enqueues on its own.

## Errors

An observer has no caller, so `Then` hands its errors to `workflow.ThenErrors(fn)`: each is a `*workflow.ThenError` with the rule, the finished run, the job (when `Start` built one) and the cause, so an app that must not lose the next job can enqueue it again from there. Without the option they are logged with `slog.Default` at error level. The errors are a `Start` error, a reserved label in the job `Start` returned (`ErrInvalid`), and the enqueue's error.

`Then` itself refuses, with `ErrInvalid`, no rules, a rule without `When` or `Start`, a status in `On` that is not terminal, and two rules with one `Name`.

## Pitfalls

- An enqueue that fails is reported, not retried: the observer runs once per delivered event. Handle `ThenError` if a lost next job matters.
- The key deduplicates only while the next job is unfinished. A driver that redelivers a run end after the next job finished would start it again, so make the next workflow idempotent where that matters.
- `Start` runs on the driver's observer goroutine; keep it quick and leave slow work to the job it returns.
