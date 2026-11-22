# Completion triggers

When an import succeeds, its index should be rebuilt; when it fails, someone should be alerted. Enqueuing the index job from import's last step couples import to everything that follows it and misses the failure case, since a failed step never reaches that code. [`workflow.Then`](ds:block?id=then-j2sz7cnw) is a plugin that watches run ends and starts the next workflow per rule, so "after import, index" lives beside the workflows and a failed or cancelled end can trigger its own follow-up.

## Before you start

- `Then` observes run ends, so it needs a client with [`workflow.RunNotifier`](ds:block?id=runnotifier-tbkv69uz); [`workflow.Use`](ds:block?id=use-yq9nazws) refuses one without it with an `*workflow.UnsupportedError` naming `CapRunFinished`.
- With [`workflow.CapRunFinished`](ds:block?id=caprunfinished-7nrsun98) ([memory](ds:block?id=memory-caps-yxkuq583), [SQL](ds:block?id=sql-caps-yu37ef2j), [Redis](ds:block?id=redis-caps-bfgdst9t)) every run end is delivered, including after a restart on the durable drivers. On [Hatchet](ds:block?id=hatchet-caps-d6x9fv8z) delivery is best effort ([`RunNotifier`'s contract](ds:block?id=runnotifier-tbkv69uz)), so a next job can be missed.
- The rules are shown in [`examples/events-more`](../../examples/events-more) (`go run ./events-more` from `examples/`); redo runs and the error hook in [`examples/then-redo`](../../examples/then-redo) (`go run ./then-redo`).
- Redo runs come from resetting or forking a run, which the memory, SQL and Redis drivers offer through [`workflow.RunResetterWith`](ds:block?id=runresetterwith-792722gn) and [`workflow.RunForker`](ds:block?id=runforker-9cpe2f7f) ([manage runs](../jobs/manage-runs.md#reset-a-run-that-succeeded)).

## Start a workflow when another finishes

[`workflow.Then(rules, opts...)`](ds:block?id=then-j2sz7cnw) returns the plugin; pass it to `Use` before `Register` and `Start`:

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

A [`ThenRule`](ds:block?id=thenrule-rcv7hzmr) fires when a run of `When` ends in one of `On` (default: `StatusSucceeded` only). `Start` gets the `RunEvent` (the run's ref, status, error, labels, failed step) and returns the next job, or `false` to start nothing. The output is not in the event; a next job that needs it reads it with `workflow.ResultReader` or gets the finished run's ref. Every matching rule starts its own job, in rule order ([`observe`](ds:block?id=thenplugin-observe-fkwupcpn)).

Each next job carries the label [`workflow.LabelThenFrom`](ds:block?id=labelthenfrom-8h5tpxrg) with the finished run's ID, and the idempotency key `then/<rule>/<finished run ID>` unless `Start` set one ([`observe`](ds:block?id=thenplugin-observe-fkwupcpn)). The rule's name is `Name`, or `When>` plus the next job's workflow when `Name` is empty; set `Name` when two rules for one workflow start the same next workflow. A run-finished event delivered twice therefore starts the next job once while that job is unfinished.

`Then` enqueues through the client `Use` returns, below the `Then` plugin itself ([its enqueue middleware](ds:block?id=thenplugin-enqueuemiddleware-45pnxp9j) binds that enqueue): put `Then` first in `Use` so the other plugins' enqueue middleware (unique jobs, tracing) applies to the next job. One `Then` value may be used with several clients; each enqueues on its own.

## Redo runs

A run reset after it had succeeded (`ResetOptions.Succeeded`) and a fork of a run that had succeeded carry [`workflow.LabelRedo`](ds:block?id=labelredo-7f5kcp9k) with the value `"true"` ([manage runs](../jobs/manage-runs.md#reset-a-run-that-succeeded)). The reset keeps the run's ref, so the run that ends is the same run that already succeeded once:

<!-- snippet: examples/then-redo/main.go#redo -->
```go
// Run the import again although it succeeded: the run keeps its ref
// and gets LabelRedo.
if _, err := c.(workflow.RunResetterWith).ResetRunWith(ctx, ref, importStep,
	workflow.ResetOptions{Succeeded: true}); err != nil {
	return err
}
```

A fork is a new run, made from the succeeded one; it carries the label too ([`ForkLabels`](ds:block?id=forklabels-479w9npr)), and it drops `workflow.LabelThenFrom` and the other membership labels of the original ([`forkDrops`](ds:block?id=forkdrops-k5wk4h5m)):

<!-- snippet: examples/then-redo/main.go#fork -->
```go
// A fork of a succeeded run is a new run that also carries LabelRedo.
if _, err := c.(workflow.RunForker).ForkRun(ctx, ref, importStep, workflow.ForkOptions{}); err != nil {
	return err
}
```

The first success already started each rule's next job, and the idempotency key deduplicates only while that job is unfinished, so by default `Then` skips the end of every run carrying the label ([`observe`](ds:block?id=thenplugin-observe-fkwupcpn)): neither the reset nor the fork above starts a second `index`. Pass [`workflow.ThenAfterRedo()`](ds:block?id=thenafterredo-zh59sk4a) when the next job must follow the corrected result too:

<!-- snippet: examples/then-redo/main.go#after-redo -->
```go
follow, err := workflow.Then(indexAfterImport(), workflow.ThenAfterRedo()) // index must be idempotent
if err != nil {
	return err
}
```

With it, the reset and the fork each start one more `index`, so the next workflow must be safe to run again for the same input.

## Errors

An observer has no caller, so `Then` hands its errors to [`workflow.ThenErrors(fn)`](ds:block?id=thenerrors-j2g8pm8g): each is a [`*workflow.ThenError`](ds:block?id=thenerror-9gk7jpsy) with the rule, the finished run (`From`), the job (when `Start` built one) and the cause (`Err`, also reached by `errors.Unwrap`), so an app that must not lose the next job can enqueue it again from there. Without the option they are logged with `slog.Default` at error level ([`Then`](ds:block?id=then-j2sz7cnw)). The errors are a `Start` error, a reserved label in the job `Start` returned (`ErrInvalid`), a plugin used without `Use`, and the enqueue's error ([`observe`](ds:block?id=thenplugin-observe-fkwupcpn)).

<!-- snippet: examples/then-redo/main.go#errors -->
```go
thenErrs := make(chan error, 1)
then, err := workflow.Then([]workflow.ThenRule{{
	Name: "audit", When: importWorkflow,
	Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
		return workflow.Job{}, false, errors.New("audit log is read-only")
	},
}}, workflow.ThenErrors(func(err error) { thenErrs <- err })) // instead of slog.Default
if err != nil {
	return err
}
```

`fn` runs on the driver's observer goroutine, so hand the error on (a channel, a queue, a counter) and return. A rule whose `Start` failed has no job, so `ThenError.Job` is the zero `Job`.

[`Then`](ds:block?id=then-j2sz7cnw) itself refuses, with `ErrInvalid`, no rules, a rule without `When` or `Start`, a status in `On` that is not terminal, and two rules with one `Name`.

## Try it

```sh
cd examples
go run ./then-redo
```

```text
default: redo runs are skipped
  first import: succeeded, redo label false, index runs so far 1
  reset import: succeeded, redo label true, index runs so far 1
  forked import: succeeded, redo label true, index runs so far 1
with ThenAfterRedo: every redo starts index
  first import: succeeded, redo label false, index runs so far 1
  reset import: succeeded, redo label true, index runs so far 2
  forked import: succeeded, redo label true, index runs so far 3
errors:
  rule "audit", job started: false, cause: audit log is read-only
```

Each line is printed by an observer registered after `Use`, which runs after `Then`'s own observer for the same end, so the count already includes the job `Then` started for it. `go run ./events-more` prints the `then:` lines shown under [Start a workflow when another finishes](#start-a-workflow-when-another-finishes), among the other event examples.

## Pitfalls

- An enqueue that fails is reported, not retried: the observer runs once per delivered event ([`observe`](ds:block?id=thenplugin-observe-fkwupcpn)). Handle `ThenError` if a lost next job matters.
- The key deduplicates only while the next job is unfinished ([`Then`](ds:block?id=then-j2sz7cnw)). A driver that redelivers a run end after the next job finished would start it again, so make the next workflow idempotent where that matters.
- `Start` runs on the driver's observer goroutine ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)); keep it quick and leave slow work to the job it returns.
- **A redo that ends while the first next job still runs starts nothing, even with `ThenAfterRedo`.** A reset keeps the run's ID, so the key `then/<rule>/<run ID>` is the same, and while the first next job is unfinished the driver hands back that job instead of starting another (the memory driver returns the existing run without an error: [`enqueueLocked`](ds:block?id=memory-client-enqueuelocked-z3x2cytk)). Wait for the next job to end before resetting, or have `Start` set its own `IdempotencyKey`.
- **Redo runs are skipped by default.** An operator who resets a succeeded import to fix its data gets no new `index` unless the plugin was built with `ThenAfterRedo`; decide per plugin which behaviour the next workflow needs.
- **Observers run in registration order** ([`RunNotifier`](ds:block?id=runnotifier-tbkv69uz)). An observer you register before `Use` sees a run end before `Then` has started its next job.

## Related

- [Manage runs](../jobs/manage-runs.md#reset-a-run-that-succeeded): reset and fork, which make redo runs.
- [Run finished hooks](../observability/run-finished.md): the run-end observer `Then` builds on.
- [Chains of jobs](chains.md): a next job fixed when the first is enqueued, instead of a rule beside the workflows.
- [Choreography](../events/choreography.md): services reacting to each other's events.
- [Idempotency keys](../jobs/idempotency.md)
- Examples: [examples/events-more](../../examples/events-more/), [examples/then-redo](../../examples/then-redo/)
