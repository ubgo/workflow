# Run outcome

A run's status says whether the worker ran it to the end; its outcome says whether the work succeeded. A sync that skipped because nothing changed, and one that finished with 3 bad rows out of 100, are both `succeeded`; their outcomes, `skipped` and `partial`, tell them apart. A step sets the outcome with `workflow.SetOutcome`, and a [run recorder](../observability/run-recorder.md) keeps it with the run, so a runs page can filter "succeeded, but partially" without parsing logs.

## Before you start

- You know [run states](../jobs/run-states.md): the outcome sits beside the status, it never changes it.
- The runnable program for this page is [examples/step-events](../../examples/step-events/), the same program as [Step events](step-events.md).

## Step 1: pick an outcome

`workflow.Outcome` has four values; `workflow.Outcomes()` returns them as a fresh slice and `Outcome.Valid()` checks one:

| Outcome | Constant | Use it when |
|---|---|---|
| `success` | `workflow.OutcomeSuccess` | the work was done in full |
| `partial` | `workflow.OutcomePartial` | some of the work was done and some was not (bad rows, a page that failed); the run still succeeded |
| `error` | `workflow.OutcomeError` | the work was not done |
| `skipped` | `workflow.OutcomeSkipped` | there was nothing to do (no changes since the last sync, a feature turned off) |

Code that switches on an outcome keeps a default branch: a later version may add one.

## Step 2: set it from a step

`workflow.SetOutcome(ctx, outcome, note)` records the run's outcome with an optional note for a person, at most `workflow.MaxOutcomeNote` (1 KiB). Any step of the run may call it, more than once; the last call in the run wins:

<!-- snippet: examples/step-events/main.go#step -->
```go
return func(ctx context.Context, _ []byte) ([]byte, error) {
	workflow.RecordEvent(ctx, "fetched changes", slog.Int("rows", 100))
	workflow.RecordEventLevel(ctx, slog.LevelWarn, "rows rejected", slog.Int("bad", 3), slog.String("first", "row-17"))
	err := workflow.SetOutcome(ctx, workflow.OutcomePartial, "3 of 100 rows rejected")
	fmt.Fprintln(out, "outcome kept:", err == nil, "unsupported:", errors.Is(err, workflow.ErrUnsupported))
	return nil, nil
}
```

`SetOutcome` returns:

- an error wrapping `workflow.ErrInvalid` for an outcome that is not one of the four, or a note over 1 KiB, checked first;
- an `UnsupportedError` (matching `workflow.ErrUnsupported`) when nothing on the context keeps an outcome: no run recorder is applied, or the context is not a step's. The caller asked for data to be kept, so dropping it silently would be wrong, as with `ReportProgress`. A step that sets an outcome whether or not one is kept ignores the error;
- the store's error when a recorder is applied and its write failed. Unlike the recorder's other writes, which never reach the step, this one does.

The example runs the step twice: on a plain memory client, where `SetOutcome` is unsupported, and on one with a recorder, where it is kept.

## Step 3: let the recorder keep it

A run recorder binds an outcome setter on every step's context, so `SetOutcome` reaches its store. Apply it with `workflow.Use` and register through the client it returns:

<!-- snippet: examples/step-events/main.go#recorder -->
```go
store, err := recorder.NewMemoryStore() // in process; sqlrecorder for a database
if err != nil {
	return err
}
rec, err := recorder.New(store)
if err != nil {
	return err
}
base := memory.New()
defer base.Stop()
c, err := workflow.Use(base, rec.Plugin())
if err != nil {
	return err
}
```

A run that set no outcome gets `workflow.DefaultOutcome(status)` when it ends: `success` for `succeeded`, `error` for `failed` and `discarded`, and none for `cancelled` or an unfinished run. `RunRecord.OutcomeSet` tells a set outcome from a default one, and `RunRecord.OutcomeNote` holds the note:

<!-- snippet: examples/step-events/main.go#read -->
```go
r, err := store.Run(ctx, syncRef)
if err != nil {
	return err
}
fmt.Fprintf(out, "run %s outcome=%s (%s)\n", r.Status, r.Outcome, r.OutcomeNote)
for _, s := range r.Steps {
	for _, e := range s.Events {
		fmt.Fprintf(out, "  %s %s %q %v\n", s.Step, e.Level, e.Message, e.Attrs)
	}
}
```

To list runs by outcome, pass `recorder.Query{Outcomes: []workflow.Outcome{workflow.OutcomePartial}}` to the store's `Records` ([Record run history › Read the history](../observability/run-recorder.md#read-the-history)).

## Try it

```sh
cd examples
go run ./step-events
```

The outcome lines of the output:

```text
outcome kept: false unsupported: true
outcome kept: true unsupported: false
run succeeded outcome=partial (3 of 100 rows rejected)
cleanup succeeded outcome=success (set by a step: false)
```

The `cleanup` workflow sets nothing, so its outcome is the default for `succeeded`. The whole output is on [Step events › Try it](step-events.md#try-it).

## Pitfalls

- **The outcome never changes the status.** `SetOutcome(ctx, OutcomeError, ...)` in a step that then returns nil leaves the run `succeeded`, with outcome `error`; return an error to fail the run.
- **A set outcome survives a failure.** A step that set `partial` before a later step failed leaves the run `failed` with outcome `partial`, because a set outcome wins over the default. Set `error` in that path when it matters.
- **Retries keep the last set value.** An attempt that set an outcome and then failed is not rolled back; the retry overrides it only by setting another.
- **Without a recorder there is nowhere to keep it.** Drivers store no outcome, and `RunInfo` has no field for one; read it from the recorder's store.

## Related

- [Step events](step-events.md) and [Trigger source](trigger-source.md): the other run metadata a recorder keeps.
- [Record run history](../observability/run-recorder.md): the recorder and its filters.
- [Run states](../jobs/run-states.md): the statuses an outcome sits beside.
- [Progress](../jobs/progress.md): `ReportProgress`, which refuses the same way when nothing keeps it.
- Example: [step-events](../../examples/step-events/).
