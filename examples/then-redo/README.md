# then-redo

How `workflow.Then` treats runs that redo work which already succeeded, on the in-memory driver. An import that succeeds starts its `index` job. Resetting the succeeded import (`ResetOptions.Succeeded`) and forking it give runs that carry `workflow.LabelRedo`; by default `Then` starts nothing for them, since the first success already started the index. With `workflow.ThenAfterRedo()` each redo starts the index again. A last part sends a failing rule's error to `workflow.ThenErrors` instead of the log.

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

## The code

Reset a succeeded run, which keeps its ref and gets `LabelRedo`:

<!-- snippet: examples/then-redo/main.go#redo -->
```go
// Run the import again although it succeeded: the run keeps its ref
// and gets LabelRedo.
if _, err := c.(workflow.RunResetterWith).ResetRunWith(ctx, ref, importStep,
	workflow.ResetOptions{Succeeded: true}); err != nil {
	return err
}
```

Fork it, which makes a new run that also carries `LabelRedo`:

<!-- snippet: examples/then-redo/main.go#fork -->
```go
// A fork of a succeeded run is a new run that also carries LabelRedo.
if _, err := c.(workflow.RunForker).ForkRun(ctx, ref, importStep, workflow.ForkOptions{}); err != nil {
	return err
}
```

Let the rules follow redo runs too:

<!-- snippet: examples/then-redo/main.go#after-redo -->
```go
follow, err := workflow.Then(indexAfterImport(), workflow.ThenAfterRedo()) // index must be idempotent
if err != nil {
	return err
}
```

Send the plugin's errors to your own function:

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

The guide is [docs/workflows/then.md](../../docs/workflows/then.md#redo-runs).
