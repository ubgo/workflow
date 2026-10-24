# run-repair

What an operator has to fix a run by hand: the error of every failed attempt kept on the run (with a stack for a panic caught by `middleware.Recover`), a failed run marked succeeded after the work was done by hand, and a stuck run marked failed with a reason, without a retry and without its `OnFailure` handler. It runs on the in-memory driver, so nothing needs to be installed.

```sh
cd examples
go run ./run-repair
```

```text
-- error history
  attempt 1 of "send": smtp 451: try again later (stack: false)
  attempt 2 of "send": smtp 451: try again later (stack: false)
  attempt 3 of "send": workflow/middleware: step panicked: nil template (stack: true)
-- force complete a failed run
  send-email: succeeded
-- force fail a stuck run
  import-feed: failed, "workflow: run ended by an operator: feed provider is down until Monday", OnFailure ran 0 times
-- a finished run cannot be forced
  ErrWrongState: true
```

## The code

Keep a bounded history of failed attempts, with stacks for panics:

<!-- snippet: examples/run-repair/main.go#client -->
```go
c := memory.New(
	memory.WithErrorHistory(10), // keep the newest 10 failed attempts per run; 0 turns it off
	memory.WithMiddleware(middleware.Recover(slog.New(slog.DiscardHandler))),
)
```

<!-- snippet: examples/run-repair/main.go#history -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
if err != nil {
	return err
}
for _, e := range page.Runs[0].Errors { // oldest first
	fmt.Fprintf(out, "  attempt %d of %q: %s (stack: %t)\n", e.Attempt, e.Step, e.Error, e.Stack != "")
}
```

Mark a failed run done, and a stuck run failed:

<!-- snippet: examples/run-repair/main.go#complete -->
```go
// The email went out by hand: record the run as done.
if err := c.ForceComplete(ctx, ref); err != nil {
	return err
}
```

<!-- snippet: examples/run-repair/main.go#fail -->
```go
err = c.ForceFail(ctx, stuck, "feed provider is down until Monday")
if err != nil {
	return err
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Manage runs › Error history](../../docs/jobs/manage-runs.md#error-history)
- [Manage runs › Force a run to an end](../../docs/jobs/manage-runs.md#force-a-run-to-an-end)
