# run-reschedule

Operations on runs that have not finished, on the in-memory driver with a fake clock: listing the delayed runs due in the next hour with `RunQuery.DueFrom` and `DueTo`, moving one with `Reschedule` (same ref, new time), and withdrawing one running step with `CancelStep`: a step that falls back to cached data lets its run continue, a step that gives up fails its run.

```sh
cd examples
go run ./run-reschedule
```

```text
due in the next hour:
  r-30m due at +30m0s, scheduled
rescheduled r-2h to +45m, same ref true
due in the next hour:
  r-2h due at +45m0s, scheduled
  r-30m due at +30m0s, scheduled
after an hour: r-2h succeeded, r-30m succeeded
reschedule a finished run: ErrWrongState true
cancel one step:
  store got "yesterday's prices"
  update-prices succeeded
  sync-feed failed, ErrStepCancelled true
  cancel it again: ErrWrongState true
```

## The code

The runs due in the next hour:

<!-- snippet: examples/run-reschedule/main.go#due -->
```go
now := clk.Now()
page, err := c.Runs(ctx, workflow.RunQuery{
	DueFrom: now,
	DueTo:   now.Add(time.Hour), // exclusive
})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(out, "  %s due at +%s, %s\n", r.Labels["name"], r.RunAt.Sub(now), r.Status)
}
```

Moving a delayed run:

<!-- snippet: examples/run-reschedule/main.go#reschedule -->
```go
moved, err := c.Reschedule(ctx, refs["r-2h"], clk.Now().Add(45*time.Minute))
if err != nil {
	return err
}
```

Withdrawing one step:

<!-- snippet: examples/run-reschedule/main.go#cancel-step -->
```go
if err := c.CancelStep(ctx, ref, "fetch"); err != nil {
	return err
}
```

A step that falls back when it is withdrawn:

<!-- snippet: examples/run-reschedule/main.go#fallback -->
```go
fetchPrices := func(ctx context.Context, _ []byte) ([]byte, error) {
	started <- "fetch"
	<-ctx.Done() // the price feed is hanging
	if errors.Is(context.Cause(ctx), workflow.ErrStepCancelled) {
		return []byte(`"yesterday's prices"`), nil // an operator withdrew the step: fall back
	}
	return nil, context.Cause(ctx)
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Manage runs: list the runs due soon](../../docs/jobs/manage-runs.md#list-the-runs-due-soon)
- [Manage runs: reschedule a delayed run](../../docs/jobs/manage-runs.md#reschedule-a-delayed-run)
- [Manage runs: cancel one step](../../docs/jobs/manage-runs.md#cancel-one-step)
