# admin-bulk

The admin module's bulk actions and charts on the memory driver, with a fake clock so the chart's times are fixed: every failed run a filter matches previewed with a dry run and then retried, a delete without a filter refused, a delete bounded by `Max` and then done, and the run ends of ten minutes read from `admin.MemoryMetrics`. Nothing needs to be installed.

```sh
cd examples
go run ./admin-bulk
```

```text
dry run: would retry 5, sample 5, done 0
retried 5 of 5, skipped 0, more to do: false
delete without a filter refused: true
dry run: would delete 3 (Max 3), more match: true
deleted 7 of 7
09:00 succeeded=2 failed=5 total=7
09:05 succeeded=5 failed=0 total=5
```

## The code

MemoryMetrics, fed by the run-finished hook and wired into the Admin:

<!-- snippet: examples/admin-bulk/main.go#metrics -->
```go
mm := admin.NewMemoryMetrics(admin.MemoryMetricsConfig{Clock: clk}) // one-minute buckets, kept a day
if err := c.OnRunFinished(mm.Observe); err != nil {                 // before Start
	return err
}
adm := admin.New(c, admin.WithMetrics(mm), admin.WithClock(clk))
```

Retry every failed run a filter matches, after a dry run:

<!-- snippet: examples/admin-bulk/main.go#retry -->
```go
failed := workflow.RunQuery{Workflow: "send-email"} // RetryAll adds statuses=[failed]
preview, err := adm.RetryAll(ctx, failed, admin.BulkOptions{DryRun: true})
if err != nil {
	return err
}
fmt.Fprintf(out, "dry run: would retry %d, sample %d, done %d\n", preview.Matched, len(preview.Sample), preview.Done)

res, err := adm.RetryAll(ctx, failed, admin.BulkOptions{})
if err != nil {
	return err
}
fmt.Fprintf(out, "retried %d of %d, skipped %d, more to do: %t\n", res.Done, res.Matched, res.Skipped, res.Truncated)
```

Delete by filter: refused without one, bounded by Max, then done:

<!-- snippet: examples/admin-bulk/main.go#delete -->
```go
_, err = adm.DeleteAll(ctx, workflow.RunQuery{}, admin.BulkOptions{DryRun: true})
fmt.Fprintln(out, "delete without a filter refused:", errors.Is(err, workflow.ErrInvalid))

done := workflow.RunQuery{Workflow: "send-email", Statuses: []workflow.Status{workflow.StatusSucceeded}}
preview, err = adm.DeleteAll(ctx, done, admin.BulkOptions{DryRun: true, Max: 3})
if err != nil {
	return err
}
fmt.Fprintf(out, "dry run: would delete %d (Max 3), more match: %t\n", preview.Matched, preview.Truncated)

res, err = adm.DeleteAll(ctx, done, admin.BulkOptions{})
if err != nil {
	return err
}
fmt.Fprintf(out, "deleted %d of %d\n", res.Done, res.Matched)
```

Run ends in five-minute steps for a chart:

<!-- snippet: examples/admin-bulk/main.go#chart -->
```go
series, err := adm.Metrics(ctx, admin.MetricsQuery{
	From: start,
	To:   start.Add(10 * time.Minute),
	Step: 5 * time.Minute,
})
if err != nil {
	return err
}
for _, p := range series.Points {
	fmt.Fprintf(out, "%s succeeded=%d failed=%d total=%d\n", p.At.Format("15:04"), p.Succeeded, p.Failed, p.Total())
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Admin operations › Act on many runs at once](../../docs/admin/operations.md#act-on-many-runs-at-once)
- [Admin operations › Run-end history for charts](../../docs/admin/operations.md#run-end-history-for-charts)
- [Admin JSON API](../../docs/admin/http-api.md)
