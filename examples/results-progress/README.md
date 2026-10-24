# results-progress

A long export that reports how far it has got and returns a result the caller waits for, on the in-memory driver with a fake clock: `ReportProgress` from inside the step, progress events and `RunInfo.Progress` outside it, `AwaitResult` and `DecodePayload` for the result, the result of a workflow with two final steps, a run that fails while someone waits on it, and runs filtered by tenant, one of them fired by a tenant's schedule.

```sh
cd examples
go run ./results-progress
```

```text
progress:
   33% exported 1000 of 3000 orders (step export, at 09:00)
   67% exported 2000 of 3000 orders (step export, at 09:00)
  100% exported 3000 of 3000 orders (step export, at 09:00)
  latest: 100% exported 3000 of 3000 orders
  outside a step: unsupported true
result:
  {"file":"orders.csv","rows":3000}
  decoded: orders.csv, 3000 rows
  checkout, final steps [email invoice]: {"email":"sent","invoice":"inv_7"}
  pay: ended failed: memory: step "charge" failed after 1 attempt(s): card declined
  Result of a failed run: ErrWrongState true
tenants:
  acme checkout succeeded (fired by acme-nightly)
  acme checkout succeeded
  acme export-orders succeeded
  tenant with a space: ErrInvalid true
```

## The code

A step that reports progress and returns a typed result:

<!-- snippet: examples/results-progress/main.go#report -->
```go
export := workflow.Define(func(ctx context.Context, _ struct{}) (ExportResult, error) {
	const total = 3000
	for done := 1000; done <= total; done += 1000 {
		// ... export the next 1,000 orders ...
		err := workflow.ReportProgress(ctx, workflow.Progress{
			Percent: 100 * float64(done) / total,
			Message: fmt.Sprintf("exported %d of %d orders", done, total),
		})
		if err != nil && !errors.Is(err, workflow.ErrUnsupported) {
			return ExportResult{}, err // the run ended under us
		}
	}
	return ExportResult{File: "orders.csv", Rows: total}, nil
})
```

Progress events for a live page:

<!-- snippet: examples/results-progress/main.go#events -->
```go
progress := make(chan workflow.Progress, 8)
err := c.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) {
	if ev.Kind == workflow.LifecycleProgress {
		progress <- *ev.Progress // a page could push this to the browser
	}
})
```

Waiting for the result and decoding it:

<!-- snippet: examples/results-progress/main.go#await -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export-orders", Tenant: "acme"})
if err != nil {
	return err
}
ctx, cancel := context.WithTimeout(ctx, time.Minute) // how long the caller will wait
defer cancel()
raw, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 10 * time.Millisecond})
if err != nil {
	return err
}
res, err := workflow.DecodePayload[ExportResult](raw)
if err != nil {
	return err
}
```

The latest progress from a run listing:

<!-- snippet: examples/results-progress/main.go#latest -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
if err != nil {
	return err
}
if p := page.Runs[0].Progress; p != nil { // nil until a step reports
	fmt.Fprintf(out, "  latest: %.0f%% %s\n", p.Percent, p.Message)
}
```

A workflow with two final steps, whose result is a JSON object of both:

<!-- snippet: examples/results-progress/main.go#sinks -->
```go
checkout := workflow.Workflow{
	Name: "checkout",
	Steps: []workflow.Step{
		{Name: "charge", Run: ok(`"ch_1"`)},
		{Name: "email", After: []string{"charge"}, Run: ok(`"sent"`)},    // a final step
		{Name: "invoice", After: []string{"charge"}, Run: ok(`"inv_7"`)}, // a final step
	},
}
```

A run that fails while the caller waits:

<!-- snippet: examples/results-progress/main.go#failed -->
```go
_, err = workflow.AwaitResult(ctx, c, ref3, workflow.AwaitOptions{Poll: 10 * time.Millisecond})
var failed *workflow.RunFailedError
if errors.As(err, &failed) {
	fmt.Fprintf(out, "  pay: ended %s: %s\n", failed.Status, failed.Err)
}
```

Jobs for two tenants:

<!-- snippet: examples/results-progress/main.go#tenant -->
```go
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "checkout", Tenant: tenant})
if err != nil {
	return err
}
```

A schedule whose runs belong to a tenant:

<!-- snippet: examples/results-progress/main.go#tenant-schedule -->
```go
id, err := c.Schedule(ctx, workflow.Schedule{
	Name:     "acme-nightly",
	Workflow: "checkout",
	Cron:     "0 2 * * *",
	Location: time.UTC,
	Tenant:   "acme", // every run it fires belongs to acme
	Labels:   map[string]string{"source": "schedule"},
})
if err != nil {
	return err
}
```

One tenant's runs:

<!-- snippet: examples/results-progress/main.go#tenant-query -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Tenant: "acme"})
if err != nil {
	return err
}
for _, r := range page.Runs {
	fmt.Fprintf(out, "  %s %s %s", r.Tenant, r.Workflow, r.Status)
	if r.Labels["source"] == "schedule" {
		fmt.Fprint(out, " (fired by acme-nightly)")
	}
	fmt.Fprintln(out)
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Progress and heartbeats](../../docs/jobs/progress.md)
- [Results and awaiting a run](../../docs/jobs/results.md)
- [Tenants](../../docs/jobs/tenants.md)
