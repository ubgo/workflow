# datasets

Data-aware scheduling and asset lineage with the `events` package, on the in-memory driver. Two loader workflows announce new dataset versions from their steps with `Datasets.Updated`; an audit trigger starts on every new orders version (any of), a revenue report trigger starts only once both orders and exchange rates have changed since its last run (all of). The orders loader's first attempt fails after its announcement, so its retry announces the same version again and starts nothing. The lineage store then names the step that produced each version and the versions the report's run read.

```sh
cd examples
go run ./datasets
```

```text
load-orders 2026-11-20T09:
  attempt 1 started: audit
  attempt 2 started: nothing
load-fx 2026-11-20T09:
  attempt 1 started: revenue
load-orders 2026-11-20T10:
  attempt 1 started: audit
  attempt 2 started: nothing
lineage:
  warehouse.orders 2026-11-20T10 produced by load-orders/write
  warehouse.orders 2026-11-20T09 produced by load-orders/write
  revenue-report run read warehouse.orders@2026-11-20T09, warehouse.fx_rates@2026-11-20T09
```

The second orders version starts the audit but not the report: exchange rates have not changed since the report's run.

## The code

Add the triggers:

<!-- snippet: examples/datasets/main.go#triggers -->
```go
err = errors.Join(
	ds.Trigger(events.DatasetTrigger{Name: "audit", Datasets: []string{orders}, Workflow: "orders-audit"}),
	ds.Trigger(events.DatasetTrigger{Name: "revenue", Datasets: []string{orders, fx}, All: true, Workflow: "revenue-report"}),
)
if err != nil {
	return err
}
```

Announce a version from a step:

<!-- snippet: examples/datasets/main.go#updated -->
```go
res, err := ds.Updated(ctx, dataset, string(in), map[string]string{"rows": "1200"})
if err != nil {
	return nil, err // retried: announcing the same version again is safe
}
```

Read the lineage:

<!-- snippet: examples/datasets/main.go#lineage -->
```go
producers, err := events.Producers(ctx, ds.Lineage(), orders)
if err != nil {
	return err
}
for _, p := range producers {
	fmt.Printf("  %s %s produced by %s/%s\n", p.Dataset, p.Version, p.Workflow, p.Step)
}
reads, err := events.Consumers(ctx, ds.Lineage(), fx)
if err != nil {
	return err
}
for _, r := range reads {
	inputs, err := events.Inputs(ctx, ds.Lineage(), r.Run)
	if err != nil {
		return err
	}
	var read []string
	for _, in := range inputs {
		read = append(read, in.Dataset+"@"+in.Version)
	}
	fmt.Printf("  %s run read %s\n", r.Workflow, strings.Join(read, ", "))
}
```

The guide that copies this program is [datasets and lineage](../../docs/events/datasets.md). `go test ./datasets` checks every line.
