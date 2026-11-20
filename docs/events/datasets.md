# Datasets and lineage

A revenue report should run when its input tables have new data, not at a guessed time after the loaders usually finish. And when a number in the report looks wrong, someone needs to know which load produced the table it read. `events.Datasets` covers both: code that writes a dataset announces the new version with `Updated`, triggers start the workflows that read it (when any one of their datasets changed, or once all of them have, as in Airflow's data-aware scheduling), and every version is recorded with the run and step that produced it and the runs that consumed it.

## Before you start

- A `Datasets` sits on an [`events.Bus`](publish-subscribe.md) and starts trigger runs on the bus's client, so deduplication rides on the client's idempotency keys: `events.New` refuses a client without `workflow.CapIdempotencyKey` unless you pass `events.AllowDuplicates()`. Every driver declares the capability.
- Triggers are code, registered at boot in every process that announces versions, as workflows and bus subscriptions are.
- Trigger state and lineage live in an `events.LineageStore`. The default, `events.MemoryLineage`, is in-process; processes that announce versions of the same datasets need one shared store (see [Share the store](#share-the-store-between-processes)).
- The example is [`examples/datasets`](../../examples/datasets). Run it with `go run ./datasets` from `examples/`.

## Start a workflow when a dataset changes

`events.NewDatasets(bus, opts...)` returns a `Datasets`; `Trigger` adds an `events.DatasetTrigger`:

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

A trigger watches the dataset names in `Datasets`. With `All` false it starts a run when any one of them has a version it has not used yet; with `All` true, once every one of them has a version it has not used yet, so a report that joins orders and exchange rates waits until both changed since its last run. A trigger's `Name` must be unique within the `Datasets` and stable across deploys: its consumed versions and every run's idempotency key are kept under it. `Trigger` returns an error wrapping `workflow.ErrInvalid` when the trigger has no `Name`, reuses one, watches no dataset or an empty or repeated name, or has neither `Workflow` nor `Job`.

A run's payload is the versions it consumes as a JSON object, `{"warehouse.fx_rates":"2026-11-20T09","warehouse.orders":"2026-11-20T09"}`. Set `Job` instead to build the job from that map (a payload of your own shape, a queue, a priority); a job it returns without a `Workflow` runs the trigger's. Every run carries `events.LabelDatasetTrigger`, the trigger's name, under the reserved internal prefix `ubgo.workflow/`, set by `Datasets` and allowed only on its own enqueue, so a public `Enqueue` cannot pass a run off as a trigger's; reserved labels in a job `Job` returns fail that trigger with `ErrInvalid`.

## Announce a new version

`Datasets.Updated(ctx, dataset, version, meta)` announces that a dataset now has a version, with optional metadata such as a row count or a location. Called from a step, it records the step's run as the producer:

<!-- snippet: examples/datasets/main.go#updated -->
```go
res, err := ds.Updated(ctx, dataset, string(in), map[string]string{"rows": "1200"})
if err != nil {
	return nil, err // retried: announcing the same version again is safe
}
```

It does three things in order: it records the version as produced (`events.LineageProduced`, with the run, workflow and step from `workflow.StepInfoFrom(ctx)`, all empty outside a step, stamped by the clock from `events.WithDatasetClock`, `clock.Real` by default); it publishes an `events.EventDatasetUpdated` message on the bus; and it starts every trigger watching the dataset whose condition now holds. The returned `events.UpdateResult` holds the produced record, the bus's `Published`, and one `events.TriggerRun` per trigger it started (the trigger's name, the versions, the run's `RunRef` and its error).

A version is opaque: the library only compares versions for equality, so give each update a unique one, such as the partition it loaded, a timestamp or a content hash. A trigger fires on the newest version recorded for each dataset, compared with the version it consumed last.

## Announce again safely

A step that announced a version and then failed is retried and announces the same version again. That starts nothing new:

- the produced record is the same fact (dataset, version, run and step), so the store ignores it;
- each trigger compares the newest version with the one it consumed, and a consumed version never starts it again, however often it is announced or how long ago its run finished;
- a trigger run's idempotency key is a hash of the trigger and the versions, so two processes deciding the same fire, or a retry after the run started but before its consumption was recorded, find the one unfinished run.

Call `Updated` again with the same version after any error: a failure to record the version returns at once, before anything is published, and publish and trigger failures are joined into the error and kept in the result, so the next call starts what failed.

## Subscribe on the bus

The message `Updated` publishes has the name `events.EventDatasetUpdated` (`dataset.updated`), the dataset's name as `Key`, the ID `events.DatasetEventID(dataset, version)` and an `events.DatasetUpdate` in JSON as its payload. An ordinary subscription with a `Filter` on `Key` reacts to one dataset, and with [`events.WithSignal()`](publish-subscribe.md#deliver-to-waiting-runs) on the bus a run waiting for `dataset.updated` with the dataset's name as key continues. Such a subscription is deduplicated by the bus, by event ID, only while its run is unfinished; use a trigger when a version must never start a workflow twice.

## Read the lineage

`Datasets.Lineage()` returns the store, and four functions query it, each newest first: `events.Producers(ctx, store, dataset)` (who produced the versions of a dataset), `events.Consumers(ctx, store, dataset)` (the runs triggers started for them), `events.Outputs(ctx, store, run)` (the versions a run produced) and `events.Inputs(ctx, store, run)` (the versions a trigger started a run for):

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

Each `events.Lineage` record has a `Kind` (`events.LineageProduced` or `events.LineageConsumed`), the `Dataset` and `Version`, the producer's `Meta`, the `Run`, `Workflow` and `Step` (no step on a consumed record; no run when the driver hands out only a `ScheduledID` for a delayed run), the consuming `Trigger` and the time `At`. `Outputs` and `Inputs` refuse an empty run ID with `ErrInvalid`.

## Share the store between processes

A trigger's decision is a function of the store alone: the newest produced version of each dataset against the version the trigger consumed last. Nothing is kept in the process, so a restart or another process sharing the store decides the same way. `events.MemoryLineage` is right for tests and a single process; it is lost on restart and grows by one record per new version and per trigger run. With several processes, implement `events.LineageStore` on your database and pass it with `events.WithLineage`. The interface has two methods:

- `Record(ctx, l)` saves a record, ignoring one equal to a saved record in `Kind`, `Dataset`, `Version`, `Run`, `Workflow`, `Step` and `Trigger` (the first one's `Meta` and `At` are kept);
- `Find(ctx, q)` returns the records matching an `events.LineageQuery` (`Kind`, `Dataset`, `Run`, `Trigger`, each empty to match any, and `Limit`, zero for all), newest first by `At`, ties in reverse order of recording.

A table with a unique index on the identifying columns and an index on (`kind`, `dataset`, `at`) serves both.

## Try it

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

The orders loader's retry announced its version again and started nothing, and the second orders version started the audit but not the report, because exchange rates had not changed since the report's run.

## Pitfalls

- **Each process keeps its own `MemoryLineage`.** Two processes with the default store each think a version is new and each start the trigger once; give them one shared store.
- **Versions are compared, not ordered.** A step that announces an older version after a newer one makes the older the newest recorded, and the trigger fires for it. Announce from the code that wrote the data, in the order it wrote it.
- **Two different versions at the same moment can both fire.** Two processes announcing a new orders version a moment apart may each start the trigger, one run per version: delivery is at least once per version, never twice for one version set.
- **A trigger fires on its first sight of a dataset.** A trigger added after versions were recorded treats the newest of each as unused, so an all-of trigger can fire on the first update after it is added.
- **A bus subscription is not a trigger.** It reacts to every announcement and is deduplicated only while its run is unfinished.

## Related

- [Publish and subscribe](publish-subscribe.md), [Inbound webhooks](webhooks.md)
- [Partitioned runs](../workflows/partitions.md), [Idempotency keys](../jobs/idempotency.md)
- Example: [examples/datasets](../../examples/datasets/)
