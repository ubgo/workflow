# Partitioned runs

A backfill of last month's totals is one job made of thirty days; a nightly sync is one job made of a run per tenant. Started as one big run, a single bad day fails the whole month and a re-run repeats every day that already worked. Started as loose runs, nothing ties them together and no view shows which day failed. A partition set is the middle way: one run of a workflow per partition key (a day, a tenant), each with its own retries and its own status, tied together by the set's name so one call lists the state of every partition and another starts again only the ones that failed.

## Before you start

- `workflow.EnqueuePartitions` gives each run an idempotency key, so it needs `workflow.CapIdempotencyKey`, which every driver declares; a client without it is refused with an `*workflow.UnsupportedError` unless the set has `AllowDuplicates: true`.
- `workflow.PartitionStatus` and `workflow.RetryFailedPartitions` read the run listing, so they need `workflow.CapRunQuery` and a client implementing `workflow.RunQuerier`; without them they refuse with an `*workflow.UnsupportedError` (field `PartitionStatus`, missing `CapRunQuery`).
- The example is [`examples/partitioned-runs`](../../examples/partitioned-runs). Run it with `go run ./partitioned-runs` from `examples/`.

## Start one run per partition

A `workflow.PartitionSet` names the set and the workflow every partition runs; `EnqueuePartitions(ctx, client, set, keys)` starts one run per key and returns their `RunRef`s in the order of `keys`:

<!-- snippet: examples/partitioned-runs/main.go#enqueue -->
```go
set := workflow.PartitionSet{Name: "totals-backfill", Workflow: "daily-totals"}
refs, err := workflow.EnqueuePartitions(ctx, c, set, []string{"2026-11-01", "2026-11-02", "2026-11-03"})
if err != nil {
	return err
}
```

Each run's payload is its key as a JSON string, so the step reads `"2026-11-01"`. Set `PartitionSet.Job` instead to build each job from its key (a payload of your own shape, a queue, a tenant, a priority); a job it returns without a `Workflow` runs the set's. Inside a step, `workflow.PartitionOf(ctx)` returns the run's set and key whatever the payload.

The set's `Name` must be stable across deploys and unique among the sets on one client: it is part of every run's idempotency key and the label the status is read by. A set without a `Name`, with neither `Workflow` nor `Job`, or with an empty or repeated key is refused with `workflow.ErrInvalid` before anything is enqueued. Every key is tried even when one fails to start: the error joins each failure, naming its key, and that key's `RunRef` is zero.

Every run carries two labels under the reserved internal prefix `ubgo.workflow/`: `workflow.LabelPartitionSet`, the set's name, and `workflow.LabelPartitionKey`, the key. `EnqueuePartitions` sets them itself, overwriting any value the job carried, and allows only these two keys on its own `Enqueue`, so on a driver that enforces `workflow.CheckJobLabels` a public `Enqueue` cannot forge a partition run and skew the set's status; like every reserved label they are a guard, not a sandbox. Reserved labels in a job `PartitionSet.Job` returns fail that key with `ErrInvalid`.

## Run a backfill again safely

Each run's idempotency key is `workflow.PartitionKey(workflow, set, key)`: `partition/<workflow>/<set>/<key>`, each part path-escaped. Enqueueing the set again, after a crash halfway through or a timeout, returns the runs of partitions still unfinished instead of starting them twice, and starts the partitions that never started:

<!-- snippet: examples/partitioned-runs/main.go#again -->
```go
tenants := workflow.PartitionSet{Name: "nightly-sync", Workflow: "tenant-sync"}
first, err := workflow.EnqueuePartitions(ctx, c, tenants, []string{"acme", "globex"})
if err != nil {
	return err
}
again, err := workflow.EnqueuePartitions(ctx, c, tenants, []string{"acme", "globex"})
if err != nil {
	return err
}
fmt.Printf("  same runs: %v\n", first[0] == again[0] && first[1] == again[1])
```

An idempotency key guards only while its run is unfinished, so a partition whose run has ended, failed or succeeded, starts afresh when it is enqueued again. To fill only the gaps of a set, compare `PartitionStatus` with the keys you expect and enqueue the missing ones. A job that `PartitionSet.Job` gives its own `IdempotencyKey` keeps it.

## Read the status of every partition

`workflow.PartitionStatus(ctx, client, set)` reads every page of the run listing filtered by `LabelPartitionSet` and returns the newest run of each partition as a `workflow.PartitionRun` (`Key` and the run's `workflow.RunInfo`), sorted by key:

<!-- snippet: examples/partitioned-runs/main.go#status -->
```go
parts, err := workflow.PartitionStatus(ctx, c, set)
if err != nil {
	return err
}
for _, p := range parts {
	line := fmt.Sprintf("  %s %s", p.Key, p.Run.Status)
	if p.Run.Error != "" {
		line += ": " + p.Run.Error
	}
	fmt.Println(line)
}
```

`Run.Ref`, `Run.Status` and `Run.Error` are the partition's state; the error text is the driver's. A partition never enqueued is missing, and so is one whose runs the driver's [retention](../operations/retention.md) already removed.

## Retry only the failed partitions

`workflow.RetryFailedPartitions(ctx, client, set)` starts a new run for every partition whose newest run ended `StatusFailed`, and returns those keys, sorted, with their new `RunRef`s:

<!-- snippet: examples/partitioned-runs/main.go#retry -->
```go
keys, refs, err := workflow.RetryFailedPartitions(ctx, c, set)
if err != nil {
	return err
}
fmt.Printf("  retried: %v\n", keys)
```

Partitions that succeeded, are still running, were cancelled or were discarded are left alone: a cancel or a discard is a decision, not a failure. The new run is a fresh run of the whole workflow under the same idempotency key (free again, since the failed run ended), so it works on every driver. To resume a failed partition from its failed step instead, on a driver with `workflow.CapRetryRun`, pass the `RunRef` from `PartitionStatus` to `RunRetrier.Retry` ([manage runs](../jobs/manage-runs.md)).

## Try it

```sh
cd examples
go run ./partitioned-runs
```

```text
backfill:
  2026-11-01 succeeded
  2026-11-02 failed: memory: step "load" failed after 1 attempt(s): source down for 2026-11-02
  2026-11-03 succeeded
retry failed:
  retried: [2026-11-02]
  2026-11-01 succeeded
  2026-11-02 succeeded
  2026-11-03 succeeded
per tenant, enqueued twice while running:
  same runs: true
  acme succeeded
  globex succeeded
```

## Pitfalls

- **A set name is an identity.** Two backfills under one name share their status and their idempotency keys; name each one (`totals-backfill-2026-11`).
- **A finished partition starts again when enqueued again.** The idempotency key only stops a second run while the first is unfinished; use `RetryFailedPartitions` or the gaps of `PartitionStatus` to re-run a part of a set.
- **The workflow's idempotency policy applies.** A workflow with `Idempotency: workflow.IdempotencyOff` ignores the keys, so enqueueing a set again starts every partition again.
- **Status is as old as the run listing keeps.** With retention, the status of an old set thins out as its runs are removed.
- **Large sets read every page.** `PartitionStatus` reads the whole listing of the set, newest first, a thousand runs per page; keep a set to the partitions of one job.

## Related

- [Child runs](children.md), [Batches](../batches/batches.md)
- [Idempotency keys](../jobs/idempotency.md), [Manage runs](../jobs/manage-runs.md), [Tenants](../jobs/tenants.md)
- Example: [examples/partitioned-runs](../../examples/partitioned-runs/)
