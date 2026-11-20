# partitioned-runs

One logical job split into partitions, on the in-memory driver. A backfill runs a workflow once per day with `workflow.EnqueuePartitions`, `workflow.PartitionStatus` shows the latest run of each day, `workflow.RetryFailedPartitions` starts again only the day that failed, and a per-tenant set enqueued twice while its runs are unfinished gets the same runs back.

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

## The code

Start one run per day:

<!-- snippet: examples/partitioned-runs/main.go#enqueue -->
```go
set := workflow.PartitionSet{Name: "totals-backfill", Workflow: "daily-totals"}
refs, err := workflow.EnqueuePartitions(ctx, c, set, []string{"2026-11-01", "2026-11-02", "2026-11-03"})
if err != nil {
	return err
}
```

Read the status of every partition:

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

Start again only the partitions that failed:

<!-- snippet: examples/partitioned-runs/main.go#retry -->
```go
keys, refs, err := workflow.RetryFailedPartitions(ctx, c, set)
if err != nil {
	return err
}
fmt.Printf("  retried: %v\n", keys)
```

Enqueue a set again while its runs are unfinished and get the same runs back:

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

The guide that copies this program is [partitioned runs](../../docs/workflows/partitions.md). `go test ./partitioned-runs` checks every line.
