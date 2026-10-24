# queue-limit

One cap on a queue for the whole cluster: `QueueConfig.Limit` bounds the step attempts of a queue running at once across every client that shares the store, where `Workers` bounds one client. The program runs nine one-step imports twice, on the in-memory driver (one process is the whole cluster) and on two SQL clients over one SQLite file (two nodes, four workers each, so eight attempts could run), with a limit of three. It counts the attempts running at once and prints whether the peak stayed within the limit and reached it.

```sh
cd examples
go run ./queue-limit
```

```text
memory: peak within limit 3: true
memory: peak reached the limit: true
sqlstore: queue limit is cluster-wide: true
sqlstore: peak within limit 3: true
sqlstore: peak reached the limit: true
```

## The code

The memory driver takes the limit on the queue, next to the per-client worker pool:

<!-- snippet: examples/queue-limit/main.go#memory -->
```go
c := memory.New(
	memory.WithWorkers(8),
	memory.WithQueues(workflow.QueueConfig{Name: "imports", Limit: importLimit}),
)
```

Two SQL nodes declare the same limit, so the database counts both nodes' attempts against it:

<!-- snippet: examples/queue-limit/main.go#sql -->
```go
// Two clients over one database: two nodes. Each has four workers, so
// eight attempts could run; Limit lets only three of the queue run at
// once across both.
newNode := func() (*sqlstore.Client, error) {
	db, err := driversqlite.Open(path)
	if err != nil {
		return nil, err
	}
	return sqlstore.New(db, sqlb.SQLiteDialect{},
		sqlstore.WithWorkers(4),
		sqlstore.WithPollInterval(50*time.Millisecond),
		sqlstore.WithQueues(workflow.QueueConfig{Name: "imports", Workers: 4, Limit: importLimit}),
	)
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Named queues: cap a queue across every worker](../../docs/jobs/queues.md#cap-a-queue-across-every-worker)
- [Capability reference: `CapQueueLimitGlobal`](../../docs/reference/capabilities.md#capability-matrix)
