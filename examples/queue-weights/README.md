# queue-weights

Two ways to order work, on the in-memory driver: weighted queues, where one worker shares its turns between two queues of equal priority in proportion to their weights (the random source is seeded, so the draws repeat), and FIFO per concurrency key, where the runs of one account start one at a time in the order they were enqueued.

```sh
cd examples
go run ./queue-weights
```

```text
weighted (E = export, I = import):
  E1 E2 E3 E4 I1 E5 I2 E6
  I3 E7 E8 I4 I5 I6 I7 I8
fifo per key:
  started sync-1
  started sync-2
  started sync-3
```

## The code

Two queues of equal priority with weights 3 and 1:

<!-- snippet: examples/queue-weights/main.go#weights -->
```go
c := memory.New(
	memory.WithWorkers(1),
	memory.WithJitterSource(draw), // the weighted draw uses it; production leaves the default
	memory.WithQueues(
		workflow.QueueConfig{Name: "exports", Weight: 3}, // about three turns in four
		workflow.QueueConfig{Name: "imports", Weight: 1}, // still gets one in four
	),
)
```

One run per account at a time, in enqueue order:

<!-- snippet: examples/queue-weights/main.go#fifo -->
```go
wf := workflow.Workflow{
	Name: "sync-account",
	Concurrency: &workflow.Concurrency{
		Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["account"] }),
		MaxRuns: 1,    // one sync per account at a time
		FIFO:    true, // and in the order they were enqueued
	},
	Steps: []workflow.Step{{Name: "sync", Run: h}},
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Named queues: share a worker between queues by weight](../../docs/jobs/queues.md#share-a-worker-between-queues-by-weight)
- [Concurrency and rate limits: keep one key's runs in order](../../docs/jobs/concurrency.md#keep-one-keys-runs-in-order)
