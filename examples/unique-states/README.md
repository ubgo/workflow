# unique-states

The finer controls of unique jobs, on the in-memory driver with a fake clock: a lock that counts only while its run waits to start (`UniqueWith` with `States`), a key taken from the payload fields tagged `unique:"true"` (`ArgsTagged`), and a duplicate answered with the existing run instead of an error (`ReturnExisting`), told apart from a new run by `WithEnqueueOutcome`, which also reports an idempotency key's duplicate.

```sh
cd examples
go run ./unique-states
```

```text
unique while waiting to start:
  s-1 in an hour: accepted
  s-1 now, other cursor, first one scheduled: refused (ErrDuplicate)
  s-2 now: accepted
  started {"shop_id":"s-2","cursor":""}
  started {"shop_id":"s-1","cursor":"a"}
  s-1 again, first one running: accepted
  started {"shop_id":"s-1","cursor":"c"}
return the existing run:
  first: same run true, duplicate false (reported true)
  second: same run true, duplicate true (reported true)
  key first: duplicate false
  key second: duplicate true
```

## The code

The payload type, with the field that counts tagged:

<!-- snippet: examples/unique-states/main.go#tagged -->
```go
// SyncArgs is the payload of "sync-shop". Only the shop decides whether two
// syncs are the same job; the cursor does not.
type SyncArgs struct {
	ShopID string `json:"shop_id" unique:"true"`
	Cursor string `json:"cursor"`
}
```

Unique only while the holder is scheduled or queued:

<!-- snippet: examples/unique-states/main.go#states -->
```go
waiting, err := middleware.UniqueWith(middleware.UniqueConfig{
	By:     middleware.UniqueByArgs(middleware.ArgsTagged[SyncArgs]()),
	Store:  middleware.NewMemoryUnique(clk),
	TTL:    24 * time.Hour, // the safety net
	States: []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued},
	Client: mc, // asked for the holder's status
})
if err != nil {
	return err
}
c := middleware.WrapClient(mc, waiting)
```

A duplicate answered with the existing run:

<!-- snippet: examples/unique-states/main.go#return-existing -->
```go
existing, err := middleware.UniqueWith(middleware.UniqueConfig{
	By:             func(j workflow.Job) string { return "export:" + j.Labels["user"] },
	Store:          middleware.NewMemoryUnique(clk),
	TTL:            time.Hour,
	ReturnExisting: true, // a duplicate gets the holder's RunRef and no error
})
if err != nil {
	return err
}
exports := middleware.WrapClient(mc, existing)
job := workflow.Job{Workflow: "export", Labels: map[string]string{"user": "u-7"}}
var first workflow.RunRef
for _, try := range []string{"first", "second"} {
	ctx, outcome := workflow.WithEnqueueOutcome(ctx) // a fresh one per Enqueue
	ref, err := exports.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	if first.IsZero() {
		first = ref
	}
	fmt.Fprintf(out, "  %s: same run %t, duplicate %t (reported %t)\n", try, ref == first, outcome.Duplicate, outcome.Reported)
}
```

The same outcome for an idempotency key:

<!-- snippet: examples/unique-states/main.go#idempotency-outcome -->
```go
keyed := workflow.Job{Workflow: "export", IdempotencyKey: "export:u-8"}
for _, try := range []string{"first", "second"} {
	ctx, outcome := workflow.WithEnqueueOutcome(ctx)
	if _, err := mc.Enqueue(ctx, keyed); err != nil { // an idempotency key, no middleware
		return err
	}
	fmt.Fprintf(out, "  key %s: duplicate %t\n", try, outcome.Duplicate)
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Unique jobs: fields tagged on the payload type](../../docs/reliability/unique-jobs.md#fields-tagged-on-the-payload-type)
- [Unique jobs: unique while the run waits to start](../../docs/reliability/unique-jobs.md#unique-while-the-run-waits-to-start)
- [Unique jobs: return the existing run](../../docs/reliability/unique-jobs.md#return-the-existing-run)
