# enqueue-only

Split web nodes from worker nodes on the SQL driver: a web client built with `sqlstore.WithEnqueueOnly()` enqueues and reads status without `Start` and runs nothing; worker clients over the same tables run the work; and a step cut short by a clean stop of one worker is finished by another on the same attempt, without spending a retry. It runs on a SQLite file in a temporary directory, so nothing needs to be installed.

```sh
cd examples
go run ./enqueue-only
```

```text
web enqueued true status queued
worker A runs attempt 1
worker A stopped
worker B runs attempt 1
run succeeded, attempts counted 1, errors recorded 0
```

## The code

The web node:

<!-- snippet: examples/enqueue-only/main.go#web -->
```go
// A web node: it enqueues and reads status, and never runs a step.
// No Start, no worker goroutines, no cron.
web, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithEnqueueOnly())
if err != nil {
	return err
}
if err := web.Migrate(ctx); err != nil { // usually your deploy step, not the web node
	return err
}
ref, err := web.Enqueue(ctx, workflow.Job{Workflow: "thumbnail", Payload: []byte(`"img-1"`)})
if err != nil {
	return err
}
```

A worker node:

<!-- snippet: examples/enqueue-only/main.go#worker -->
```go
c, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(pollInterval))
if err != nil {
	return nil, nil, err
}
if err := c.Register(wf); err != nil {
	return nil, nil, err
}
wctx, cancel := context.WithCancel(ctx)
if err := c.Start(wctx); err != nil {
	cancel()
	return nil, nil, err
}
```

A clean stop:

<!-- snippet: examples/enqueue-only/main.go#shutdown -->
```go
// SIGTERM on worker A: cancel its Start context and wait for Stop.
// The step it was running is put back without spending an attempt.
stopA()
workerA.Stop()
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [Deploy workers › Web nodes that only enqueue](../../docs/operations/deploy.md#web-nodes-that-only-enqueue)
- [Deploy workers › Shut down cleanly](../../docs/operations/deploy.md#shut-down-cleanly)
