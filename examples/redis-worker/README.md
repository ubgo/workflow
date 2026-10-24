# redis-worker

Run jobs and a schedule on the Redis driver: what it declares and refuses (checked without a server), then, with `REDIS_URL` set, a web client that only enqueues, two workers sharing one prefix, one run per idempotency key, a retried step that sends once, and a schedule triggered by hand on another worker. Every key it writes is under a fresh prefix, deleted at the end.

```sh
cd examples
go run ./redis-worker
```

```text
durable: true cron: true overlap skip: true
closures: false false
REDIS_URL is not set: the live run is skipped
```

Against the shared local stack's Redis (start it with `task up:redis` in the stack's repository):

```sh
REDIS_URL=redis://localhost:7012/0 go run ./redis-worker
```

```text
durable: true cron: true overlap skip: true
closures: false false
same key, same run: true
send-email succeeded after 2 attempts, sent 1 time(s), errors kept 1
schedule nightly: last run succeeded, next fire 02:00 UTC
```

## The code

<!-- snippet: examples/redis-worker/main.go#client -->
```go
rdb := goredis.NewClient(opts) // the package is named redis too, hence goredis
defer rdb.Close()
prefix := "example-" + strconv.FormatInt(time.Now().UnixNano(), 36)
newWorker := func() (*redis.Client, error) {
	return redis.New(rdb,
		redis.WithPrefix(prefix), // clients that share a prefix share every run and schedule
		redis.WithPollInterval(50*time.Millisecond),
	)
}
```

<!-- snippet: examples/redis-worker/main.go#web -->
```go
web, err := redis.New(rdb, redis.WithPrefix(prefix), redis.WithEnqueueOnly())
if err != nil {
	return err
}
ref, err := web.Enqueue(ctx, workflow.Job{Workflow: "send-email", IdempotencyKey: "welcome:u-7"})
if err != nil {
	return err
}
again, err := web.Enqueue(ctx, workflow.Job{Workflow: "send-email", IdempotencyKey: "welcome:u-7"})
```

<!-- snippet: examples/redis-worker/main.go#trigger -->
```go
id, err := workers[0].Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "report", Cron: "0 2 * * *", Location: time.UTC, Overlap: workflow.OverlapSkip})
if err != nil {
	return err
}
run, err := workers[1].TriggerSchedule(ctx, id) // any client of the prefix
if err != nil {
	return err
}
```

<!-- snippet: examples/redis-worker/main.go#refused -->
```go
// A closure is a Go value that cannot be stored in Redis, so the
// driver does not declare CapClosure and has no EnqueueFunc.
_, isClosure := any(c).(workflow.ClosureEnqueuer)
fmt.Fprintln(out, "closures:", caps.Has(workflow.CapClosure), isClosure)
```

`main_test.go` runs the program and checks the offline lines, and the live ones when `REDIS_URL` is set.

## Guides

- [The Redis driver](../../docs/drivers/redis.md)
