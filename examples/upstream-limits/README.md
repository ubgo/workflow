# upstream-limits

`workflow.UpstreamLimit` is a leaky-bucket rate limit on the calls a step makes to an outside API, counted in the upstream's own cost units, one bucket per key; here it runs on a fake clock, so the output never varies. The second half shows `PeriodKey` and `OncePerPeriod`: a stable idempotency key for work that must happen once per customer per month, on the in-memory driver.

```sh
cd examples
go run ./upstream-limits
```

```text
bucket:
  call 1: throttled=false retry after 0s
  call 2: throttled=false retry after 0s
  call 3: throttled=false retry after 0s
  call 4: throttled=false retry after 0s
  call 5: throttled=true retry after 500ms
  after 500ms: <nil>
  another key has its own bucket: <nil>
  after Observe(1), a call of cost 3 waits 0s
  cost above capacity: ErrInvalid=true permanent=true
  TakeOrSnooze on a full bucket: snooze=true for 500ms
period:
  day     2026-11-01
  week    2026-W44
  month   2026-11
  quarter 2026-Q4
  year    2026
  time.Local refused: true
  key period/customer-42/2026-11
  second enqueue in the month returned the first run: true
  a job that already has a key is refused: true
```

## The code

A bucket of 4 that drains 2 units a second:

<!-- snippet: examples/upstream-limits/main.go#limit -->
```go
limit, err := workflow.NewUpstreamLimit(&workflow.MemoryBuckets{}, workflow.BucketSpec{
	Capacity:      4, // Shopify REST is 40; small here so the output stays short
	LeakPerSecond: 2,
}, clk)
if err != nil {
	return err
}
```

Five calls of cost 1: the fifth has no room and says when it will:

<!-- snippet: examples/upstream-limits/main.go#take -->
```go
for i := 1; i <= 5; i++ {
	err := limit.Take(ctx, "shop:acme", 1) // one bucket per credential
	wait, _ := workflow.RetryAfterDelay(err, 0)
	fmt.Fprintf(out, "  call %d: throttled=%v retry after %s\n", i, errors.Is(err, workflow.ErrThrottled), wait)
}
```

The upstream's own count, from a response header, replaces the bucket's estimate:

<!-- snippet: examples/upstream-limits/main.go#observe -->
```go
// The response said "X-Shopify-Shop-Api-Call-Limit: 1/4": other apps
// on the credential use less than the bucket assumed.
if err := limit.Observe(ctx, "shop:acme", 1); err != nil {
	return err
}
wait, err := limit.Wait(ctx, "shop:acme", 3) // reserves 3 now, or reports the wait
```

One key per customer per month:

<!-- snippet: examples/upstream-limits/main.go#period -->
```go
job, err := workflow.OncePerPeriod(workflow.Job{Workflow: "monthly-charge"}, "customer-42", workflow.PeriodMonth, at, berlin)
if err != nil {
	return err
}
first, err := c.Enqueue(ctx, job)
if err != nil {
	return err
}
second, err := c.Enqueue(ctx, job) // a second fire in the same month, from any node
if err != nil {
	return err
}
```

The guides that copy this program are [upstream rate limits](../../docs/jobs/upstream-limits.md) and [idempotency keys › Once per period](../../docs/jobs/idempotency.md#once-per-period). `go test ./upstream-limits` checks every line.
