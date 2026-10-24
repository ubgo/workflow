# Upstream rate limits

An outside API limits what you may call it, often as a leaky bucket counted in its own units: Shopify's REST API allows a bucket of 40 calls per store that drains 2 a second, and its GraphQL API counts the cost of each query against a bucket of 1000 that drains 50. `workflow.UpstreamLimit` tracks that bucket on your side, one per key (a store, a credential), so a step waits for room instead of collecting 429s. It counts what the upstream counts, which `Workflow.RateLimits` cannot: those count run starts, not the cost of each call a step makes.

## Before you start

- No driver capability is needed: the limit is called from inside a step, on any driver.
- Bucket levels live in a `workflow.BucketStore`. `workflow.MemoryBuckets` is in-process: each worker process keeps its own bucket, so several processes calling one upstream each believe they have the whole limit. The Redis driver ships a shared one, `redis.NewBucketStore(rdb, prefix)` or `Client.BucketStore()`. The SQL driver ships one too, `sqlstore.NewBucketStore(db, dialect)` or `Client.BucketStore()`, whose `Take` is atomic across processes (table `<prefix>buckets`). Any other is your own `BucketStore` ([below](#share-a-bucket-across-workers)).
- The example is [`examples/upstream-limits`](../../examples/upstream-limits). It runs on a fake clock, so the output never varies. Run it with `go run ./upstream-limits` from `examples/`.

## Build a limit

`workflow.NewUpstreamLimit(store, spec, clk)` takes the store (a new `MemoryBuckets` when nil), a `workflow.BucketSpec` and a clock (`clock.Real` when nil, a `clock.Fake` in tests):

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

A bucket holds at most `Capacity` units and drains `LeakPerSecond` units every second; a call of cost `c` fits when the level plus `c` is at most `Capacity`. Both must be positive and finite, or `NewUpstreamLimit` returns an error wrapping `workflow.ErrInvalid`.

## Take before each call

`Take(ctx, key, cost)` reserves `cost` units in `key`'s bucket and returns nil when they fit. When they do not, it reserves nothing and returns `workflow.ErrThrottled` marked with `workflow.RetryAfter` and the time until the cost fits:

<!-- snippet: examples/upstream-limits/main.go#take -->
```go
for i := 1; i <= 5; i++ {
	err := limit.Take(ctx, "shop:acme", 1) // one bucket per credential
	wait, _ := workflow.RetryAfterDelay(err, 0)
	fmt.Fprintf(out, "  call %d: throttled=%v retry after %s\n", i, errors.Is(err, workflow.ErrThrottled), wait)
}
```

Return that error from the step as it is. On a workflow whose `RetryPolicy.DelayFor` is `workflow.RetryAfterDelay`, the retry waits exactly until the bucket has room; under any other policy it retries on the normal backoff ([retries](retries.md#retry-after-a-delay-the-error-names)). A throttled call is "not yet" rather than a failure, so it spends an attempt; on a driver with `workflow.CapSnooze`, `TakeOrSnooze` returns `workflow.Snooze(wait)` instead, which puts the step back without spending one. On a driver without `CapSnooze` a snooze is an ordinary error and does spend an attempt, so use `Take` there.

`Wait(ctx, key, cost)` is `Take` without the error: it reserves the cost and returns zero, or reserves nothing and returns how long until it fits, for a step that would rather sleep than be retried. A negative cost, or one above the bucket's capacity, can never fit and is refused with a permanent `ErrInvalid` by all three.

## Follow the upstream's own count

Other apps on the same credential use the same upstream bucket, and your estimate drifts. `Observe(ctx, key, level)` sets the bucket's level to what the upstream reported, such as Shopify's `X-Shopify-Shop-Api-Call-Limit: 32/40` header or a GraphQL throttle status, so the next `Take` decides from the upstream's count:

<!-- snippet: examples/upstream-limits/main.go#observe -->
```go
// The response said "X-Shopify-Shop-Api-Call-Limit: 1/4": other apps
// on the credential use less than the bucket assumed.
if err := limit.Observe(ctx, "shop:acme", 1); err != nil {
	return err
}
wait, err := limit.Wait(ctx, "shop:acme", 3) // reserves 3 now, or reports the wait
```

`MemoryBuckets` and the Redis store clamp a reported level to between zero and the capacity.

## Share a bucket across workers

`workflow.BucketStore` has two methods: `Take(ctx, key, spec, cost, now)`, which adds the cost and returns zero when it fits or adds nothing and returns the wait, and `Observe(ctx, key, spec, level, now)`. A store applies the leak itself from the `now` it is given, must be safe for concurrent use, and must check and add in one atomic step. A store on a database or Redis that every worker shares makes them draw from one bucket, which is what a per-credential limit needs once more than one process calls the upstream. The Redis driver's `redis.BucketStore` is one: a hash per bucket key, with each take or observe done in one Lua script from the `now` the limit passes (never Redis's clock, so a fake clock still drives it), and an idle bucket's key expiring once it would have drained to empty, plus a minute.

## Try it

```sh
cd examples
go run ./upstream-limits
```

The bucket's part:

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
```

## Pitfalls

- **`MemoryBuckets` is per process.** Ten workers on it each spend the whole limit; use the Redis bucket store or another shared one, or give each process a fraction of the capacity.
- **Take before the call, Observe after it.** Reserving after the call lets a burst of steps overshoot; observing keeps the estimate honest.
- **A throttled `Take` spends an attempt.** Give the workflow a retry policy with room for throttling, or use `TakeOrSnooze` on a driver with `CapSnooze`.
- **Keys are yours.** One key per credential or store; a key per call or per run is a bucket that never fills.

## Related

- [Concurrency and rate limits](concurrency.md) for limits on run starts
- [Retries](retries.md), [Time](../concepts/time.md)
- [Idempotency keys › Once per period](idempotency.md#once-per-period), shown by the same example
- Example: [examples/upstream-limits](../../examples/upstream-limits/)
