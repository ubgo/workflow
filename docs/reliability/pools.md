# Pools, critical sections and auto-disable

Some resources take only so many users at once, whoever they are: a render box with two GPUs, a licence server with five seats, a partner API that allows three connections. Concurrency keys limit runs that share a key inside one workflow; a pool limits every step that names it, across workflows, workers and processes. A critical section is the same idea inside one run: two parallel steps that write the same row take turns. And when a downstream endpoint keeps failing, auto-disable stops calling it until someone turns it back on.

All three are step middleware over stores the library already has: pools and critical sections hold their slots in a `middleware.UniqueStore`, the same lock store `Unique` uses, and auto-disable keeps its switches in a `middleware.DisableStore`.

## Before you start

- A `UniqueStore` whose reach matches the limit: `middleware.NewMemoryUnique` limits one process; `sqlstore.UniqueStore` (SQLite, Postgres, MySQL) and the Redis `UniqueStore` limit every process that shares them.
- A step that finds no free slot snoozes, which needs `CapSnooze` (memory, sqlstore, Redis). On Hatchet use `PoolRetryAfter()`, which spends an attempt instead.
- Auto-disable runs on every driver; its discard ends a run `discarded` with `CapRunStates`.
- The runnable program for this page is [examples/job-guards](../../examples/job-guards/).

## Share a pool of slots

`middleware.Pool(store, name, slots, ttl, opts...)` lets at most `slots` steps hold the pool at once. Before the step runs it tries the slot keys `pool/<name>/0` to `pool/<name>/<slots-1>`; holding one, it runs the step and releases the slot afterwards, whatever the step returned. With every slot taken the step does not run: it snoozes for `PoolRetry` (one second by default) and tries again, without spending an attempt. `PoolSteps(names...)` limits the pool to the named steps; you can also wrap a single step's handler with it.

<!-- snippet: examples/job-guards/main.go#pool -->
```go
store := middleware.NewMemoryUnique(nil) // sqlstore.UniqueStore or the Redis one across processes
gpus := middleware.Pool(store, "gpu", 2, 10*time.Minute, middleware.PoolSteps("render"), middleware.PoolRetry(5*time.Millisecond))
mc := memory.New(memory.WithWorkers(6), memory.WithMiddleware(gpus))
```

Six renders on six workers run two at a time and all succeed.

## Keep two steps of one run apart

`middleware.CriticalSection(store, section, ttl, steps...)` is a pool of one slot keyed by the run's ID and the section name, so the named steps of one run never run at once, while the same steps of other runs are not held up:

<!-- snippet: examples/job-guards/main.go#section -->
```go
// debit and credit run in parallel, except that they both write the
// account row: within one run, one at a time.
ledger := middleware.CriticalSection(middleware.NewMemoryUnique(nil), "account-row", time.Minute, "debit", "credit")
mc := memory.New(memory.WithWorkers(4), memory.WithMiddleware(ledger))
```

## Switch off a failing endpoint

`middleware.AutoDisable(store, key, after, onDisable)` names the endpoint of each attempt with `key` (from its `StepInfo` and input) and counts its consecutive failures. After `after` failures in a row the key is disabled, `onDisable` runs once, and every later attempt for that key is discarded without running (an error matching `middleware.ErrEndpointDisabled`) until the key is enabled again. A success resets the count; snoozes and cancellations count as neither.

<!-- snippet: examples/job-guards/main.go#autodisable -->
```go
switches := middleware.NewMemoryDisable()
endpoint := func(_ workflow.StepInfo, in []byte) string { return string(in) } // the payload names the endpoint
off := middleware.AutoDisable(switches, endpoint, 3, func(_ context.Context, key string) {
	fmt.Println("  disabled", key, "- tell its owner") // runs once, when the switch trips
})
mc := memory.New(memory.WithMiddleware(off))
```

Once the owner has fixed the endpoint, turn it back on:

<!-- snippet: examples/job-guards/main.go#enable -->
```go
if err := switches.Enable(ctx, "https://hooks.example.com/a"); err != nil { // after the owner fixed it
	return err
}
```

Every failed attempt counts by default, which suits endpoint health: each attempt is a delivery the endpoint failed. `DisableCountFinal(policy)` counts only the failures that end a step (permanent, or the last attempt the policy allows). `MemoryDisable` keeps the switches in one process; implement `DisableStore` (four methods, with `Fail` atomic) over a shared table or Redis to switch an endpoint off for every worker.

Per-endpoint ordering and isolation come from the workflow, not from this middleware: `Concurrency{Key: endpoint, MaxRuns: 1, FIFO: true}` delivers one message per endpoint at a time and in order (FIFO needs `CapKeyFIFO`), so a slow endpoint holds back only its own deliveries.

## Pitfalls

- `ttl` is what frees a slot whose holder crashed. A step that runs longer than `ttl` loses its exclusivity: its slot expires and another step may take it while the first still runs. Set `ttl` above the step's timeout.
- Pool and section keys live in the same store namespace as `Unique` keys; do not use unique keys that start with `pool/` or `section/`.
- With `MemoryUnique` the limit holds inside one process only. Two worker processes each with their own memory store get `slots` each.
- `PoolRetryAfter` spends an attempt every time the pool is busy, so the step needs retries to spare, and the delay is honoured only under a policy whose `DelayFor` reads it (`workflow.RetryAfterDelay`).
- A store error fails the attempt (it is retried); the step never runs without a slot. A slot that could not be released frees itself after `ttl`.
- `AutoDisable` drops a store error while recording an outcome, so the switch may trip one failure late; an error reading the switch fails the attempt rather than calling an endpoint that may be disabled.
