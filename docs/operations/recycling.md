# Worker recycling

A long-lived worker can drift: a library that leaks, a cache that only grows, a connection pool that went bad. Recycling is the old, dull fix: after a number of jobs, after a length of time, or once the heap passes a size, the worker stops taking work, lets what it is running finish, and exits, so the supervisor that runs it (systemd, Kubernetes, a process manager) starts a fresh one. In Go this is rarely needed, but when a dependency misbehaves it is cheaper than finding the leak today.

## Before you start

- Recycling is a driver option: `memory.WithRecycle`, `sqlstore.WithRecycle`, or `Config.Recycle` on the Hatchet driver, each taking a `workflow.Recycle`. The zero value never recycles.
- The process needs a supervisor that restarts it. Recycling stops the worker; it never starts one.
- The example is [`examples/worker-recycling`](../../examples/worker-recycling). Run it with `go run ./worker-recycling` from `examples/`.

## Step 1: set the limits

| Field | Stops the worker | Like |
|---|---|---|
| `MaxJobs` | After this many finished step attempts by this client, whatever their outcome | `--max-jobs` |
| `MaxTime` | This long after `Start`, on the driver's clock | `--max-time` |
| `MaxMemory` | Once `Memory()` reads at least this many bytes, checked after each attempt and every `CheckEvery` | `--memory` |
| `Memory` | The reading; nil means `workflow.HeapObjectBytes` (the Go heap's live objects, from `runtime/metrics`). Pass your own to read RSS or a cgroup counter | |
| `CheckEvery` | How often memory is checked between attempts; zero means `workflow.DefaultRecycleCheck` (one second) | |

Any field left at zero is no limit. A negative limit is refused at `Start` with `workflow.ErrInvalid` (`workflow.ValidateRecycle`).

<!-- snippet: examples/worker-recycling/main.go#max-memory -->
```go
c := memory.New(memory.WithClock(clk), memory.WithRecycle(workflow.Recycle{
	MaxMemory:  512 << 20,                            // like --memory=512
	Memory:     func() uint64 { return heap.Load() }, // nil reads the Go heap
	CheckEvery: 5 * time.Second,                      // between jobs too
	MaxTime:    time.Hour,                            // like --max-time=3600
}))
```

## Step 2: serve until the worker stops

`Start` returns once the workers are up, so recycling is not reported through it. `workflow.Serve(ctx, client)` starts the client and blocks until it has stopped, then returns why: Start's error, a `*workflow.RecycleError` when a limit stopped it, or nil for every stop you asked for (the context ending, `Drain`, stop-when-idle):

<!-- snippet: examples/worker-recycling/main.go#max-jobs -->
```go
c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
	sqlstore.WithWorkers(1),
	sqlstore.WithPollInterval(20*time.Millisecond),
	sqlstore.WithRecycle(workflow.Recycle{MaxJobs: 2}), // like --max-jobs=2
	sqlstore.WithStopWhenIdle(200*time.Millisecond),    // like --stop-when-empty
)
if err != nil {
	return err
}
if err := c.Register(thumbnail(&done)); err != nil {
	return err
}
genCtx, cancel := context.WithTimeout(ctx, runTimeout)
err = workflow.Serve(genCtx, c) // Start, then block until the client stops
cancel()
```

## Step 3: exit so the supervisor restarts you

The error matches `workflow.ErrRecycle`, and `errors.As` gives the `*RecycleError` with the `Reason` (`RecycleMaxJobs`, `RecycleMaxTime`, `RecycleMaxMemory`), the jobs finished, the time since `Start` and the last memory reading. In a real worker, exit with a code your supervisor restarts on; the example loops instead, each pass standing in for a new process:

<!-- snippet: examples/worker-recycling/main.go#supervisor -->
```go
var rec *workflow.RecycleError
switch {
case errors.As(err, &rec):
	fmt.Fprintf(out, "generation %d: recycled (%s), jobs done: %d\n", gen, rec.Reason, rec.Jobs)
	continue // a real worker exits 0 here and its supervisor starts a new process
case err != nil:
	return err
}
fmt.Fprintf(out, "generation %d: queue empty, jobs done: %d, exit\n", gen, done.Load()-before)
```

```text
generation 1: recycled (max_jobs), jobs done: 2
generation 2: recycled (max_jobs), jobs done: 2
generation 3: queue empty, jobs done: 1, exit
done: 5 of 5
memory worker: recycled (max_memory) at 600 MiB, ErrRecycle true
```

An app with its own loop reads the same answer without `Serve`: wait on `client.(workflow.Drainer).Done()`, then call `client.(workflow.StopCauser).StopCause()`. Clients wrapped with `middleware.WrapClient` or `WithCodec` forward `StopCause`; the failover client joins both sides, so its error matches `ErrRecycle` when either backend recycled.

## What a recycle does

The worker drains exactly as `Drain` does, with no deadline: it claims nothing more, lets running attempts finish, then stops, and `Done` closes.

| Driver | Work left behind |
|---|---|
| SQL | Unclaimed and delayed runs stay in the database for the next process or any other worker |
| Hatchet | Runs stay on the engine and go to any worker serving their workflow. On-failure handlers count as attempts; durable sleep and wait steps do not, since they hold no worker |
| In-memory | Nothing outlives the client: runs not finished become cancelled. Recycling on this driver suits tests and single-process tools only |

## Pitfalls

- **A long step delays the exit.** The drain has no deadline, so a step that runs for an hour keeps the old process alive for that hour after the limit. Bound steps with `Step.Timeout`.
- **`MaxJobs` counts attempts, not runs.** A run of three steps, or one step retried twice, counts three. Several workers in one client can finish attempts at once, so the count can pass the limit by up to the number of workers before the drain stops claiming. On Hatchet, up to `Slots` attempts the engine already handed out before the pause landed may start past `MaxJobs`.
- **The default memory reading is the Go heap,** not the process's resident size. Memory held by cgo, mmap or goroutine stacks is not in it; pass your own `Memory` for those.
- **`Serve` returns nil for stops you asked for.** Treat only `ErrRecycle` as "restart me"; a nil return after stop-when-idle means the queue was empty.
- **Give both failover backends the same limits.** The failover client's `Done` waits for both sides, so a recycle on one side alone stops nothing until the other stops too.
- **Without a supervisor nothing restarts.** On a platform that does not restart an exited process, a recycle ends the worker for good.

## Related

- [Graceful drain](../concepts/client.md#stop-gracefully): `Drain`, `Done` and stop-when-idle, the stop a recycle uses.
- [Deploy](deploy.md): running workers as separate processes.
- [Tuning](tuning.md): worker counts and poll intervals.
