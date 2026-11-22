# Worker recycling

A long-lived worker can drift: a library that leaks, a cache that only grows, a connection pool that went bad. Recycling is the old, dull fix: after a number of jobs, after a length of time, or once the heap passes a size, the worker stops taking work, lets what it is running finish, and exits, so the supervisor that runs it (systemd, Kubernetes, a process manager) starts a fresh one. In Go this is rarely needed, but when a dependency misbehaves it is cheaper than finding the leak today.

## Before you start

- Recycling is a driver option: [`memory.WithRecycle`](ds:block?id=memory-withrecycle-tsfrb2gk), [`sqlstore.WithRecycle`](ds:block?id=sql-withrecycle-tekpfn73), or `Config.Recycle` on the Hatchet driver, each taking a [`workflow.Recycle`](ds:block?id=recycle-xh3besny). The zero value never recycles.
- The process needs a supervisor that restarts it. Recycling stops the worker; it never starts one.
- The example is [`examples/worker-recycling`](../../examples/worker-recycling). Run it with `go run ./worker-recycling` from `examples/`.

## Step 1: set the limits

| Field | Stops the worker | Like |
|---|---|---|
| `MaxJobs` | After this many finished step attempts by this client, whatever their outcome ([`RecycleWatch`](ds:block?id=recyclewatch-checklocked-mwcm8y6t) counts each one) | `--max-jobs` |
| `MaxTime` | This long after `Start`, on the driver's clock | `--max-time` |
| `MaxMemory` | Once `Memory()` reads more than this many bytes ([`RecycleWatch`](ds:block?id=recyclewatch-checklocked-mwcm8y6t)), checked after each attempt and every `CheckEvery` | `--memory` |
| `Memory` | The reading; nil means [`workflow.HeapObjectBytes`](ds:block?id=heapobjectbytes-c5bmxcaa) (the Go heap's live objects, from `runtime/metrics`). Pass your own to read RSS or a cgroup counter | |
| `CheckEvery` | How often memory is checked between attempts; zero means [`workflow.DefaultRecycleCheck`](ds:block?id=defaultrecyclecheck-amqh8hxt) (one second) | |

Any field left at zero is no limit. A negative `MaxJobs`, `MaxTime` or `CheckEvery` is refused at `Start` with `workflow.ErrInvalid` ([`workflow.ValidateRecycle`](ds:block?id=validaterecycle-nuecyb5b)); `MaxMemory` is unsigned.

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

`Start` returns once the workers are up, so recycling is not reported through it. [`workflow.Serve(ctx, client)`](ds:block?id=serve-m3sgzd2c) starts the client and blocks until it has stopped, then returns why: Start's error, a `*workflow.RecycleError` when a limit stopped it, or nil for every stop you asked for (the context ending, `Drain`, stop-when-idle):

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

The error matches [`workflow.ErrRecycle`](ds:block?id=errrecycle-smp6ce9f), and `errors.As` gives the [`*RecycleError`](ds:block?id=recycleerror-qfzrpxmt) with the `Reason` ([`RecycleMaxJobs`](ds:block?id=recyclemaxjobs-a7e34ytz), `RecycleMaxTime`, `RecycleMaxMemory`), the jobs finished, the time since `Start` and the last memory reading. In a real worker, exit with a code your supervisor restarts on; the example loops instead, each pass standing in for a new process:

<!-- snippet: examples/worker-recycling/main.go#supervisor -->
```go
var rec *workflow.RecycleError
switch {
case errors.As(err, &rec):
	fmt.Printf("generation %d: recycled (%s), jobs done: %d\n", gen, rec.Reason, rec.Jobs)
	continue // a real worker exits 0 here and its supervisor starts a new process
case err != nil:
	return err
}
fmt.Printf("generation %d: queue empty, jobs done: %d, exit\n", gen, done.Load()-before)
```

```text
generation 1: recycled (max_jobs), jobs done: 2
generation 2: recycled (max_jobs), jobs done: 2
generation 3: queue empty, jobs done: 1, exit
done: 5 of 5
memory worker: recycled (max_memory) at 600 MiB, ErrRecycle true
```

An app with its own loop reads the same answer without `Serve`: wait on `client.(workflow.Drainer).Done()`, then call [`client.(workflow.StopCauser).StopCause()`](ds:block?id=stopcauser-6chpc2ds). Clients wrapped with `middleware.WrapClient` or `WithCodec` forward `StopCause` ([`workflow.Forward`](ds:block?id=forward-stopcause-8kqevxch)); the [failover client](ds:block?id=failover-client-stopcause-nf8squej) joins both sides, so its error matches `ErrRecycle` when either backend recycled.

## What a recycle does

The worker drains exactly as `Drain` does, with no deadline ([memory](ds:block?id=memory-client-recyclenow-fmh3b27f), [SQL](ds:block?id=sql-client-recyclenow-8yuv3ad5), [Hatchet](ds:block?id=hatchet-client-recycle-v94a4x8t)): it claims nothing more, lets running attempts finish ([`TestRecycleLetsRunningAttemptsFinish`](ds:block?id=testrecycleletsrunningattemptsfinish-dm5rrvq6)), then stops, and `Done` closes.

| Driver | Work left behind |
|---|---|
| SQL | Unclaimed and delayed runs stay in the database for the next process or any other worker |
| Hatchet | Runs stay on the engine and go to any worker serving their workflow. On-failure handlers count as attempts; durable sleep and wait steps do not, since they hold no worker |
| In-memory | Nothing outlives the client: runs not finished become cancelled ([`memory.WithRecycle`](ds:block?id=memory-withrecycle-tsfrb2gk)). Recycling on this driver suits tests and single-process tools only |

## Try it

```sh
cd examples
go run ./worker-recycling
```

```text
generation 1: recycled (max_jobs), jobs done: 2
generation 2: recycled (max_jobs), jobs done: 2
generation 3: queue empty, jobs done: 1, exit
done: 5 of 5
memory worker: recycled (max_memory) at 600 MiB, ErrRecycle true
```

The program plays the supervisor: five jobs wait in a SQLite queue and each generation is a fresh worker with `MaxJobs: 2`. The first two generations recycle after two jobs each and are restarted; the third finds one job, then the queue empty, and stop-when-idle ends it with a nil error, so the loop exits. The last line is a separate memory worker whose `Memory` reading (a fake that rises from 200 to 600 MiB) passes `MaxMemory` of 512 MiB at its next `CheckEvery`; once `Done` closes, its `StopCause` is a `*workflow.RecycleError` with the reason and the reading, and matches `workflow.ErrRecycle`.

## Pitfalls

- **A long step delays the exit.** The drain has no deadline (`Drain(context.Background())` in [`recycleNow`](ds:block?id=sql-client-recyclenow-8yuv3ad5)), so a step that runs for an hour keeps the old process alive for that hour after the limit. Bound steps with `Step.Timeout`.
- **`MaxJobs` counts attempts, not runs.** A run of three steps, or one step retried twice, counts three. Several workers in one client can finish attempts at once, so the count can pass the limit by up to the number of workers before the drain stops claiming ([`Recycle.MaxJobs`](ds:block?id=recycle-xh3besny)). On Hatchet ([`TestRecycleLive`](ds:block?id=hatchet-testrecyclelive-dc5wbr5w)), up to `Slots` attempts the engine already handed out before the pause landed may start past `MaxJobs`.
- **The default memory reading is the Go heap** ([`HeapObjectBytes`](ds:block?id=heapobjectbytes-c5bmxcaa)), not the process's resident size. Memory held by cgo, mmap or goroutine stacks is not in it; pass your own `Memory` for those.
- **`Serve` returns nil for stops you asked for** ([`TestServeNilOnRequestedStop`](ds:block?id=testservenilonrequestedstop-bbccvjfa)). Treat only `ErrRecycle` as "restart me"; a nil return after stop-when-idle means the queue was empty.
- **Give both failover backends the same limits.** The failover client's `Done` waits for both sides ([`StopCause`](ds:block?id=failover-client-stopcause-nf8squej)), so a recycle on one side alone stops nothing until the other stops too.
- **Without a supervisor nothing restarts.** On a platform that does not restart an exited process, a recycle ends the worker for good.

## Related

- [Graceful drain](../concepts/client.md#stop-gracefully): `Drain`, `Done` and stop-when-idle, the stop a recycle uses.
- [Deploy](deploy.md): running workers as separate processes.
- [Tuning](tuning.md): worker counts and poll intervals.
