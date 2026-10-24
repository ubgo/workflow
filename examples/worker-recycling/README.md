# worker-recycling

A worker that stops itself after a limit so its supervisor (systemd, Kubernetes, a process manager) starts a fresh process. The first part enqueues five jobs into a SQLite file, then plays the supervisor: each worker generation serves with `sqlstore.WithRecycle(workflow.Recycle{MaxJobs: 2})`, and `workflow.Serve` returns a `*workflow.RecycleError` once two jobs have finished; the backlog stays in the database for the next generation, and the last one stops when the queue is empty (`WithStopWhenIdle`). The second part recycles an in-memory worker whose heap reading, from an injected `Memory` function checked every `CheckEvery` on a fake clock, crosses `MaxMemory`.

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

A recycle drains like `Drain`: running attempts finish, nothing new is claimed, and on the SQL driver unclaimed runs stay queued for the next process. `Serve` returns nil for every stop that was asked for (the context ending, `Drain`, stop-when-idle), so the exit code can tell the supervisor which happened.

The guide that copies this program is [Worker recycling](../../docs/operations/recycling.md). `go test ./worker-recycling` checks every line.
