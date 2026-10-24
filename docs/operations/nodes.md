# Nodes and health

Once workers run on several machines, an operator needs three answers: which machine ran this attempt, which machines are up right now, and whether this process can take work. A driver with node identity names the node on every attempt and lists the online nodes; every client can be asked for its health in the shape a liveness or readiness probe wants. On drivers that support it, a process can also be only a scheduler or only a worker, so schedules fire from one small process while the workers scale on their own.

## Before you start

- Node identity needs `workflow.CapNodeIdentity`: the memory and Redis drivers declare it. The Hatchet driver does not: it fills `StepInfo.Node` but keeps no per-attempt history, so `AttemptedBy` and `AttemptError.Node` stay empty there. The SQL driver declares it too: the attempt's node is stored with the run.
- The node list is `workflow.NodeLister`, health is `workflow.HealthChecker`; check the interfaces, not a capability (a client may list its nodes without per-attempt identity, and health has no capability bit). The memory, Redis, SQL and Hatchet drivers implement both. On a wrapped client (`workflow.Forward`, a codec or plugin client, `failover`) every optional method exists whatever is underneath, so ask `workflow.Supports(c, workflow.OpNodes)` before offering a node list: a wrapper answers for the client it wraps (`failover` for both backends).
- Scheduler-only and worker-only processes are options of the Redis and SQL drivers.
- The example is [`examples/node-health`](../../examples/node-health). The memory part runs anywhere; set `REDIS_URL` for the scheduler and worker nodes. Run it with `go run ./node-health` from `examples/`.

## Name the node

Every client has a node ID. The default is `workflow.DefaultNodeID()`, the host name and the process ID (`"web-1:4182"`), so two processes on one host are told apart. On a platform whose host names mean nothing (containers, a scheduler that reuses names) pass your own with the driver's option: `memory.WithNodeID`, `redis.WithNodeID`, `sqlstore.WithNodeID`, or `Config.NodeID` on Hatchet, which also labels every worker it registers. Inside a step, `StepInfo.Node` is the node running the attempt:

<!-- snippet: examples/node-health/main.go#nodeid -->
```go
c := memory.New(memory.WithNodeID("worker-1")) // default: workflow.DefaultNodeID(), "host:pid"
var tries atomic.Int32
err := c.Register(workflow.Workflow{
	Name:  "sync",
	Retry: workflow.RetryPolicy{Max: 1},
	Steps: []workflow.Step{{Name: "pull", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		if tries.Add(1) == 1 {
			return nil, fmt.Errorf("upstream timeout on %s", info.Node) // the node running this attempt
		}
		return nil, nil
	}}},
})
```

## See which node ran each attempt

With `CapNodeIdentity`, each failed attempt in `RunInfo.Errors` carries the node it ran on (`AttemptError.Node`), and `RunInfo.AttemptedBy` lists the nodes that ran the run's attempts, in first-attempt order, without repeats. A run that failed on one machine and succeeded on another says so:

<!-- snippet: examples/node-health/main.go#attempts -->
```go
page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
if err != nil {
	return err
}
r := page.Runs[0]
fmt.Fprintf(out, "run %s, attempted by %v\n", r.Status, r.AttemptedBy)
for _, e := range r.Errors {
	fmt.Fprintf(out, "  attempt %d on %s: %s\n", e.Attempt, e.Node, e.Error)
}
```

```text
run succeeded, attempted by [worker-1]
  attempt 1 on worker-1: upstream timeout on worker-1
```

The node also appears in [lifecycle events](../observability/lifecycle.md) (`LifecycleEvent.Node`), in a [stuck-attempt report](../observability/stuck-jobs.md) (`StuckEvent.Info.Node`), and in the admin run page and API (`attempted_by`, and `node` on each attempt).

## List the online nodes

`NodeLister.Nodes(ctx)` returns a `workflow.NodeInfo` per online node: `ID`, `Mode`, `Queues` (empty means every queue), `Workers` (its pool size), `Running` (attempts running when it last reported in), `StartedAt` and `LastSeen`:

<!-- snippet: examples/node-health/main.go#nodes -->
```go
if nl, ok := workflow.Client(c).(workflow.NodeLister); ok {
	nodes, err := nl.Nodes(ctx)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		fmt.Fprintf(out, "node %s mode=%s workers=%d running=%d\n", n.ID, n.Mode, n.Workers, n.Running)
	}
}
```

`Mode` is a `workflow.NodeMode`: `NodeFull` (workers and scheduler, the default), `NodeWorker`, `NodeScheduler` or `NodeEnqueueOnly`. How a node drops out differs by driver:

| Driver | What it lists | When a dead node drops out |
|---|---|---|
| memory | Itself, once started; nothing before `Start` | Never: there is one process |
| redis | Every started client of the prefix; each reports in every third of `WithLease` and a stopped client removes itself | After three leases without a report (90 seconds with the default 30-second lease) |
| hatchet | One entry per `Config.NodeID` whose workers the engine reports active, merging its workers (one per served queue); mode `NodeFull`; enqueue-only clients register no worker and are not listed | When the engine marks its workers inactive, after their heartbeats stop |
| sql | Every client of the tables that reported in within the last hour; each writes its row every third of `WithLease` and removes it on a clean stop | After it has been silent for an hour; read `LastSeen` for liveness, since a crashed client stays listed until then |

## Health and readiness probes

`workflow.CheckHealth(ctx, c)` returns a `workflow.HealthReport`: `Live` (the process should keep running; restart it when false), `Ready` (it can take work now; route nothing to it when false) and `Checks`, each a `Name`, an `OK` verdict and a `Detail`. A client that does not implement `HealthChecker` is reported live and ready with one check saying it cannot tell, so an app on such a driver needs its own probe. Serve it on routes your orchestrator calls, without the admin login, with a deadline, because a probe that hangs is worse than one that fails:

<!-- snippet: examples/node-health/main.go#probes -->
```go
// probes serves a liveness and a readiness route from the client's health.
// They need no login: a load balancer or an orchestrator calls them.
func probes(c workflow.Client) http.Handler {
	mux := http.NewServeMux()
	answer := func(w http.ResponseWriter, ok bool, rep workflow.HealthReport) {
		code := http.StatusOK
		if !ok {
			code = http.StatusServiceUnavailable
		}
		w.WriteHeader(code)
		for _, ch := range rep.Checks {
			fmt.Fprintf(w, "%s ok=%t %s\n", ch.Name, ch.OK, ch.Detail)
		}
	}
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second) // a probe must not hang
		defer cancel()
		rep := workflow.CheckHealth(ctx, c)
		answer(w, rep.Live, rep) // not live: restart the process
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		rep := workflow.CheckHealth(ctx, c)
		answer(w, rep.Ready, rep) // not ready: send no traffic, keep the process
	})
	return mux
}
```

| Driver | Live | Ready | Checks |
|---|---|---|---|
| memory | Until stopped | Started, not draining, not stopped | `workers` ("3 of 8 busy", "not started", "draining", "stopped") |
| redis | Until stopped | Started (an enqueue-only client: always), not draining, and Redis answers a `PING` within the deadline | `client` (its mode, or why not), `redis` ("PING ok", or the error) |
| hatchet | Until a recycle (`Config.Recycle`) stopped it; a requested stop leaves it live | Started and not stopped, the engine answers its version call within the deadline and, for a worker client, every worker it registered is active on the engine | `client`, `engine` ("version ..." or the error), `workers` ("N active" or which is not) |
| sql | Until stopped | Started (an enqueue-only client: at once) and the database answers a ping within the deadline | `started` (its mode), `database` ("ping ok", or the error) |

A client is live but not ready before `Start` and while its backend is unreachable, so a readiness probe holds traffic back during a database or Redis outage without the orchestrator restarting a healthy process:

```text
-- before Start
/livez 200 workers ok=false not started
/readyz 503 workers ok=false not started
-- started
/readyz 200 workers ok=true 0 of 8 busy
-- stopped
/livez 503 workers ok=false stopped
```

A client without `HealthChecker`:

<!-- snippet: examples/node-health/main.go#fallback -->
```go
// A client without HealthChecker (here the memory client hidden behind
// a type that has only the Client methods) is reported live and ready,
// with one check saying it cannot tell.
rep := workflow.CheckHealth(ctx, struct{ workflow.Client }{c})
fmt.Fprintf(out, "no HealthChecker: live=%t ready=%t check=%q\n", rep.Live, rep.Ready, rep.Checks[0].Detail)
```

The admin API reads the same report at `GET /health` for its System screen, behind the admin login and always with status 200; a probe uses `CheckHealth` on its own route, as above ([Admin JSON API](../admin/http-api.md#routes)).

## Scheduler-only and worker-only processes

By default every client both runs workers and fires schedules, and the driver makes each tick fire once across them. Splitting the roles lets the workers scale (or scale to zero) without touching the scheduler, and keeps a small, always-on process for the clock:

- `WithSchedulerOnly()` makes a scheduler node (`NodeScheduler`): `Start` runs no worker; it fires schedules and admits pending runs for the worker nodes. It reports zero workers.
- `WithoutScheduler()` makes a worker node (`NodeWorker`): it runs workers and admits pending runs but fires no schedule. Declared `Workflow.Cron` entries are still written at `Start`, so the scheduler node fires them.

Both register the workflows: a schedule's `ScheduleWhen` and windows are code the firing node must have.

<!-- snippet: examples/node-health/main.go#split -->
```go
scheduler, err := redis.New(rdb, redis.WithPrefix(prefix), redis.WithNodeID("sched-1"),
	redis.WithSchedulerOnly()) // fires schedules, runs no step
if err != nil {
	return err
}
worker, err := redis.New(rdb, redis.WithPrefix(prefix), redis.WithNodeID("worker-1"),
	redis.WithoutScheduler(), redis.WithPollInterval(50*time.Millisecond)) // runs steps, fires no schedule
if err != nil {
	return err
}
for _, c := range []*redis.Client{scheduler, worker} {
	if err := c.Register(tick); err != nil { // both register: the scheduler needs ScheduleWhen and windows
		return err
	}
	if err := c.Start(nctx); err != nil {
		return err
	}
}
```

```text
tick fired by sched-1, ran on worker-1
node sched-1 mode=scheduler workers=0
node worker-1 mode=worker workers=10
```

These are Redis and SQL options (`redis.WithSchedulerOnly`, `sqlstore.WithSchedulerOnly` and their `WithoutScheduler` twins). Hatchet has neither: its engine fires the crons itself, so every Hatchet node reports `NodeFull`. The memory driver has neither, since nothing outside the process would run what it fires; to fire crons in process and run the jobs on a durable driver, use [hybrid placement](../drivers/hybrid.md). A web node that only enqueues is a third role, `NodeEnqueueOnly` ([Deploy workers › Web nodes that only enqueue](deploy.md#web-nodes-that-only-enqueue)).

## Try it

```sh
cd examples
go run ./node-health
REDIS_URL=redis://localhost:7012/0 go run ./node-health
```

Without `REDIS_URL` the program prints the memory part and says the Redis part is skipped; with it, the last lines are the scheduler and worker nodes above. `go test ./node-health` checks every line.

## Pitfalls

- **Give containers a stable, unique node ID.** Two processes with the same `WithNodeID` overwrite each other in the node list and their attempts cannot be told apart. Use the pod or task name, or keep the default.
- **Liveness is not readiness.** Restarting on `Ready == false` turns a short database outage into a restart storm; restart only on `Live == false`.
- **A scheduler-only node must stay up.** With every other node started `WithoutScheduler`, no schedule fires while it is down; run it under a supervisor, or run two (each tick still fires once, decided by the driver).
- **A worker-only fleet with no scheduler node fires nothing.** Declared crons are written at `Start` but only a node without `WithoutScheduler` fires them.
- **The node list lags.** A crashed Redis node stays listed for up to three leases; `LastSeen` says how stale an entry is.
- **The admin health route is not a probe.** It needs the admin login and always answers 200; probes call `CheckHealth` on their own routes.

## Related

- [Deploy workers](deploy.md): several workers on one store, web nodes that only enqueue, clean shutdown.
- [Hybrid placement](../drivers/hybrid.md): crons in process, jobs on a durable driver.
- [Lifecycle events](../observability/lifecycle.md), [Stuck jobs](../observability/stuck-jobs.md).
- [The admin web UI](../admin/ui.md): the System screen shows health and nodes.
- Example: [examples/node-health](../../examples/node-health/).
