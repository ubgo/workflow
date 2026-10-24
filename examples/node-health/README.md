# node-health

What a client says about the process it runs in, on the memory driver: the node that ran each attempt (`StepInfo.Node`, `AttemptError.Node`, `RunInfo.AttemptedBy`), the online nodes (`workflow.NodeLister`), and liveness and readiness probe routes built on `workflow.CheckHealth`, checked before `Start`, after it and after `Stop`. With `REDIS_URL` set it also runs a scheduler-only node and a worker-only node on one Redis prefix, deleted at the end: the scheduler fires a schedule, the worker runs it, and `Nodes` lists both.

```sh
cd examples
go run ./node-health
```

```text
-- before Start
/livez 200 workers ok=false not started
/readyz 503 workers ok=false not started
-- started
/readyz 200 workers ok=true 0 of 8 busy
run succeeded, attempted by [worker-1]
  attempt 1 on worker-1: upstream timeout on worker-1
node worker-1 mode=full workers=8 running=0
no HealthChecker: live=true ready=true check="the driver reports no health"
-- stopped
/livez 503 workers ok=false stopped
REDIS_URL is not set: the scheduler and worker nodes are skipped
```

Against the shared local stack's Redis:

```sh
REDIS_URL=redis://localhost:7012/0 go run ./node-health
```

The last line is replaced by:

```text
tick fired by sched-1, ran on worker-1
node sched-1 mode=scheduler workers=0
node worker-1 mode=worker workers=10
```

## The code

Name the node, and read it inside a step:

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

Which node ran each attempt, after the run:

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

The online nodes:

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

Liveness and readiness routes:

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

A client that reports no health:

<!-- snippet: examples/node-health/main.go#fallback -->
```go
// A client without HealthChecker (here the memory client hidden behind
// a type that has only the Client methods) is reported live and ready,
// with one check saying it cannot tell.
rep := workflow.CheckHealth(ctx, struct{ workflow.Client }{c})
fmt.Fprintf(out, "no HealthChecker: live=%t ready=%t check=%q\n", rep.Live, rep.Ready, rep.Checks[0].Detail)
```

A scheduler-only node and a worker-only node on Redis:

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

`main_test.go` runs the program and checks the memory lines, and the Redis lines when `REDIS_URL` is set.

## Guides

- [Nodes and health](../../docs/operations/nodes.md)
