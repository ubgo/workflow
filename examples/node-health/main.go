// Command node-health shows what a client says about the process it runs
// in: the node that ran each attempt (StepInfo.Node, AttemptError.Node,
// RunInfo.AttemptedBy), the online nodes (workflow.NodeLister), and its
// health for liveness and readiness probes (workflow.CheckHealth), on the
// memory driver. With REDIS_URL set it also runs a scheduler-only node and
// a worker-only node on one Redis prefix: the scheduler fires a schedule,
// the worker runs it, and Nodes lists both.
//
// It is the program behind docs/operations/nodes.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/redis"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "node-health:", err)
		os.Exit(1)
	}
}

// envURL names the Redis for the scheduler and worker nodes, such as
// redis://localhost:7012/0.
const envURL = "REDIS_URL"

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := onMemory(ctx, out); err != nil {
		return err
	}
	url := os.Getenv(envURL)
	if url == "" {
		fmt.Fprintln(out, envURL, "is not set: the scheduler and worker nodes are skipped")
		return nil
	}
	return onRedis(ctx, out, url)
}

// snippet:probes
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

// snippet:end

// onMemory shows node identity, the node list and the probes on the
// memory driver.
func onMemory(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:nodeid
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
	// snippet:end
	if err != nil {
		return err
	}
	srv := httptest.NewServer(probes(c))
	defer srv.Close()
	probe := func(path string) {
		res, err := http.Get(srv.URL + path)
		if err != nil {
			fmt.Fprintln(out, path, err)
			return
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		fmt.Fprintf(out, "%s %d %s\n", path, res.StatusCode, strings.TrimSpace(string(body)))
	}

	fmt.Fprintln(out, "-- before Start")
	probe("/livez")
	probe("/readyz")
	if err := c.Start(ctx); err != nil {
		return err
	}
	fmt.Fprintln(out, "-- started")
	probe("/readyz")

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync"})
	if err != nil {
		return err
	}
	if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
		return err
	}
	// snippet:attempts
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil {
		return err
	}
	r := page.Runs[0]
	fmt.Fprintf(out, "run %s, attempted by %v\n", r.Status, r.AttemptedBy)
	for _, e := range r.Errors {
		fmt.Fprintf(out, "  attempt %d on %s: %s\n", e.Attempt, e.Node, e.Error)
	}
	// snippet:end

	// snippet:nodes
	if nl, ok := workflow.Client(c).(workflow.NodeLister); ok {
		nodes, err := nl.Nodes(ctx)
		if err != nil {
			return err
		}
		for _, n := range nodes {
			fmt.Fprintf(out, "node %s mode=%s workers=%d running=%d\n", n.ID, n.Mode, n.Workers, n.Running)
		}
	}
	// snippet:end

	// snippet:fallback
	// A client without HealthChecker (here the memory client hidden behind
	// a type that has only the Client methods) is reported live and ready,
	// with one check saying it cannot tell.
	rep := workflow.CheckHealth(ctx, struct{ workflow.Client }{c})
	fmt.Fprintf(out, "no HealthChecker: live=%t ready=%t check=%q\n", rep.Live, rep.Ready, rep.Checks[0].Detail)
	// snippet:end

	c.Stop()
	fmt.Fprintln(out, "-- stopped")
	probe("/livez")
	return nil
}

// onRedis runs a scheduler-only node and a worker-only node on one prefix.
func onRedis(ctx context.Context, out io.Writer, url string) error {
	opts, err := goredis.ParseURL(url)
	if err != nil {
		return err
	}
	rdb := goredis.NewClient(opts)
	defer rdb.Close()
	prefix := "example-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	defer func() { _ = deletePrefix(context.Background(), rdb, prefix) }()

	ran := make(chan string, 8)
	tick := workflow.Workflow{Name: "tick", Steps: []workflow.Step{{Name: "beat", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		select {
		case ran <- info.Node:
		default:
		}
		return nil, nil
	}}}}

	nctx, stop := context.WithCancel(ctx)
	defer stop()
	// snippet:split
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
	// snippet:end
	defer scheduler.Stop()
	defer worker.Stop()

	if _, err := scheduler.Schedule(ctx, workflow.Schedule{Name: "every-second", Workflow: "tick", Cron: "* * * * * *", Seconds: true, Location: time.UTC}); err != nil {
		return err
	}
	select {
	case node := <-ran:
		fmt.Fprintln(out, "tick fired by sched-1, ran on", node)
	case <-time.After(10 * time.Second):
		return errors.New("no tick ran within 10s")
	}
	nodes, err := worker.Nodes(ctx)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		fmt.Fprintf(out, "node %s mode=%s workers=%d\n", n.ID, n.Mode, n.Workers)
	}
	return nil
}

// deletePrefix removes every key of the example's prefix.
func deletePrefix(ctx context.Context, rdb *goredis.Client, prefix string) error {
	iter := rdb.Scan(ctx, 0, "{"+prefix+"}*", 500).Iterator()
	for iter.Next(ctx) {
		if err := rdb.Del(ctx, iter.Val()).Err(); err != nil {
			return err
		}
	}
	return iter.Err()
}
