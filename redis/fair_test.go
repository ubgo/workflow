package redis_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	wfredis "github.com/ubgo/workflow/redis"
)

// fairRecorder records the key of every run a fairness workflow starts and
// holds the runs whose key is "hold" until released.
type fairRecorder struct {
	done    atomic.Int64 // handler calls that returned
	mu      sync.Mutex
	order   []string
	release chan struct{}
	held    chan struct{}
}

func newFairRecorder(holds int) *fairRecorder {
	return &fairRecorder{release: make(chan struct{}), held: make(chan struct{}, holds)}
}

func (f *fairRecorder) step(pause time.Duration) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		defer f.done.Add(1)
		key, _, _ := strings.Cut(string(in), ":")
		if key == "hold" {
			select {
			case f.held <- struct{}{}:
			default:
			}
			select {
			case <-f.release:
			case <-ctx.Done():
			}
			return nil, nil
		}
		f.mu.Lock()
		f.order = append(f.order, key)
		f.mu.Unlock()
		time.Sleep(pause)
		return nil, nil
	}
}

func (f *fairRecorder) starts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

func (f *fairRecorder) waitStarts(t *testing.T, n int, within time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(within)
	for len(f.starts()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d runs started: %v", len(f.starts()), n, f.starts())
		}
		time.Sleep(testPoll)
	}
	return f.starts()
}

func fairWorkflow(name string, f *fairRecorder, maxRuns int, weight func(string) int, pause time.Duration) workflow.Workflow {
	return workflow.Workflow{Name: name, Concurrency: &workflow.Concurrency{
		Key:     workflow.KeyFunc(func(j workflow.Job) string { k, _, _ := strings.Cut(string(j.Payload), ":"); return k }),
		MaxRuns: maxRuns, Strategy: workflow.ConcurrencyRoundRobin, Weight: weight,
	}, Steps: []workflow.Step{{Name: "s", Run: f.step(pause)}}}
}

func fairJobs(wf, key string, n int) []workflow.Job {
	out := make([]workflow.Job, n)
	for i := range out {
		out[i] = workflow.Job{Workflow: wf, Payload: []byte(fmt.Sprintf("%s:%d", key, i))}
	}
	return out
}

func startFair(t *testing.T, c *wfredis.Client, wfs ...workflow.Workflow) context.Context {
	t.Helper()
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx
}

// TestFairAdmissionWindow proves FC-05 at admission: with MaxRuns 1, a
// backlog of 150 runs of one full bucket (more than the 100-run admission
// window) does not keep key b's one run out. The full bucket's runs are
// parked per bucket and come back one per freed slot.
func TestFairAdmissionWindow(t *testing.T) {
	rdb := redisClient(t)
	f := newFairRecorder(1)
	c := newClient(t, rdb, testPrefix(t, rdb), wfredis.WithWorkers(2))
	wf := fairWorkflow("fairwin", f, 1, nil, 0)
	ctx := startFair(t, c, wf)
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte("hold:0")}); err != nil {
		t.Fatal(err)
	}
	<-f.held
	// 150 more runs of key "hold" queue behind the held one in its full
	// bucket, then one run of key b.
	jobs := append(fairJobs(wf.Name, "hold", 150), fairJobs(wf.Name, "b", 1)...)
	for _, j := range jobs {
		if _, err := c.Enqueue(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.waitStarts(t, 1, 10*time.Second); got[0] != "b" {
		t.Fatalf("first start %q, want b", got[0])
	}
	// Every parked run still runs, one at a time, once the gate opens.
	close(f.release)
	deadline := time.Now().Add(60 * time.Second)
	for f.done.Load() < 152 {
		if time.Now().After(deadline) {
			t.Fatalf("%d of 152 runs finished", f.done.Load())
		}
		time.Sleep(10 * testPoll)
	}
}

// TestFairSharesUnderContention measures FC-06 with four workers claiming
// at once: weights a=1, b=3, 60 runs each queued behind four held
// workers; while both keys have work waiting, b gets three of every four
// starts (within a tolerance for concurrent claims).
func TestFairSharesUnderContention(t *testing.T) {
	rdb := redisClient(t)
	const workers = 4
	f := newFairRecorder(workers)
	c := newClient(t, rdb, testPrefix(t, rdb), wfredis.WithWorkers(workers))
	wf := fairWorkflow("fairshare", f, 1000, workflow.KeyWeights(map[string]int{"b": 3}, 1), 2*time.Millisecond)
	ctx := startFair(t, c, wf)
	for _, j := range fairJobs(wf.Name, "hold", workers) {
		if _, err := c.Enqueue(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	for range workers {
		<-f.held
	}
	for _, j := range append(fairJobs(wf.Name, "a", 60), fairJobs(wf.Name, "b", 60)...) {
		if _, err := c.Enqueue(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * testPoll) // every run admitted before a worker is free
	close(f.release)
	first := f.waitStarts(t, 120, time.Minute)[:40]
	b := strings.Count(strings.Join(first, ""), "b")
	if b < 27 || b > 33 {
		t.Fatalf("b had %d of the first 40 starts, want 30 (+/-3) for weights a=1 b=3: %v", b, first)
	}
}

// TestFairKeepsTheWorkflowsPlace proves the place-in-line rule: round
// robin reorders runs inside its workflow only; a run of another workflow
// enqueued between round-robin runs keeps its place by arrival. Runs are
// enqueued a few milliseconds apart, since Redis orders arrivals by run ID
// (millisecond resolution).
func TestFairKeepsTheWorkflowsPlace(t *testing.T) {
	rdb := redisClient(t)
	f := newFairRecorder(1)
	c := newClient(t, rdb, testPrefix(t, rdb), wfredis.WithWorkers(1))
	fair := fairWorkflow("fairplace", f, 10, nil, 0)
	plain := workflow.Workflow{Name: "plain", Steps: []workflow.Step{{Name: "s", Run: f.step(0)}}}
	ctx := startFair(t, c, fair, plain)
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: fair.Name, Payload: []byte("hold:0")}); err != nil {
		t.Fatal(err)
	}
	<-f.held
	for _, j := range []workflow.Job{
		{Workflow: fair.Name, Payload: []byte("a:1")},
		{Workflow: fair.Name, Payload: []byte("a:2")},
		{Workflow: plain.Name, Payload: []byte("p:1")},
		{Workflow: fair.Name, Payload: []byte("b:1")},
	} {
		time.Sleep(3 * time.Millisecond)
		if _, err := c.Enqueue(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * testPoll)
	close(f.release)
	if got := strings.Join(f.waitStarts(t, 4, 10*time.Second), " "); got != "a b p a" {
		t.Fatalf("order of starts %q, want %q", got, "a b p a")
	}
}
