package enginetest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// fairRecorder records the key of every run a fairness workflow starts and
// holds the runs whose key is "hold" until released.
type fairRecorder struct {
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
		key, _, _ := strings.Cut(string(in), ":")
		if key == "hold" {
			f.held <- struct{}{}
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

// TestFairAdmissionWindow proves FC-05 at admission: with MaxRuns 1 and a
// fetch window of 5, key a's backlog of 30 waiting runs (its bucket full)
// does not keep key b's one run out of the window. Before 011 the window
// held only a's runs and b waited for all of them.
func TestFairAdmissionWindow(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			f := newFairRecorder(1)
			c := newClient(t, e, db, prefix, sqlstore.WithWorkers(2), sqlstore.WithFetchBatch(5))
			wf := fairWorkflow("fairwin", f, 1, nil, 0)
			ctx := startWith(t, c, wf)
			defer close(f.release)
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte("hold:0")}); err != nil {
				t.Fatal(err)
			}
			<-f.held
			// The hold run's key is "hold"; a's runs each wait for a's
			// one slot, which its first run keeps while the gate is shut.
			jobs := append(fairJobs(wf.Name, "a", 30), fairJobs(wf.Name, "b", 1)...)
			for i := range 30 {
				jobs[i].Payload = []byte(fmt.Sprintf("hold:a%d", i))
			}
			// Bucket by the payload's prefix: "hold" for a's runs too, so
			// a's 30 runs queue behind the held one in one full bucket.
			if _, err := c.EnqueueMany(ctx, jobs); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				if s := f.starts(); len(s) > 0 && s[0] == "b" {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("key b's run did not start within 10s behind a full bucket's backlog of 30 (starts %v)", f.starts())
				}
				time.Sleep(testPoll)
			}
		})
	}
}

// TestFairSharesUnderContention measures FC-06 with several workers
// claiming at once: weights a=1, b=3, 60 runs each queued behind four
// held workers; while both keys have work waiting, b gets three of every
// four starts. Concurrent claims may swap neighbours, so the share is
// checked within a tolerance.
func TestFairSharesUnderContention(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			const workers = 4
			f := newFairRecorder(workers)
			c := newClient(t, e, db, prefix, sqlstore.WithWorkers(workers))
			wf := fairWorkflow("fairshare", f, 1000, workflow.KeyWeights(map[string]int{"b": 3}, 1), 2*time.Millisecond)
			ctx := startWith(t, c, wf)
			if _, err := c.EnqueueMany(ctx, fairJobs(wf.Name, "hold", workers)); err != nil {
				t.Fatal(err)
			}
			for range workers {
				<-f.held
			}
			jobs := append(fairJobs(wf.Name, "a", 60), fairJobs(wf.Name, "b", 60)...)
			if _, err := c.EnqueueMany(ctx, jobs); err != nil {
				t.Fatal(err)
			}
			time.Sleep(20 * testPoll) // every run admitted before a worker is free
			close(f.release)
			deadline := time.Now().Add(60 * time.Second)
			for len(f.starts()) < 120 {
				if time.Now().After(deadline) {
					t.Fatalf("only %d of 120 runs started", len(f.starts()))
				}
				time.Sleep(testPoll)
			}
			first := f.starts()[:40]
			b := 0
			for _, k := range first {
				if k == "b" {
					b++
				}
			}
			if b < 27 || b > 33 {
				t.Fatalf("b had %d of the first 40 starts, want 30 (+/-3) for weights a=1 b=3: %v", b, first)
			}
		})
	}
}

// TestFairKeepsTheWorkflowsPlace proves the slot rule: round robin
// reorders runs inside its workflow only. A run of another workflow
// enqueued between two round-robin runs keeps its place by arrival.
func TestFairKeepsTheWorkflowsPlace(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			f := newFairRecorder(1)
			c := newClient(t, e, db, prefix, sqlstore.WithWorkers(1))
			fair := fairWorkflow("fairplace", f, 10, nil, 0)
			plain := workflow.Workflow{Name: "plain", Steps: []workflow.Step{{Name: "s", Run: f.step(0)}}}
			ctx := startWith(t, c, fair, plain)
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
				if _, err := c.Enqueue(ctx, j); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(20 * testPoll)
			close(f.release)
			deadline := time.Now().Add(10 * time.Second)
			for len(f.starts()) < 4 {
				if time.Now().After(deadline) {
					t.Fatalf("starts %v", f.starts())
				}
				time.Sleep(testPoll)
			}
			// a and b alternate inside the round-robin workflow (a, b,
			// a), and p keeps the third place it had by arrival.
			if got := strings.Join(f.starts(), " "); got != "a b p a" {
				t.Fatalf("order of starts %q, want %q", got, "a b p a")
			}
		})
	}
}
