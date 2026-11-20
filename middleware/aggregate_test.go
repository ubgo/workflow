package middleware_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/workflowtest"
)

// TestAggregate_Triggers checks when a flush run is enqueued: the job that
// opens a chunk enqueues one for the window's end, the job that fills a
// chunk one for now, and every other job none (a zero RunRef, nil error).
// It also checks what the flush run carries.
func TestAggregate_Triggers(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := clock.NewFake(start)
	stub := &stubClient{caps: workflow.CapDelayedStart}
	mw, err := middleware.Aggregate(stub, middleware.NewMemoryAggregate(), middleware.AggregateConfig{
		Workflows: []string{"digest"}, Window: 10 * time.Second, MaxItems: 3, Clock: clk,
		Group: func(j workflow.Job) string { return j.Labels["user"] },
	})
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(stub, mw)
	ctx := context.Background()
	job := workflow.Job{Workflow: "digest", Payload: []byte(`"c"`), Queue: "mail", Tenant: "t1", Labels: map[string]string{"user": "u1"}}
	end := start.Add(11 * time.Second) // the first job comes at +1s, the window counts from it
	// count -> RunAt of the flush it enqueues (zero: now), or none.
	want := []struct {
		flush bool
		runAt time.Time
	}{{true, end}, {false, time.Time{}}, {true, time.Time{}}, {true, end}, {false, time.Time{}}, {true, time.Time{}}, {true, end}}
	for i, w := range want {
		clk.Advance(time.Second)
		before := len(stub.jobs)
		ref, err := c.Enqueue(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(stub.jobs) > before; got != w.flush {
			t.Fatalf("job %d: flush enqueued = %v, want %v", i+1, got, w.flush)
		}
		if !w.flush {
			if ref != (workflow.RunRef{}) {
				t.Fatalf("job %d joined a pending flush but returned %+v", i+1, ref)
			}
			continue
		}
		f := stub.jobs[len(stub.jobs)-1]
		if !f.RunAt.Equal(w.runAt) {
			t.Fatalf("job %d: flush RunAt = %v, want %v", i+1, f.RunAt, w.runAt)
		}
		if f.Payload != nil || f.Queue != "mail" || f.Tenant != "t1" || f.Labels["user"] != "u1" ||
			f.Labels[middleware.AggregateGroupLabel] != "u1" || f.Labels[middleware.AggregateMaxLabel] != "3" {
			t.Fatalf("job %d: flush job = %+v", i+1, f)
		}
	}
	if job.Labels[middleware.AggregateGroupLabel] != "" {
		t.Fatal("the caller's label map was changed")
	}
}

// TestAggregate_PassesThroughAndRefuses covers what Aggregate leaves alone
// and what it refuses before storing anything.
func TestAggregate_PassesThroughAndRefuses(t *testing.T) {
	ctx := context.Background()
	if _, err := middleware.Aggregate(&stubClient{}, middleware.NewMemoryAggregate(), middleware.AggregateConfig{Workflows: []string{"d"}, Window: time.Second}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("a client without CapDelayedStart: %v", err)
	}
	stub := &stubClient{caps: workflow.CapDelayedStart}
	for _, cfg := range []middleware.AggregateConfig{{Window: time.Second}, {Workflows: []string{"d"}}, {Workflows: []string{"d"}, Window: time.Second, MaxItems: -1}, {Workflows: []string{""}, Window: time.Second}} {
		if _, err := middleware.Aggregate(stub, middleware.NewMemoryAggregate(), cfg); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("config %+v accepted: %v", cfg, err)
		}
	}
	store := middleware.NewMemoryAggregate()
	mw, err := middleware.Aggregate(stub, store, middleware.AggregateConfig{Workflows: []string{"d"}, Window: time.Second,
		Group: func(j workflow.Job) string { return j.Labels["g"] }})
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(stub, mw)
	for _, job := range []workflow.Job{
		{Workflow: "other", Payload: []byte("not json")},
		{Workflow: "d", Payload: []byte(`1`)}, // no group: an ordinary run
	} {
		if _, err := c.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
	}
	if len(stub.jobs) != 2 || stub.jobs[1].Labels[middleware.AggregateGroupLabel] != "" {
		t.Fatalf("pass-through jobs = %+v", stub.jobs)
	}
	g := map[string]string{"g": "x"}
	for name, job := range map[string]workflow.Job{
		"key":       {Workflow: "d", Labels: g, IdempotencyKey: "k"},
		"chain":     {Workflow: "d", Labels: g, Chain: []workflow.Job{{Workflow: "other"}}},
		"not json":  {Workflow: "d", Labels: g, Payload: []byte("plain text")},
		"own label": {Workflow: "d", Labels: map[string]string{"g": "x", middleware.AggregateGroupLabel: "y"}},
	} {
		if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	if store.Pending("x") != 0 {
		t.Fatal("a refused job was stored")
	}
	raw, err := middleware.Aggregate(stub, store, middleware.AggregateConfig{Workflows: []string{"d"}, Window: time.Second, RawItems: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := middleware.WrapClient(stub, raw).Enqueue(ctx, workflow.Job{Workflow: "d", Payload: []byte("plain text")}); err != nil || store.Pending("d") != 1 {
		t.Fatalf("RawItems: %v, pending %d", err, store.Pending("d"))
	}
}

// TestAggregate_FailedFlushIsRetried: when the flush enqueue fails, the
// item stays stored, the error matches ErrFlushNotEnqueued, and the next
// job of the group (which would not trigger by count) enqueues an
// immediate flush.
func TestAggregate_FailedFlushIsRetried(t *testing.T) {
	stub := &stubClient{caps: workflow.CapDelayedStart, enqueueErr: errors.New("backend down")}
	store := middleware.NewMemoryAggregate()
	mw, err := middleware.Aggregate(stub, store, middleware.AggregateConfig{Workflows: []string{"d"}, Window: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(stub, mw)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "d", Payload: []byte(`1`)}); !errors.Is(err, middleware.ErrFlushNotEnqueued) {
		t.Fatalf("err = %v", err)
	}
	if store.Pending("d") != 1 {
		t.Fatal("the item was dropped with the failed flush")
	}
	stub.enqueueErr = nil
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "d", Payload: []byte(`2`)})
	if err != nil || ref.RunID.IsZero() || len(stub.jobs) != 1 || !stub.jobs[0].RunAt.IsZero() {
		t.Fatalf("second job: %+v, %v, jobs %+v; want an immediate flush", ref, err, stub.jobs)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "d", Payload: []byte(`3`)}); err != nil || len(stub.jobs) != 1 {
		t.Fatalf("third job enqueued another flush: %v, %d", err, len(stub.jobs))
	}
}

// TestMemoryAggregate_ConcurrentAppendTake is the store contract under
// -race: appenders and takers on one group at once, then a final drain;
// every item comes out of exactly one Take, in append order per appender.
func TestMemoryAggregate_ConcurrentAppendTake(t *testing.T) {
	workflowtest.AggregateStoreContract(t, middleware.NewMemoryAggregate())
}

// TestAggregate_EndToEnd runs Aggregate on the memory driver with the
// Journal plugin: many goroutines enqueue items for a few groups at once,
// each flush run's step gets its items as a JSON array, and some flush
// steps fail once after taking their chunk. Every item is delivered
// exactly once, and a retried step sees the chunk it took.
func TestAggregate_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inner := memory.New()
	store := middleware.NewMemoryAggregate()
	plugin, err := middleware.AggregatePlugin(inner, store, middleware.AggregateConfig{
		Workflows: []string{"digest"}, Window: 20 * time.Millisecond, MaxItems: 7,
		Group: func(j workflow.Job) string { return j.Labels["user"] },
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := workflow.Use(inner, workflow.Journal(workflow.NewMemoryJournal()), plugin)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	delivered := map[string]int{}
	firstTry := map[workflow.RunID][]string{}
	var mismatch atomic.Int64
	err = c.Register(workflow.Workflow{Name: "digest", Retry: workflow.RetryPolicy{Max: 3}, Steps: []workflow.Step{{Name: "send", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var items []string
		if err := json.Unmarshal(in, &items); err != nil {
			return nil, workflow.Permanent(err)
		}
		info, _ := workflow.StepInfoFrom(ctx)
		mu.Lock()
		defer mu.Unlock()
		if info.Attempt == 1 && len(items) > 0 && len(firstTry)%2 == 0 {
			firstTry[info.RunID] = items
			return nil, errors.New("mail server busy") // retried: must see the same chunk
		}
		if prev, ok := firstTry[info.RunID]; ok && fmt.Sprint(prev) != fmt.Sprint(items) {
			mismatch.Add(1)
		}
		for _, it := range items {
			delivered[it]++
		}
		return nil, nil
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	const senders, per = 8, 25
	var wg sync.WaitGroup
	for s := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				p, _ := json.Marshal(fmt.Sprintf("%d-%d", s, i))
				job := workflow.Job{Workflow: "digest", Payload: p, Labels: map[string]string{"user": fmt.Sprint(i % 3)}}
				if _, err := c.Enqueue(ctx, job); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		if n == senders*per || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let any stray flush finish
	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != senders*per {
		t.Fatalf("delivered %d distinct items, want %d", len(delivered), senders*per)
	}
	for it, n := range delivered {
		if n != 1 {
			t.Fatalf("item %s delivered %d times", it, n)
		}
	}
	if mismatch.Load() != 0 || len(firstTry) == 0 {
		t.Fatalf("retried steps saw another chunk %d times (%d retried)", mismatch.Load(), len(firstTry))
	}
	for g := range 3 {
		if p := store.Pending(fmt.Sprint(g)); p != 0 {
			t.Fatalf("group %d still holds %d items", g, p)
		}
	}
}

// TestAggregatedItems_OutsideAFlushRun refuses a run without the group
// label, Permanent, before taking anything.
func TestAggregatedItems_OutsideAFlushRun(t *testing.T) {
	_, err := middleware.AggregatedItems(context.Background(), middleware.NewMemoryAggregate())
	if !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
		t.Fatalf("err = %v", err)
	}
	if g, ok := middleware.AggregateGroup(context.Background()); ok || g != "" {
		t.Fatal("a group outside a flush run")
	}
}
