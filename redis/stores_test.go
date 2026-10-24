package redis_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
	wfredis "github.com/ubgo/workflow/redis"
)

// TestJournalStore checks the JournalStore contract: Load of nothing,
// Save replaces, entries are per run, Forget drops a run's entries and is
// fine on a run with none.
func TestJournalStore(t *testing.T) {
	rdb := redisClient(t)
	j, err := wfredis.NewJournalStore(rdb, testPrefix(t, rdb), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, ok, err := j.Load(ctx, "r1", "k"); ok || err != nil {
		t.Fatalf("Load of nothing = %v, %v", ok, err)
	}
	for _, v := range []string{"one", "two"} {
		if err := j.Save(ctx, "r1", "k", []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Save(ctx, "r2", "k", []byte("other")); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := j.Load(ctx, "r1", "k"); !ok || err != nil || !bytes.Equal(v, []byte("two")) {
		t.Fatalf("Load = %q, %v, %v; want the last Save", v, ok, err)
	}
	if err := j.Save(ctx, "r1", "empty", nil); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := j.Load(ctx, "r1", "empty"); !ok || len(v) != 0 {
		t.Fatalf("an empty value = %q, %v; want saved and empty", v, ok)
	}
	if err := j.Forget(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := j.Load(ctx, "r1", "k"); ok {
		t.Fatal("Forget kept an entry")
	}
	if v, ok, _ := j.Load(ctx, "r2", "k"); !ok || string(v) != "other" {
		t.Fatal("Forget of one run dropped another's entry")
	}
	if err := j.Forget(ctx, "never"); err != nil {
		t.Fatalf("Forget of a run with no entries: %v", err)
	}
}

// TestBucketStoreMatchesMemory drives the numbers of the core's leaky
// bucket test through UpstreamLimit on the Redis store.
func TestBucketStoreMatchesMemory(t *testing.T) {
	rdb := redisClient(t)
	b, err := wfredis.NewBucketStore(rdb, testPrefix(t, rdb))
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC))
	lim, err := workflow.NewUpstreamLimit(b, workflow.BucketSpec{Capacity: 4, LeakPerSecond: 2}, clk)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for range 4 {
		if err := lim.Take(ctx, "shop-1", 1); err != nil {
			t.Fatal(err)
		}
	}
	if w, _ := lim.Wait(ctx, "shop-1", 1); w != 500*time.Millisecond {
		t.Fatalf("full bucket wait = %v, want 500ms", w)
	}
	if err := lim.Take(ctx, "shop-2", 4); err != nil {
		t.Fatalf("another key shares the bucket: %v", err)
	}
	clk.Advance(500 * time.Millisecond)
	if err := lim.Take(ctx, "shop-1", 1); err != nil {
		t.Fatalf("after leak: %v", err)
	}
	clk.Advance(time.Second) // level 4 - 2 = 2
	if w, _ := lim.Wait(ctx, "shop-1", 3); w != 500*time.Millisecond {
		t.Fatalf("wait for cost 3 = %v, want 500ms", w)
	}
	if err := lim.Observe(ctx, "shop-1", 0); err != nil {
		t.Fatal(err)
	}
	if err := lim.Take(ctx, "shop-1", 4); err != nil {
		t.Fatalf("after observe 0: %v", err)
	}
	if err := lim.Observe(ctx, "shop-1", 99); err != nil {
		t.Fatal(err)
	}
	if w, _ := b.Take(ctx, "shop-1", workflow.BucketSpec{Capacity: 4, LeakPerSecond: 2}, 1, clk.Now()); w != 500*time.Millisecond {
		t.Fatalf("after observe above capacity: wait %v, want 500ms (clamped to 4)", w)
	}
}

// TestBucketStoreTakeIsAtomic has 50 takers race for a bucket of 10 on
// one instant: exactly 10 fit.
func TestBucketStoreTakeIsAtomic(t *testing.T) {
	rdb := redisClient(t)
	b, err := wfredis.NewBucketStore(rdb, testPrefix(t, rdb))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	spec := workflow.BucketSpec{Capacity: 10, LeakPerSecond: 1}
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := b.Take(context.Background(), "k", spec, 1, now)
			if err != nil {
				t.Error(err)
			}
			if w == 0 {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := admitted.Load(); n != 10 {
		t.Fatalf("%d takes fit a bucket of 10, want 10", n)
	}
}

// TestDebounceStoreSwap checks Get and Swap, and that 20 racing swaps
// each get back a different previous burst.
func TestDebounceStoreSwap(t *testing.T) {
	rdb := redisClient(t)
	d, err := wfredis.NewDebounceStore(rdb, testPrefix(t, rdb))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, ok, err := d.Get(ctx, "k"); ok || err != nil {
		t.Fatalf("Get of nothing = %v, %v", ok, err)
	}
	first := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	b1 := middleware.DebounceBurst{First: first, RunAt: first.Add(time.Minute), Ref: workflow.RunRef{ScheduledID: "s-1"}}
	if _, found, err := d.Swap(ctx, "k", b1); found || err != nil {
		t.Fatalf("first Swap found %v, %v", found, err)
	}
	got, ok, err := d.Get(ctx, "k")
	if !ok || err != nil || !got.First.Equal(b1.First) || !got.RunAt.Equal(b1.RunAt) || got.Ref != b1.Ref {
		t.Fatalf("Get = %+v, %v, %v; want %+v", got, ok, err, b1)
	}
	var mu sync.Mutex
	seen := map[workflow.ScheduledID]int{}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := middleware.DebounceBurst{First: first, RunAt: first.Add(time.Minute), Ref: workflow.RunRef{ScheduledID: workflow.ScheduledID("s-" + string(rune('A'+i)))}}
			prev, found, err := d.Swap(ctx, "k", b)
			if err != nil || !found {
				t.Errorf("Swap = %v, %v", found, err)
				return
			}
			mu.Lock()
			seen[prev.Ref.ScheduledID]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("burst %s was handed back %d times, want once", id, n)
		}
	}
	if len(seen) != 20 {
		t.Fatalf("%d distinct previous bursts, want 20", len(seen))
	}
}

// TestQueueLoads reports a configured queue's depth and oldest wait on
// the scheduling clock, and SetWorkers changes its cap.
func TestQueueLoads(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	clk := clock.NewFake(start)
	wf := workflow.Workflow{Name: "w", Queue: "q", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	c := startOn(t, rdb, prefix, []workflow.Workflow{wf}, wfredis.WithClock(clk), wfredis.WithQueues(workflow.QueueConfig{Name: "q", Workers: 2}))
	ctx := context.Background()
	if err := c.Pause(ctx, workflow.PauseQueue("q")); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"}); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(5 * time.Minute)
	loads, err := c.QueueLoads(ctx)
	if err != nil || len(loads) != 1 {
		t.Fatalf("QueueLoads = %v, %v", loads, err)
	}
	if l := loads[0]; l.Queue != "q" || l.Depth != 3 || l.OldestWait != 5*time.Minute || l.Workers != 2 {
		t.Fatalf("load = %+v, want q depth 3, oldest wait 5m, 2 workers", l)
	}
	if err := c.SetWorkers(ctx, "q", 4); err != nil {
		t.Fatal(err)
	}
	if err := c.Resume(ctx, workflow.PauseQueue("q")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the queue to drain", func() bool {
		l, err := c.QueueLoads(ctx)
		return err == nil && l[0].Depth == 0 && l[0].OldestWait == 0 && l[0].Workers == 4
	})
}
