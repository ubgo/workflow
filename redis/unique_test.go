package redis_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
	wfredis "github.com/ubgo/workflow/redis"
)

var (
	_ middleware.UniqueStore   = (*wfredis.UniqueStore)(nil)
	_ middleware.UniqueHolders = (*wfredis.UniqueStore)(nil)
)

// TestUniqueStoreOneWinner races twenty acquirers on one key: one gets
// it, and once released a new owner can; another owner's release is a
// no-op.
func TestUniqueStoreOneWinner(t *testing.T) {
	rdb := redisClient(t)
	u, err := wfredis.NewUniqueStore(rdb, testPrefix(t, rdb))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var won atomic.Int32
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := u.Acquire(ctx, "k", "owner-"+string(rune('a'+i)), time.Minute)
			if err != nil {
				t.Error(err)
			}
			if ok {
				won.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := won.Load(); n != 1 {
		t.Fatalf("%d acquirers won one key, want 1", n)
	}
	if err := u.Release(ctx, "k", "nobody"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := u.Acquire(ctx, "k", "late", time.Minute); ok {
		t.Fatal("a release by another owner freed the key")
	}
	if ok, _ := u.Acquire(ctx, "short", "a", 50*time.Millisecond); !ok {
		t.Fatal("acquire of a free key failed")
	}
	time.Sleep(120 * time.Millisecond)
	if ok, _ := u.Acquire(ctx, "short", "b", time.Minute); !ok {
		t.Fatal("an expired lock was still held")
	}
}

// TestUniqueWithReturnExisting runs UniqueWith on the Redis store across
// two clients: the second enqueue of a key returns the first run, marked a
// duplicate, and once that run has started (States: queued only) a new
// job is let in.
func TestUniqueWithReturnExisting(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	wf := workflow.Workflow{Name: "u", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return in, nil
	}}}}
	a := startOn(t, rdb, prefix, []workflow.Workflow{wf}, wfredis.WithWorkers(1))
	t.Cleanup(func() { close(release) })
	mw, err := middleware.UniqueWith(middleware.UniqueConfig{By: func(workflow.Job) string { return "one" }, Store: a.UniqueStore(),
		TTL: time.Minute, ReturnExisting: true, States: []workflow.Status{workflow.StatusQueued, workflow.StatusScheduled}, Client: a})
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(a, mw)
	ctx := context.Background()
	first, err := c.Enqueue(ctx, workflow.Job{Workflow: "u"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	// The holder is running, which States does not list: a new job is let in.
	second, err := c.Enqueue(ctx, workflow.Job{Workflow: "u"})
	if err != nil {
		t.Fatal(err)
	}
	if second.RunID == first.RunID {
		t.Fatal("a job was answered with a running holder that States does not count")
	}
	// The second run waits for the single busy worker: it is queued, so a
	// third job is its duplicate.
	octx, out := workflow.WithEnqueueOutcome(ctx)
	third, err := c.Enqueue(octx, workflow.Job{Workflow: "u"})
	if err != nil {
		t.Fatal(err)
	}
	if third.RunID != second.RunID || !out.Duplicate {
		t.Fatalf("third enqueue = %+v (duplicate %v), want the queued holder %+v", third, out.Duplicate, second)
	}
}
