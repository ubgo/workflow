package redis_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	wfredis "github.com/ubgo/workflow/redis"
)

// TestPoolOverRedisUniqueStore proves middleware.Pool on the Redis lock
// store: two processes, each with its own UniqueStore on one Redis,
// share a pool of two slots, and at most two steps ever run at once
// across both while every run succeeds.
func TestPoolOverRedisUniqueStore(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	newStore := func() middleware.UniqueStore {
		u, err := wfredis.NewUniqueStore(rdb, prefix)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	if peak := poolPeak(t, newStore, 2, 4); peak != 2 {
		t.Fatalf("peak concurrency %d across two processes, want exactly 2", peak)
	}
}

// poolPeak runs n runs of one step on each of two memory clients, each
// wrapping its step in Pool over its own store from newStore (two
// processes sharing one durable store), and returns the most steps that
// ran at once across both.
func poolPeak(t *testing.T, newStore func() middleware.UniqueStore, slots, n int) int32 {
	t.Helper()
	var cur, peak atomic.Int32
	body := func(context.Context, []byte) ([]byte, error) {
		now := cur.Add(1)
		for {
			p := peak.Load()
			if now <= p || peak.CompareAndSwap(p, now) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		return nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type proc struct {
		c    *memory.Client
		refs []workflow.RunRef
	}
	var procs []*proc
	for range 2 {
		pool := middleware.Pool(newStore(), "licence", slots, time.Minute, middleware.PoolRetry(5*time.Millisecond))
		m := memory.New(memory.WithWorkers(n), memory.WithMiddleware(pool))
		if err := m.Register(workflow.Workflow{Name: "render", Steps: []workflow.Step{{Name: "seat", Run: body}}}); err != nil {
			t.Fatal(err)
		}
		if err := m.Start(ctx); err != nil {
			t.Fatal(err)
		}
		defer m.Stop()
		procs = append(procs, &proc{c: m})
	}
	for _, p := range procs {
		for range n {
			ref, err := p.c.Enqueue(ctx, workflow.Job{Workflow: "render"})
			if err != nil {
				t.Fatal(err)
			}
			p.refs = append(p.refs, ref)
		}
	}
	wctx, wcancel := context.WithTimeout(ctx, 20*time.Second)
	defer wcancel()
	for _, p := range procs {
		for _, ref := range p.refs {
			st, err := workflow.Await(wctx, p.c, ref, workflow.AwaitOptions{Poll: 2 * time.Millisecond})
			if err != nil || st != workflow.StatusSucceeded {
				t.Fatalf("run ended %q: %v", st, err)
			}
		}
	}
	return peak.Load()
}
