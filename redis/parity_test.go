package redis_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	wfredis "github.com/ubgo/workflow/redis"
)

// waitFor polls cond every few milliseconds until it holds or d passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func statusOf(t *testing.T, c workflow.Client, ref workflow.RunRef) workflow.Status {
	t.Helper()
	st, err := c.Status(context.Background(), ref)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	return st
}

// TestSignalBeforeEnqueueIsNotTaken pins the signal floor: a signal sent
// before a run was enqueued never resumes that run's wait, even while it
// is still kept (housekeeping has not dropped it), and one sent after
// does.
func TestSignalBeforeEnqueueIsNotTaken(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	c := newClient(t, rdb, prefix)
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "wait", Wait: &workflow.WaitSpec{Event: "ev", Key: "k"}},
		{Name: "after", After: []string{"wait"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
	}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the first housekeeping pass is over; the next is a minute away
	if err := c.Signal(ctx, workflow.Event{Name: "ev", Key: "k", Payload: []byte(`"stale"`)}); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the run to wait", func() bool { return statusOf(t, c, ref) == workflow.StatusWaiting })
	time.Sleep(100 * time.Millisecond)
	if st := statusOf(t, c, ref); st != workflow.StatusWaiting {
		t.Fatalf("a signal sent before the enqueue moved the run to %s", st)
	}
	if err := c.Signal(ctx, workflow.Event{Name: "ev", Key: "k", Payload: []byte(`"fresh"`)}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the run to succeed", func() bool { return statusOf(t, c, ref) == workflow.StatusSucceeded })
}

// TestConcurrencyAcrossClients runs one bucket of MaxRuns 1 on three
// clients sharing a prefix: no two runs of the bucket ever execute at
// once, and every run finishes.
func TestConcurrencyAcrossClients(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	var active, peak atomic.Int32
	wf := workflow.Workflow{Name: "conc", Concurrency: &workflow.Concurrency{MaxRuns: 1},
		Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) {
			n := active.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
			return in, nil
		}}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var clients []*wfredis.Client
	for range 3 {
		c := newClient(t, rdb, prefix)
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
	}
	var refs []workflow.RunRef
	for i := range 12 {
		ref, err := clients[i%3].Enqueue(ctx, workflow.Job{Workflow: "conc"})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	for _, ref := range refs {
		waitFor(t, 20*time.Second, "every run to succeed", func() bool { return statusOf(t, clients[0], ref) == workflow.StatusSucceeded })
	}
	if p := peak.Load(); p != 1 {
		t.Fatalf("%d runs of one bucket ran at once across clients, want 1", p)
	}
}

// TestRunEndsDeliveredAfterRestart ends runs while no client with an
// observer runs, then starts one: it delivers every end once it is up.
func TestRunEndsDeliveredAfterRestart(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	ctx, cancel := context.WithCancel(context.Background())
	first := newClient(t, rdb, prefix)
	if err := first.Register(wf); err != nil {
		t.Fatal(err)
	}
	if err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var refs []workflow.RunRef
	for range 3 {
		ref, err := first.Enqueue(ctx, workflow.Job{Workflow: "w"})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	for _, ref := range refs {
		waitFor(t, 5*time.Second, "the run to succeed", func() bool { return statusOf(t, first, ref) == workflow.StatusSucceeded })
	}
	cancel()
	<-first.Done()

	second := newClient(t, rdb, prefix)
	var mu sync.Mutex
	got := map[workflow.RunID]int{}
	if err := second.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
		mu.Lock()
		got[ev.Ref.RunID]++
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	if err := second.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := second.Start(ctx2); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "every end to be delivered", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == len(refs)
	})
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for _, ref := range refs {
		if got[ref.RunID] != 1 {
			t.Errorf("run %s delivered %d times, want once", ref.RunID, got[ref.RunID])
		}
	}
}

// TestQueueWorkerCap gives a client one worker slot on a queue: two runs
// of that queue never execute at once on it, while it has more workers.
func TestQueueWorkerCap(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	var active, peak atomic.Int32
	wf := workflow.Workflow{Name: "q", Queue: "slow", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		active.Add(-1)
		return in, nil
	}}}}
	c := newClient(t, rdb, prefix, wfredis.WithWorkers(4), wfredis.WithQueues(workflow.QueueConfig{Name: "slow", Workers: 1}))
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var refs []workflow.RunRef
	for range 8 {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "q"})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	for _, ref := range refs {
		waitFor(t, 10*time.Second, "every run to succeed", func() bool { return statusOf(t, c, ref) == workflow.StatusSucceeded })
	}
	if p := peak.Load(); p != 1 {
		t.Fatalf("%d runs of a queue capped at 1 ran at once, want 1", p)
	}
}

// TestUnservedQueueWaits gives a client one queue: a run of another queue
// stays queued until a client serving that queue starts.
func TestUnservedQueueWaits(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := newClient(t, rdb, prefix, wfredis.WithQueues(workflow.QueueConfig{Name: "a"}))
	if err := a.Register(wf); err != nil {
		t.Fatal(err)
	}
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := a.Enqueue(ctx, workflow.Job{Workflow: "w", Queue: "b"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if st := statusOf(t, a, ref); st != workflow.StatusQueued {
		t.Fatalf("a run of queue b on a client serving only a is %s, want queued", st)
	}
	b := newClient(t, rdb, prefix, wfredis.WithQueues(workflow.QueueConfig{Name: "b"}))
	if err := b.Register(wf); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the run to succeed on the client serving b", func() bool { return statusOf(t, a, ref) == workflow.StatusSucceeded })
}

// startOn registers wfs on a fresh client over prefix and starts it; the
// test's cleanup stops it.
func startOn(t *testing.T, rdb goredis.UniversalClient, prefix string, wfs []workflow.Workflow, opts ...wfredis.Option) *wfredis.Client {
	t.Helper()
	c := newClient(t, rdb, prefix, opts...)
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
	return c
}

// TestRunTimeoutWhileSleeping times out a run that is sleeping, held by no
// worker: the run fails with ErrRunTimeout once its deadline passes and
// OnTimeout runs.
func TestRunTimeoutWhileSleeping(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	clk := clock.NewFake(start)
	var timedOut atomic.Int32
	wf := workflow.Workflow{Name: "slow", RunTimeout: time.Hour,
		OnTimeout: func(_ context.Context, in []byte) ([]byte, error) { timedOut.Add(1); return in, nil },
		Steps: []workflow.Step{
			{Name: "nap", Sleep: 3 * time.Hour},
			{Name: "after", After: []string{"nap"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
		}}
	c := startOn(t, rdb, prefix, []workflow.Workflow{wf}, wfredis.WithClock(clk))
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "slow"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the run to sleep", func() bool { return statusOf(t, c, ref) == workflow.StatusWaiting })
	clk.Advance(61 * time.Minute)
	waitFor(t, 5*time.Second, "the run to time out", func() bool { return statusOf(t, c, ref) == workflow.StatusFailed })
	page, err := c.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs: %v, %v", page.Runs, err)
	}
	if !strings.Contains(page.Runs[0].Error, workflow.ErrRunTimeout.Error()) {
		t.Errorf("Error = %q, want the run timeout", page.Runs[0].Error)
	}
	waitFor(t, 5*time.Second, "OnTimeout to run", func() bool { return timedOut.Load() == 1 })
}

// TestKeyFIFOUnderRateLimit holds a FIFO key's later run behind an earlier
// one a rate limit holds back, even when the later run's own rate key has
// room.
func TestKeyFIFOUnderRateLimit(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	clk := clock.NewFake(start)
	var mu sync.Mutex
	var order []string
	wf := workflow.Workflow{Name: "fifo",
		Concurrency: &workflow.Concurrency{MaxRuns: 10, FIFO: true},
		RateLimits:  []workflow.RateLimit{{Key: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["r"] }), Limit: 1, Per: time.Hour}},
		Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) {
			mu.Lock()
			order = append(order, string(in))
			mu.Unlock()
			return in, nil
		}}}}
	// One worker, so the start order is the claim order.
	c := startOn(t, rdb, prefix, []workflow.Workflow{wf}, wfredis.WithClock(clk), wfredis.WithWorkers(1))
	ctx := context.Background()
	var refs []workflow.RunRef
	for _, j := range []struct{ name, r string }{{"1", "x"}, {"2", "x"}, {"3", "y"}} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "fifo", Payload: []byte(j.name), Labels: map[string]string{"r": j.r}})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	waitFor(t, 5*time.Second, "the first run", func() bool { return statusOf(t, c, refs[0]) == workflow.StatusSucceeded })
	time.Sleep(150 * time.Millisecond)
	if st := statusOf(t, c, refs[2]); st != workflow.StatusQueued {
		t.Fatalf("run 3 is %s while run 2, before it on the FIFO key, waits on its rate limit; want queued", st)
	}
	clk.Advance(61 * time.Minute)
	for _, ref := range refs[1:] {
		waitFor(t, 5*time.Second, "the held runs", func() bool { return statusOf(t, c, ref) == workflow.StatusSucceeded })
	}
	mu.Lock()
	defer mu.Unlock()
	if got := strings.Join(order, ","); got != "1,2,3" {
		t.Fatalf("start order %s, want 1,2,3", got)
	}
}

// TestCancelStepInBackoff withdraws a step waiting on its retry backoff:
// the run fails at once with ErrStepCancelled, without the retry.
func TestCancelStepInBackoff(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	clk := clock.NewFake(start)
	var attempts atomic.Int32
	wf := workflow.Workflow{Name: "flaky", Retry: workflow.RetryPolicy{Max: 5, Backoff: workflow.Backoff{Initial: time.Hour, Factor: 1}},
		Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
			attempts.Add(1)
			return nil, errors.New("boom")
		}}}}
	c := startOn(t, rdb, prefix, []workflow.Workflow{wf}, wfredis.WithClock(clk))
	ref, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "flaky"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the backoff", func() bool { return statusOf(t, c, ref) == workflow.StatusRetrying })
	if err := c.CancelStep(context.Background(), ref, "s"); err != nil {
		t.Fatalf("CancelStep: %v", err)
	}
	waitFor(t, 5*time.Second, "the run to fail", func() bool { return statusOf(t, c, ref) == workflow.StatusFailed })
	if n := attempts.Load(); n != 1 {
		t.Fatalf("%d attempts, want 1 (the withdrawn step must not retry)", n)
	}
}

// TestWeightedQueues pins the weighted draw: of two queues of equal
// priority with ready work, the one PickWeighted picks for the draw goes
// first, whatever the age of their runs.
func TestWeightedQueues(t *testing.T) {
	for _, tc := range []struct {
		draw  float64
		first string
	}{{0.9, "b"}, {0.1, "a"}} {
		rdb := redisClient(t)
		prefix := testPrefix(t, rdb)
		var mu sync.Mutex
		var order []string
		step := func(_ context.Context, in []byte) ([]byte, error) {
			mu.Lock()
			order = append(order, string(in))
			mu.Unlock()
			return in, nil
		}
		wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: step}}}
		c := startOn(t, rdb, prefix, []workflow.Workflow{wf}, wfredis.WithWorkers(1),
			wfredis.WithJitterSource(func() float64 { return tc.draw }),
			wfredis.WithQueues(workflow.QueueConfig{Name: "a", Weight: 3}, workflow.QueueConfig{Name: "b", Weight: 1}))
		ctx := context.Background()
		if err := c.Pause(ctx, workflow.PauseAll()); err != nil {
			t.Fatal(err)
		}
		var refs []workflow.RunRef
		for _, q := range []string{"a", "b"} {
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Queue: q, Payload: []byte(q)})
			if err != nil {
				t.Fatal(err)
			}
			refs = append(refs, ref)
		}
		if err := c.Resume(ctx, workflow.PauseAll()); err != nil {
			t.Fatal(err)
		}
		for _, ref := range refs {
			waitFor(t, 5*time.Second, "both runs", func() bool { return statusOf(t, c, ref) == workflow.StatusSucceeded })
		}
		mu.Lock()
		if order[0] != tc.first {
			t.Errorf("draw %v: queue %s went first, want %s (weights a=3, b=1)", tc.draw, order[0], tc.first)
		}
		mu.Unlock()
	}
}

// TestOverlapSkipFiresOneOfSeveralTicks catches up three missed ticks of
// an OverlapSkip schedule at once: the first fires, and the run it starts
// holds the other two back, reported as skipped for overlap.
func TestOverlapSkipFiresOneOfSeveralTicks(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	clk := clock.NewFake(start)
	gate := make(chan struct{})
	t.Cleanup(func() { close(gate) })
	var runs atomic.Int32
	wf := workflow.Workflow{Name: "cron", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		runs.Add(1)
		select {
		case <-gate:
		case <-ctx.Done():
		}
		return in, nil
	}}}}
	var mu sync.Mutex
	var ticks []workflow.Tick
	c := newClient(t, rdb, prefix, wfredis.WithClock(clk))
	if err := c.OnTick(func(_ context.Context, tk workflow.Tick) { mu.Lock(); ticks = append(ticks, tk); mu.Unlock() }); err != nil {
		t.Fatal(err)
	}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "cron", Cron: "* * * * *", Location: time.UTC, Overlap: workflow.OverlapSkip,
		Misfire: workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 10}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	clk.Advance(3*time.Minute + time.Second)
	waitFor(t, 5*time.Second, "the three ticks to be reported", func() bool { mu.Lock(); defer mu.Unlock(); return len(ticks) >= 3 })
	time.Sleep(100 * time.Millisecond)
	if n := runs.Load(); n != 1 {
		t.Fatalf("%d runs started, want 1", n)
	}
	mu.Lock()
	defer mu.Unlock()
	fired, skipped := 0, 0
	for _, tk := range ticks {
		switch {
		case tk.Outcome == workflow.TickFired:
			fired++
		case tk.Outcome == workflow.TickSkipped && tk.Reason == workflow.TickReasonOverlap:
			skipped++
		}
	}
	if fired != 1 || skipped != 2 {
		t.Fatalf("ticks %+v: want 1 fired and 2 skipped for overlap", ticks)
	}
}

// TestSchedulerAndWorkerNodes splits a cluster: a worker node
// (WithoutScheduler) alone never fires a schedule; a scheduler node
// (WithSchedulerOnly) fires it and runs nothing, and the worker runs it.
func TestSchedulerAndWorkerNodes(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	clk := clock.NewFake(start)
	var mu sync.Mutex
	var nodes []string
	wf := workflow.Workflow{Name: "cron", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		mu.Lock()
		nodes = append(nodes, info.Node)
		mu.Unlock()
		return in, nil
	}}}}
	worker := startOn(t, rdb, prefix, []workflow.Workflow{wf}, wfredis.WithClock(clk), wfredis.WithoutScheduler(), wfredis.WithNodeID("worker"))
	ctx := context.Background()
	if _, err := worker.Schedule(ctx, workflow.Schedule{Workflow: "cron", Cron: "* * * * *", Location: time.UTC}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	clk.Advance(time.Minute)
	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	n := len(nodes)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("a worker-only node fired the schedule (%d runs)", n)
	}
	startOn(t, rdb, prefix, []workflow.Workflow{wf}, wfredis.WithClock(clk), wfredis.WithSchedulerOnly(), wfredis.WithNodeID("scheduler"))
	waitFor(t, 5*time.Second, "the scheduler node to fire the missed tick", func() bool { mu.Lock(); defer mu.Unlock(); return len(nodes) >= 1 })
	mu.Lock()
	defer mu.Unlock()
	for _, n := range nodes {
		if n != "worker" {
			t.Fatalf("a run ran on node %q, want the worker node", n)
		}
	}
}
