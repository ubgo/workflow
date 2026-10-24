package redis_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	wfredis "github.com/ubgo/workflow/redis"
)

// ledger records step executions per run across several clients.
type ledger struct {
	mu      sync.Mutex
	runs    map[string]map[string]int // run -> step -> executions by live workers
	active  map[string]int            // run -> live executions right now
	overlap []string                  // runs two live workers executed at once
}

func newLedger() *ledger {
	return &ledger{runs: map[string]map[string]int{}, active: map[string]int{}}
}

// enter records the start of an execution and returns its end.
func (l *ledger) enter(run, step string) func() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.runs[run] == nil {
		l.runs[run] = map[string]int{}
	}
	l.runs[run][step]++
	l.active[run]++
	if l.active[run] > 1 {
		l.overlap = append(l.overlap, run+"/"+step)
	}
	return func() {
		l.mu.Lock()
		l.active[run]--
		l.mu.Unlock()
	}
}

// TestCrashedWorkerRunsResumeOncePerLease kills one of three workers
// while it holds runs mid-step: every run still succeeds, each step runs
// exactly once on the surviving workers, and no two live workers ever
// execute one run at the same time (a claim and a lease are exclusive).
func TestCrashedWorkerRunsResumeOncePerLease(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	const runs, victimWorkers = 24, 4
	led := newLedger()
	victimStarted := make(chan string, runs)
	blocked := make(chan struct{})

	wfFor := func(victim bool) workflow.Workflow {
		step := func(name string) workflow.Handler {
			return func(ctx context.Context, in []byte) ([]byte, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				if victim {
					victimStarted <- string(info.RunID)
					<-blocked // the process is gone: this attempt never returns in time
					return nil, fmt.Errorf("victim attempt of %s", info.RunID)
				}
				end := led.enter(string(info.RunID), name)
				defer end()
				time.Sleep(5 * time.Millisecond)
				return in, nil
			}
		}
		return workflow.Workflow{Name: "crash", Steps: []workflow.Step{
			{Name: "a", Run: step("a")},
			{Name: "b", After: []string{"a"}, Run: step("b")},
		}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	lease := wfredis.WithLease(time.Second)

	victim := newClient(t, rdb, prefix, lease, wfredis.WithWorkers(victimWorkers))
	// Registered after the client, so it runs before the client's Stop
	// (cleanups run last first) and frees the victim's blocked attempts.
	t.Cleanup(func() { close(blocked) })
	if err := victim.Register(wfFor(true)); err != nil {
		t.Fatal(err)
	}
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var refs []workflow.RunRef
	for i := range runs {
		ref, err := victim.Enqueue(ctx, workflow.Job{Workflow: "crash", Payload: fmt.Appendf(nil, `%d`, i)})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	held := map[string]bool{}
	for len(held) < victimWorkers {
		select {
		case id := <-victimStarted:
			held[id] = true
		case <-time.After(10 * time.Second):
			t.Fatalf("the victim started %d runs, want %d", len(held), victimWorkers)
		}
	}
	wfredis.Crash(victim)

	for range 2 {
		c := newClient(t, rdb, prefix, lease, wfredis.WithWorkers(3))
		if err := c.Register(wfFor(false)); err != nil {
			t.Fatal(err)
		}
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for _, ref := range refs {
		for {
			st, err := victim.Status(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if st == workflow.StatusSucceeded {
				break
			}
			if st.IsTerminal() || time.Now().After(deadline) {
				t.Fatalf("run %s is %s, want succeeded", ref.RunID, st)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	led.mu.Lock()
	defer led.mu.Unlock()
	if len(led.overlap) > 0 {
		t.Errorf("two live workers executed the same run at once: %v", led.overlap)
	}
	for _, ref := range refs {
		steps := led.runs[string(ref.RunID)]
		if steps["a"] != 1 || steps["b"] != 1 {
			t.Errorf("run %s: step executions %v on the live workers, want a=1 b=1", ref.RunID, steps)
		}
	}
	for id := range held {
		if led.runs[id]["a"] != 1 {
			t.Errorf("run %s, held by the crashed worker, was not taken over", id)
		}
	}
}

// TestLostLeaseWritesNothing: a worker that lost its lease (here its
// lease clock stands still, so its lease looks long expired to everyone
// else) finishes its step while another client, which took the run over,
// is still running that step. The late result must be fenced off: the
// next step runs once, with the new holder's output.
func TestLostLeaseWritesNothing(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	var mu sync.Mutex
	var bInputs []string
	slowStarted := make(chan struct{})
	release := make(chan struct{})
	liveStarted := make(chan struct{})
	liveGate := make(chan struct{})
	wfFor := func(name string, slow bool) workflow.Workflow {
		return workflow.Workflow{Name: "fence", Steps: []workflow.Step{
			{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				if slow {
					close(slowStarted)
					<-release
				} else {
					close(liveStarted)
					<-liveGate
				}
				return []byte(`"` + name + `"`), nil
			}},
			{Name: "b", After: []string{"a"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
				mu.Lock()
				bInputs = append(bInputs, string(in))
				mu.Unlock()
				return in, nil
			}},
		}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// The stale holder measures its lease on a clock far in the past and
	// never advanced: its claims look expired at once to the live client,
	// and its own renewals never fire.
	stale := newClient(t, rdb, prefix, wfredis.WithLeaseClock(clock.NewFake(time.Unix(0, 0))), wfredis.WithWorkers(1))
	if err := stale.Register(wfFor("stale", true)); err != nil {
		t.Fatal(err)
	}
	if err := stale.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := stale.Enqueue(ctx, workflow.Job{Workflow: "fence"})
	if err != nil {
		t.Fatal(err)
	}
	<-slowStarted
	live := newClient(t, rdb, prefix)
	if err := live.Register(wfFor("live", false)); err != nil {
		t.Fatal(err)
	}
	if err := live.Start(ctx); err != nil {
		t.Fatal(err)
	}
	<-liveStarted
	close(release) // the stale holder's attempt returns while the live one runs
	time.Sleep(300 * time.Millisecond)
	close(liveGate)
	waitStatus(t, live, ref, workflow.StatusSucceeded)
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(bInputs) != 1 || bInputs[0] != `"live"` {
		t.Fatalf("step b ran with inputs %q, want once with the live holder's output", bInputs)
	}
	if st, err := live.Status(ctx, ref); err != nil || st != workflow.StatusSucceeded {
		t.Fatalf("status after the stale write = %s, %v; want succeeded", st, err)
	}
}

// waitStatus polls until ref reaches want, failing on another terminal
// status or after 10s.
func waitStatus(t *testing.T, c workflow.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := c.Status(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if st == want {
			return
		}
		if st.IsTerminal() || time.Now().After(deadline) {
			t.Fatalf("run %+v is %s, want %s", ref, st, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCronTickFiresOnceAcrossClients: three clients over one prefix and
// one scheduling clock race to fire every tick of a schedule; each tick
// makes exactly one run.
func TestCronTickFiresOnceAcrossClients(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	clk := clock.NewFake(start)
	var mu sync.Mutex
	runs := 0
	wf := workflow.Workflow{Name: "tick", Steps: []workflow.Step{{Name: "only", Run: func(_ context.Context, in []byte) ([]byte, error) {
		mu.Lock()
		runs++
		mu.Unlock()
		return in, nil
	}}}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var first *wfredis.Client
	for i := range 3 {
		c := newClient(t, rdb, prefix, wfredis.WithClock(clk))
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = c
		}
	}
	if _, err := first.Schedule(ctx, workflow.Schedule{Name: "every", Workflow: "tick", Cron: "* * * * *", Location: time.UTC}); err != nil {
		t.Fatal(err)
	}
	const ticks = 5
	for i := 1; i <= ticks; i++ {
		time.Sleep(50 * time.Millisecond) // every client has armed its timers
		clk.Advance(time.Minute)
		deadline := time.Now().Add(5 * time.Second)
		for {
			mu.Lock()
			n := runs
			mu.Unlock()
			if n >= i || time.Now().After(deadline) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if runs != ticks {
		t.Fatalf("%d ticks made %d runs across three clients, want %d", ticks, runs, ticks)
	}
}
