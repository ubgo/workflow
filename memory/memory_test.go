package memory_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is a fixed Monday, 2026-01-05 10:00 UTC, so cron maths in tests is
// predictable.
var start = time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)

// pollFor is how long a test waits for an asynchronous state change before
// failing. It only bounds a broken test; passing tests finish far sooner.
const pollFor = 5 * time.Second

func newClient(t *testing.T, wfs []workflow.Workflow, opts ...memory.Option) (*memory.Client, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(start)
	c := memory.New(append([]memory.Option{memory.WithClock(fc)}, opts...)...)
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatalf("Register(%s): %v", wf.Name, err)
		}
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(c.Stop)
	return c, fc
}

func one(name string, h workflow.Handler) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: h}}}
}

func ok(context.Context, []byte) ([]byte, error) { return nil, nil }

// block returns a handler that signals started, then waits for release or
// the run's cancellation.
func block(started chan<- string, release <-chan struct{}) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		started <- string(in)
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func enqueue(t *testing.T, c *memory.Client, job workflow.Job) workflow.RunRef {
	t.Helper()
	ref, err := c.Enqueue(context.Background(), job)
	if err != nil {
		t.Fatalf("Enqueue(%+v): %v", job, err)
	}
	return ref
}

func waitStatus(t *testing.T, c *memory.Client, ref workflow.RunRef, want workflow.Status) {
	t.Helper()
	deadline := time.Now().Add(pollFor)
	var got workflow.Status
	for time.Now().Before(deadline) {
		var err error
		got, err = c.Status(context.Background(), ref)
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("run %+v status %q, want %q", ref, got, want)
}

func blockUntil(t *testing.T, fc *clock.Fake, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), pollFor)
	defer cancel()
	if err := fc.BlockUntil(ctx, n); err != nil {
		t.Fatalf("waiting for %d timers (have %d): %v", n, fc.Pending(), err)
	}
}

func TestLifecycle(t *testing.T) {
	c := memory.New(memory.WithClock(clock.NewFake(start)))
	defer c.Stop()
	ctx := context.Background()
	if c.Capabilities().Has(workflow.CapDurable) {
		t.Fatal("memory driver must not claim CapDurable")
	}
	if !c.Capabilities().Has(workflow.CapCron | workflow.CapWaitForEvent | workflow.CapDelayedStart) {
		t.Fatalf("missing caps: %s", c.Capabilities())
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Enqueue before Start: %v", err)
	}
	if err := c.Signal(ctx, workflow.Event{Name: "e"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Signal before Start: %v", err)
	}
	if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Schedule before Start: %v", err)
	}
	badCron := one("cron", ok)
	badCron.Cron = []string{"61 * * * *"}
	if err := c.Register(badCron); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("bad cron: %v", err)
	}
	if err := c.Register(one("w", ok)); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); !errors.Is(err, workflow.ErrAlreadyStarted) {
		t.Fatalf("second Start: %v", err)
	}
	if err := c.Register(one("late", ok)); !errors.Is(err, workflow.ErrAlreadyStarted) {
		t.Fatalf("Register after Start: %v", err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "nope"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("unknown workflow: %v", err)
	}
	if _, err := c.Status(ctx, workflow.RunRef{RunID: "x"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("unknown run: %v", err)
	}
	if err := c.Cancel(ctx, workflow.RunRef{RunID: "x"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("cancel unknown run: %v", err)
	}
}

// capless is a driver stand-in proving Start uses the core capability
// check: the memory driver itself lacks only CapDurable, which Requires
// never infers, so the refusal path is exercised through CheckCaps here.
func TestStartRefusesMissingCaps(t *testing.T) {
	wf := one("w", ok)
	wf.Cron = []string{"@hourly"}
	if err := workflow.CheckCaps(memory.New().Capabilities(), wf); err != nil {
		t.Fatalf("memory caps refuse a cron workflow: %v", err)
	}
	if err := workflow.CheckCaps(memory.New().Capabilities()&^workflow.CapCron, wf); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("CheckCaps without Cron: %v", err)
	}
}

func TestDAGPassesOutputs(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{}
	rec := func(name, out string) workflow.Handler {
		return func(_ context.Context, in []byte) ([]byte, error) {
			mu.Lock()
			got[name] = string(in)
			mu.Unlock()
			return []byte(out), nil
		}
	}
	wf := workflow.Workflow{Name: "dag", Steps: []workflow.Step{
		{Name: "a", Run: rec("a", `{"n":1}`)},
		{Name: "b", After: []string{"a"}, Run: rec("b", `{"b":true}`)},
		{Name: "c", After: []string{"a"}, Run: rec("c", "plain")},
		{Name: "d", After: []string{"b", "c"}, Run: rec("d", "")},
	}}
	c, _ := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "dag", Payload: []byte("payload")})
	waitStatus(t, c, ref, workflow.StatusSucceeded)

	mu.Lock()
	defer mu.Unlock()
	if got["a"] != "payload" || got["b"] != `{"n":1}` || got["c"] != `{"n":1}` {
		t.Fatalf("inputs: %v", got)
	}
	var d map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got["d"]), &d); err != nil {
		t.Fatalf("fan-in input %q is not JSON: %v", got["d"], err)
	}
	if string(d["b"]) != `{"b":true}` || string(d["c"]) != `"plain"` {
		t.Fatalf("fan-in input: %s", got["d"])
	}
}

func TestWaitForSignal(t *testing.T) {
	var out atomic.Value
	wf := workflow.Workflow{Name: "approve", Steps: []workflow.Step{
		{Name: "wait", Wait: &workflow.WaitSpec{Event: "approved", Key: "order-1"}},
		{Name: "after", After: []string{"wait"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			out.Store(string(in))
			return nil, nil
		}},
	}}
	c, _ := newClient(t, []workflow.Workflow{wf})
	ctx := context.Background()
	ref := enqueue(t, c, workflow.Job{Workflow: "approve"})
	waitStatus(t, c, ref, workflow.StatusWaiting)
	if err := c.Signal(ctx, workflow.Event{Name: "approved", Key: "order-2", Payload: []byte("wrong")}); err != nil {
		t.Fatal(err)
	}
	if err := c.Signal(ctx, workflow.Event{Name: "approved", Key: "order-1", Payload: []byte("yes")}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	if out.Load() != "yes" {
		t.Fatalf("step after the wait got %v", out.Load())
	}
}

func TestWaitTimeoutFailsAndRunsOnFailureOnce(t *testing.T) {
	var failures atomic.Int32
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "wait", Wait: &workflow.WaitSpec{Event: "never", Timeout: 48 * time.Hour}},
	}, OnFailure: func(context.Context, []byte) ([]byte, error) {
		failures.Add(1)
		return nil, nil
	}}
	c, fc := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, ref, workflow.StatusWaiting)
	blockUntil(t, fc, 1)
	fc.Advance(48 * time.Hour)
	waitStatus(t, c, ref, workflow.StatusFailed)
	if n := failures.Load(); n != 1 {
		t.Fatalf("OnFailure ran %d times, want 1", n)
	}
}

func TestDurableSleepOnFakeClock(t *testing.T) {
	var ran atomic.Bool
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "sleep", Sleep: 72 * time.Hour},
		{Name: "after", After: []string{"sleep"}, Run: func(context.Context, []byte) ([]byte, error) {
			ran.Store(true)
			return nil, nil
		}},
	}}
	c, fc := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, ref, workflow.StatusWaiting)
	blockUntil(t, fc, 1)
	fc.Advance(72*time.Hour - time.Second)
	if ran.Load() {
		t.Fatal("ran before the sleep ended")
	}
	fc.Advance(time.Second)
	waitStatus(t, c, ref, workflow.StatusSucceeded)
}

func TestRetriesWithBackoff(t *testing.T) {
	var attempts atomic.Int32
	wf := one("flaky", func(context.Context, []byte) ([]byte, error) {
		if attempts.Add(1) < 3 {
			return nil, errors.New("transient")
		}
		return nil, nil
	})
	wf.Retry = workflow.RetryPolicy{Max: 5, Backoff: workflow.Backoff{Initial: time.Minute, Factor: 2}}
	c, fc := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "flaky"})

	blockUntil(t, fc, 1) // backoff before retry 1: 1m
	waitStatus(t, c, ref, workflow.StatusRetrying)
	fc.Advance(time.Minute)
	blockUntil(t, fc, 1) // backoff before retry 2: 2m
	if attempts.Load() != 2 {
		t.Fatalf("attempts %d, want 2", attempts.Load())
	}
	fc.Advance(time.Minute)
	if attempts.Load() != 2 {
		t.Fatal("retried before the 2m backoff ended")
	}
	fc.Advance(time.Minute)
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	if attempts.Load() != 3 {
		t.Fatalf("attempts %d, want 3", attempts.Load())
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	var attempts, failures atomic.Int32
	var failIn atomic.Value
	wf := one("w", func(context.Context, []byte) ([]byte, error) {
		attempts.Add(1)
		return nil, workflow.Permanent(errors.New("bad input"))
	})
	wf.Retry = workflow.RetryPolicy{Max: 5}
	wf.OnFailure = func(_ context.Context, in []byte) ([]byte, error) {
		failures.Add(1)
		failIn.Store(string(in))
		return nil, nil
	}
	c, _ := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "w", Payload: []byte("p")})
	waitStatus(t, c, ref, workflow.StatusFailed)
	if attempts.Load() != 1 || failures.Load() != 1 || failIn.Load() != "p" {
		t.Fatalf("attempts %d, OnFailure %d with %v", attempts.Load(), failures.Load(), failIn.Load())
	}
}

func TestStepTimeoutOnFakeClock(t *testing.T) {
	started := make(chan string, 4)
	wf := one("slow", block(started, nil))
	wf.Timeout = time.Minute
	wf.Steps[0].Retry = &workflow.RetryPolicy{Max: 1}
	c, fc := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "slow"})
	<-started
	blockUntil(t, fc, 1)
	fc.Advance(time.Minute) // attempt 1 times out; retry at once (no backoff)
	<-started
	blockUntil(t, fc, 1)
	fc.Advance(time.Minute)
	waitStatus(t, c, ref, workflow.StatusFailed)
}

func TestCancel(t *testing.T) {
	started := make(chan string, 1)
	c, _ := newClient(t, []workflow.Workflow{one("w", block(started, nil))})
	ctx := context.Background()
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	<-started
	waitStatus(t, c, ref, workflow.StatusRunning)
	if err := c.Cancel(ctx, ref); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusCancelled)
	if err := c.Cancel(ctx, ref); err != nil {
		t.Fatalf("second Cancel: %v", err)
	}
}

func TestDelayedStart(t *testing.T) {
	c, fc := newClient(t, []workflow.Workflow{one("w", ok)})
	ctx := context.Background()
	ref := enqueue(t, c, workflow.Job{Workflow: "w", RunAt: start.Add(time.Hour)})
	if ref.ScheduledID.IsZero() || !ref.RunID.IsZero() {
		t.Fatalf("delayed ref %+v, want only ScheduledID", ref)
	}
	waitStatus(t, c, ref, workflow.StatusScheduled)
	blockUntil(t, fc, 1)
	fc.Advance(time.Hour)
	waitStatus(t, c, ref, workflow.StatusSucceeded)

	ref2 := enqueue(t, c, workflow.Job{Workflow: "w", RunAt: fc.Now().Add(time.Hour)})
	if err := c.Cancel(ctx, ref2); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref2, workflow.StatusCancelled)
	fc.Advance(time.Hour)
	waitStatus(t, c, ref2, workflow.StatusCancelled)
}

func TestIdempotency(t *testing.T) {
	started := make(chan string, 4)
	release := make(chan struct{})
	req := one("req", ok)
	req.Idempotency = workflow.IdempotencyRequired
	off := one("off", ok)
	off.Idempotency = workflow.IdempotencyOff
	c, _ := newClient(t, []workflow.Workflow{one("w", block(started, release)), req, off})
	ctx := context.Background()

	first := enqueue(t, c, workflow.Job{Workflow: "w", IdempotencyKey: "k"})
	<-started
	dup := enqueue(t, c, workflow.Job{Workflow: "w", IdempotencyKey: "k"})
	if dup != first {
		t.Fatalf("duplicate got %+v, want holder %+v", dup, first)
	}
	close(release)
	waitStatus(t, c, first, workflow.StatusSucceeded)
	again := enqueue(t, c, workflow.Job{Workflow: "w", IdempotencyKey: "k"})
	if again == first {
		t.Fatal("key still held after the run finished")
	}
	<-started

	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "req"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("required key missing: %v", err)
	}
	a := enqueue(t, c, workflow.Job{Workflow: "off", IdempotencyKey: "k"})
	b := enqueue(t, c, workflow.Job{Workflow: "off", IdempotencyKey: "k"})
	if a == b {
		t.Fatal("IdempotencyOff deduplicated")
	}
}

func TestConcurrencyStrategies(t *testing.T) {
	byPayload := workflow.KeyFunc(func(j workflow.Job) string { return string(j.Payload) })
	mk := func(name string, s workflow.ConcurrencyStrategy, started chan string, release chan struct{}) workflow.Workflow {
		wf := one(name, block(started, release))
		wf.Concurrency = &workflow.Concurrency{Key: byPayload, MaxRuns: 1, Strategy: s}
		return wf
	}
	for _, tc := range []struct {
		strategy        workflow.ConcurrencyStrategy
		first, second   workflow.Status // after the second enqueue
		secondEventuals workflow.Status // after the first is released
	}{
		{workflow.ConcurrencyQueueNewest, workflow.StatusRunning, workflow.StatusQueued, workflow.StatusSucceeded},
		{workflow.ConcurrencyRoundRobin, workflow.StatusRunning, workflow.StatusQueued, workflow.StatusSucceeded},
		{workflow.ConcurrencyDropNewest, workflow.StatusRunning, workflow.StatusCancelled, workflow.StatusCancelled},
		{workflow.ConcurrencyCancelNewest, workflow.StatusRunning, workflow.StatusCancelled, workflow.StatusCancelled},
		{workflow.ConcurrencyCancelInProgress, workflow.StatusCancelled, workflow.StatusRunning, workflow.StatusSucceeded},
	} {
		t.Run(string(tc.strategy), func(t *testing.T) {
			started := make(chan string, 8)
			release := make(chan struct{})
			c, _ := newClient(t, []workflow.Workflow{mk("w", tc.strategy, started, release)})
			first := enqueue(t, c, workflow.Job{Workflow: "w", Payload: []byte("a")})
			<-started
			other := enqueue(t, c, workflow.Job{Workflow: "w", Payload: []byte("b")}) // other key: own bucket
			<-started
			second := enqueue(t, c, workflow.Job{Workflow: "w", Payload: []byte("a")})
			waitStatus(t, c, first, tc.first)
			waitStatus(t, c, second, tc.second)
			waitStatus(t, c, other, workflow.StatusRunning)
			close(release)
			waitStatus(t, c, second, tc.secondEventuals)
			waitStatus(t, c, other, workflow.StatusSucceeded)
		})
	}
}

func TestRateLimit(t *testing.T) {
	wf := one("w", ok)
	wf.RateLimits = []workflow.RateLimit{{Limit: 2, Per: time.Minute}}
	c, fc := newClient(t, []workflow.Workflow{wf})
	a := enqueue(t, c, workflow.Job{Workflow: "w"})
	b := enqueue(t, c, workflow.Job{Workflow: "w"})
	third := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, a, workflow.StatusSucceeded)
	waitStatus(t, c, b, workflow.StatusSucceeded)
	waitStatus(t, c, third, workflow.StatusQueued)
	blockUntil(t, fc, 1)
	fc.Advance(time.Minute - time.Second)
	waitStatus(t, c, third, workflow.StatusQueued)
	fc.Advance(time.Second)
	waitStatus(t, c, third, workflow.StatusSucceeded)
}

func TestPriorityOrdersQueuedRuns(t *testing.T) {
	started := make(chan string, 8)
	release := make(chan struct{}, 8)
	c, _ := newClient(t, []workflow.Workflow{one("w", block(started, release))}, memory.WithWorkers(1))
	enqueue(t, c, workflow.Job{Workflow: "w", Payload: []byte("busy")})
	<-started
	low := enqueue(t, c, workflow.Job{Workflow: "w", Payload: []byte("low"), Priority: 1})
	enqueue(t, c, workflow.Job{Workflow: "w", Payload: []byte("high"), Priority: 9})
	waitStatus(t, c, low, workflow.StatusQueued)
	release <- struct{}{}
	if got := <-started; got != "high" {
		t.Fatalf("first queued run to start was %q, want high", got)
	}
	release <- struct{}{}
	if got := <-started; got != "low" {
		t.Fatalf("second was %q, want low", got)
	}
	release <- struct{}{}
}

func TestCron(t *testing.T) {
	var fired atomic.Int32
	declared := one("declared", func(context.Context, []byte) ([]byte, error) {
		fired.Add(1)
		return nil, nil
	})
	declared.Cron = []string{"0 * * * *"}
	var zoned atomic.Int32
	var zonedIn atomic.Value
	c, fc := newClient(t, []workflow.Workflow{declared, one("zoned", func(_ context.Context, in []byte) ([]byte, error) {
		zoned.Add(1)
		zonedIn.Store(string(in))
		return nil, nil
	})})
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Skip("no tzdata:", err)
	}
	// 10:00 UTC is 15:30 in Kolkata; "0 16 * * *" there is 10:30 UTC.
	id, err := c.Schedule(context.Background(), workflow.Schedule{Workflow: "zoned", Cron: "0 16 * * *", Location: kolkata, Input: []byte("tick")})
	if err != nil {
		t.Fatal(err)
	}
	blockUntil(t, fc, 2)
	fc.Advance(30 * time.Minute)
	waitCount(t, &zoned, 1)
	if fired.Load() != 0 {
		t.Fatal("hourly cron fired at 10:30")
	}
	blockUntil(t, fc, 2)
	fc.Advance(30 * time.Minute)
	waitCount(t, &fired, 1)
	if zonedIn.Load() != "tick" {
		t.Fatalf("schedule input %v", zonedIn.Load())
	}

	if err := c.Unschedule(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := c.Unschedule(context.Background(), id); err != nil {
		t.Fatalf("second Unschedule: %v", err)
	}
	if err := c.Unschedule(context.Background(), ""); err != nil {
		t.Fatalf("empty Unschedule: %v", err)
	}
	blockUntil(t, fc, 1)
	fc.Advance(24 * time.Hour) // the hourly cron fires once (no catch-up); the zoned one is gone
	waitCount(t, &fired, 2)
	time.Sleep(10 * time.Millisecond)
	if zoned.Load() != 1 || fired.Load() != 2 {
		t.Fatalf("after a day: zoned %d (want 1), hourly %d (want 2, no catch-up)", zoned.Load(), fired.Load())
	}
}

func waitCount(t *testing.T, n *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(pollFor)
	for n.Load() < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := n.Load(); got != want {
		t.Fatalf("count %d, want %d", got, want)
	}
}

func TestShutdownCancelsRunsAndStopsGoroutines(t *testing.T) {
	started := make(chan string, 1)
	fc := clock.NewFake(start)
	c := memory.New(memory.WithClock(fc))
	if err := c.Register(one("w", block(started, nil))); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	running := enqueue(t, c, workflow.Job{Workflow: "w"})
	<-started
	delayed := enqueue(t, c, workflow.Job{Workflow: "w", RunAt: start.Add(time.Hour)})
	cancel()
	c.Stop() // returns only once every goroutine has exited
	for _, ref := range []workflow.RunRef{running, delayed} {
		if st, _ := c.Status(context.Background(), ref); st != workflow.StatusCancelled {
			t.Fatalf("run %+v is %q after shutdown", ref, st)
		}
	}
	if _, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("Enqueue after shutdown: %v", err)
	}
	if fc.Pending() != 0 {
		t.Fatalf("%d timers left armed after shutdown", fc.Pending())
	}
}

func TestMiddlewareWrapsSteps(t *testing.T) {
	var calls atomic.Int32
	mw := func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			calls.Add(1)
			return next(ctx, in)
		}
	}
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: ok}, {Name: "b", After: []string{"a"}, Run: ok}}}
	c, _ := newClient(t, []workflow.Workflow{wf}, memory.WithMiddleware(mw))
	waitStatus(t, c, enqueue(t, c, workflow.Job{Workflow: "w"}), workflow.StatusSucceeded)
	if calls.Load() != 2 {
		t.Fatalf("middleware ran %d times, want 2", calls.Load())
	}
}

func TestPanicBecomesError(t *testing.T) {
	c, _ := newClient(t, []workflow.Workflow{one("w", func(context.Context, []byte) ([]byte, error) { panic("boom") })})
	waitStatus(t, c, enqueue(t, c, workflow.Job{Workflow: "w"}), workflow.StatusFailed)
}

func TestStartRefusesExpressionKeys(t *testing.T) {
	expr := workflow.KeyExpr("input.id")
	conc := one("w", ok)
	conc.Concurrency = &workflow.Concurrency{Key: expr, MaxRuns: 1}
	rate := one("w", ok)
	rate.RateLimits = []workflow.RateLimit{{Key: expr, Limit: 1, Per: time.Minute}}
	wait := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Wait: &workflow.WaitSpec{Event: "e", KeyFrom: expr}}}}
	for name, wf := range map[string]workflow.Workflow{"concurrency": conc, "rate": rate, "wait": wait} {
		t.Run(name, func(t *testing.T) {
			c := memory.New(memory.WithClock(clock.NewFake(start)))
			defer c.Stop()
			if err := c.Register(wf); err != nil {
				t.Fatalf("Register: %v", err)
			}
			if err := c.Start(context.Background()); !errors.Is(err, workflow.ErrUnsupported) {
				t.Fatalf("Start with an expression key: %v", err)
			}
		})
	}
}

func TestWaitKeyFromJob(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "wait", Wait: &workflow.WaitSpec{
		Event:   "paid",
		KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["order"] }),
	}}}}
	c, _ := newClient(t, []workflow.Workflow{wf})
	a := enqueue(t, c, workflow.Job{Workflow: "w", Labels: map[string]string{"order": "a"}})
	b := enqueue(t, c, workflow.Job{Workflow: "w", Labels: map[string]string{"order": "b"}})
	waitStatus(t, c, a, workflow.StatusWaiting)
	waitStatus(t, c, b, workflow.StatusWaiting)
	if err := c.Signal(context.Background(), workflow.Event{Name: "paid", Key: "b"}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, b, workflow.StatusSucceeded)
	waitStatus(t, c, a, workflow.StatusWaiting)
}

func TestStepInfoOnContext(t *testing.T) {
	infos := make(chan workflow.StepInfo, 4)
	wf := one("w", func(ctx context.Context, _ []byte) ([]byte, error) {
		info, ok := workflow.StepInfoFrom(ctx)
		if !ok {
			return nil, workflow.Permanent(errors.New("no StepInfo"))
		}
		infos <- info
		if info.Attempt == 1 {
			return nil, errors.New("retry me")
		}
		return nil, nil
	})
	wf.Retry = workflow.RetryPolicy{Max: 1}
	seen := make(chan bool, 4)
	mw := func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			_, ok := workflow.StepInfoFrom(ctx)
			seen <- ok
			return next(ctx, in)
		}
	}
	c, _ := newClient(t, []workflow.Workflow{wf}, memory.WithMiddleware(mw))
	ref := enqueue(t, c, workflow.Job{Workflow: "w", Labels: map[string]string{"tenant": "t1"}})
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	for want := 1; want <= 2; want++ {
		info := <-infos
		if info.Workflow != "w" || info.Step != "s" || info.RunID != ref.RunID || info.Attempt != want || info.Labels["tenant"] != "t1" {
			t.Fatalf("attempt %d: StepInfo %+v", want, info)
		}
		if !<-seen {
			t.Fatal("middleware ran without StepInfo on ctx")
		}
	}
}

// TestSignalBeforeWaitIsKept pins the look-back contract: a signal sent
// after enqueue but before the run reaches its wait step is not lost, a run
// enqueued after the signal does not take it, and one run takes a signal
// once.
func TestSignalBeforeWaitIsKept(t *testing.T) {
	outs := make(chan string, 4)
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "sleep", Sleep: time.Hour},
		{Name: "wait1", After: []string{"sleep"}, Wait: &workflow.WaitSpec{Event: "paid", Key: "o1"}},
		{Name: "wait2", After: []string{"wait1"}, Wait: &workflow.WaitSpec{Event: "paid", Key: "o1"}},
		{Name: "done", After: []string{"wait2"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			outs <- string(in)
			return nil, nil
		}},
	}}
	c, fc := newClient(t, []workflow.Workflow{wf})
	ctx := context.Background()
	early := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, early, workflow.StatusWaiting)
	if err := c.Signal(ctx, workflow.Event{Name: "paid", Key: "o1", Payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	late := enqueue(t, c, workflow.Job{Workflow: "w"}) // after the signal: must not take it
	blockUntil(t, fc, 2)
	fc.Advance(time.Hour)
	// early took "first" in wait1 and now waits in wait2: the same signal
	// is not taken twice. late waits in wait1.
	waitStatus(t, c, early, workflow.StatusWaiting)
	waitStatus(t, c, late, workflow.StatusWaiting)
	time.Sleep(20 * time.Millisecond)
	if st, _ := c.Status(ctx, early); st != workflow.StatusWaiting {
		t.Fatalf("early run is %q; it took one signal twice", st)
	}
	if err := c.Signal(ctx, workflow.Event{Name: "paid", Key: "o1", Payload: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, early, workflow.StatusSucceeded)
	if got := <-outs; got != "second" {
		t.Fatalf("early run's last wait got %q", got)
	}
	// late took "second" in wait1 (sent after its enqueue) and waits in wait2.
	waitStatus(t, c, late, workflow.StatusWaiting)
	if err := c.Signal(ctx, workflow.Event{Name: "paid", Key: "o1", Payload: []byte("third")}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, late, workflow.StatusSucceeded)
}
