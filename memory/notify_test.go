package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// collector records run ends and, from inside the observer, the run's
// Status, proving the observer may call back into the client.
type collector struct {
	c      *memory.Client
	mu     sync.Mutex
	events []workflow.RunEvent
	seen   []workflow.Status // Status as the observer saw it
	ctxErr []error
}

func (col *collector) observe(ctx context.Context, ev workflow.RunEvent) {
	st, err := col.c.Status(context.Background(), ev.Ref)
	if err != nil {
		st = workflow.Status("error: " + err.Error())
	}
	col.mu.Lock()
	defer col.mu.Unlock()
	col.events = append(col.events, ev)
	col.seen = append(col.seen, st)
	col.ctxErr = append(col.ctxErr, ctx.Err())
}

func (col *collector) waitN(t *testing.T, n int) []workflow.RunEvent {
	t.Helper()
	deadline := time.Now().Add(pollFor)
	for time.Now().Before(deadline) {
		col.mu.Lock()
		got := len(col.events)
		col.mu.Unlock()
		if got >= n {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // a duplicate would arrive by now
	col.mu.Lock()
	defer col.mu.Unlock()
	if len(col.events) != n {
		t.Fatalf("got %d run ends, want %d: %+v", len(col.events), n, col.events)
	}
	for i, ev := range col.events {
		if col.seen[i] != ev.Status {
			t.Fatalf("observer saw Status %q for an event with %q", col.seen[i], ev.Status)
		}
	}
	return append([]workflow.RunEvent(nil), col.events...)
}

func startWithCollector(t *testing.T, wfs []workflow.Workflow) (*memory.Client, *clock.Fake, *collector) {
	t.Helper()
	fc := clock.NewFake(start)
	c := memory.New(memory.WithClock(fc))
	col := &collector{c: c}
	if err := c.OnRunFinished(col.observe); err != nil {
		t.Fatal(err)
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return c, fc, col
}

func TestRunFinishedEveryTerminalRun(t *testing.T) {
	errApp := errors.New("app failure")
	started := make(chan string, 4)
	busy := one("busy", block(started, nil))
	busy.Concurrency = &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyDropNewest}
	c, fc, col := startWithCollector(t, []workflow.Workflow{
		one("ok", ok),
		one("bad", func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errApp) }),
		busy,
	})
	if !c.Capabilities().Has(workflow.CapRunFinished) {
		t.Fatal("memory must declare CapRunFinished")
	}
	ctx := context.Background()
	labels := map[string]string{"tenant": "t1"}

	succeeded := enqueue(t, c, workflow.Job{Workflow: "ok", Labels: labels})
	waitStatus(t, c, succeeded, workflow.StatusSucceeded)
	failed := enqueue(t, c, workflow.Job{Workflow: "bad"})
	waitStatus(t, c, failed, workflow.StatusFailed)
	running := enqueue(t, c, workflow.Job{Workflow: "busy"})
	<-started
	dropped := enqueue(t, c, workflow.Job{Workflow: "busy"})
	if err := c.Cancel(ctx, running); err != nil {
		t.Fatal(err)
	}
	delayed := enqueue(t, c, workflow.Job{Workflow: "ok", RunAt: start.Add(time.Hour)})
	fc.Advance(time.Minute)
	if err := c.Cancel(ctx, delayed); err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(ctx, delayed); err != nil { // idempotent: no second event
		t.Fatal(err)
	}

	evs := col.waitN(t, 5)
	byRef := map[workflow.RunRef]workflow.RunEvent{}
	for _, ev := range evs {
		byRef[ev.Ref] = ev
	}
	check := func(name string, ref workflow.RunRef, st workflow.Status, errIs error) workflow.RunEvent {
		t.Helper()
		ev, ok := byRef[ref]
		if !ok {
			t.Fatalf("%s: no event for %+v", name, ref)
		}
		if ev.Status != st {
			t.Fatalf("%s: status %q, want %q", name, ev.Status, st)
		}
		if errIs == nil && ev.Err != nil || errIs != nil && !errors.Is(ev.Err, errIs) {
			t.Fatalf("%s: Err %v, want %v", name, ev.Err, errIs)
		}
		return ev
	}
	ev := check("succeeded", succeeded, workflow.StatusSucceeded, nil)
	if ev.Workflow != "ok" || ev.Labels["tenant"] != "t1" || !ev.FinishedAt.Equal(start) {
		t.Fatalf("succeeded event %+v", ev)
	}
	check("failed", failed, workflow.StatusFailed, errApp)
	check("cancelled", running, workflow.StatusCancelled, workflow.ErrCancelled)
	check("dropped", dropped, workflow.StatusCancelled, workflow.ErrCancelled)
	ev = check("delayed", delayed, workflow.StatusCancelled, workflow.ErrCancelled)
	if !ev.Ref.RunID.IsZero() || !ev.FinishedAt.Equal(start.Add(time.Minute)) {
		t.Fatalf("cancelled delayed run event %+v", ev)
	}
}

func TestRunFinishedObserversOrderPanicAndLateRegister(t *testing.T) {
	fc := clock.NewFake(start)
	c := memory.New(memory.WithClock(fc))
	var mu sync.Mutex
	var order []string
	note := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}
	if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) { note("first"); panic("observer bug") }); err != nil {
		t.Fatal(err)
	}
	if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) { note("second") }); err != nil {
		t.Fatal(err)
	}
	if err := c.Register(one("w", ok)); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) {}); !errors.Is(err, workflow.ErrAlreadyStarted) {
		t.Fatalf("OnRunFinished after Start: %v", err)
	}
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	deadline := time.Now().Add(pollFor)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("observer calls %v, want [first second]", order)
	}
	if st, _ := c.Status(context.Background(), ref); st != workflow.StatusSucceeded {
		t.Fatalf("a panicking observer changed the run to %q", st)
	}
}

func TestRunFinishedDeliversShutdownCancellations(t *testing.T) {
	started := make(chan string, 1)
	fc := clock.NewFake(start)
	c := memory.New(memory.WithClock(fc))
	col := &collector{c: c}
	if err := c.OnRunFinished(col.observe); err != nil {
		t.Fatal(err)
	}
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
	c.Stop() // waits for the notifier to drain
	evs := col.waitN(t, 2)
	for _, ev := range evs {
		if ev.Ref != running && ev.Ref != delayed {
			t.Fatalf("unexpected event %+v", ev)
		}
		if ev.Status != workflow.StatusCancelled || !errors.Is(ev.Err, workflow.ErrCancelled) {
			t.Fatalf("shutdown event %+v", ev)
		}
	}
	col.mu.Lock()
	defer col.mu.Unlock()
	for _, err := range col.ctxErr {
		if err == nil {
			t.Fatal("observer ctx was live after shutdown; it must be the cancelled Start ctx")
		}
	}
}
