package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func TestRunAllRunsDelayedRetriedSnoozedAndSleepingWorkNow(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	hit := func(name string) int {
		mu.Lock()
		defer mu.Unlock()
		calls[name]++
		return calls[name]
	}
	retried := one("retried", func(context.Context, []byte) ([]byte, error) {
		if hit("retried") == 1 {
			return nil, errors.New("once")
		}
		return nil, nil
	})
	retried.Retry = workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: time.Hour}}
	snoozed := one("snoozed", func(context.Context, []byte) ([]byte, error) {
		if hit("snoozed") == 1 {
			return nil, workflow.Snooze(time.Hour)
		}
		return nil, nil
	})
	sleeper := workflow.Workflow{Name: "sleeper", Steps: []workflow.Step{
		{Name: "nap", Sleep: 24 * time.Hour}, {Name: "after", After: []string{"nap"}, Run: ok}}}
	waiter := workflow.Workflow{Name: "waiter", Steps: []workflow.Step{{Name: "w", Wait: &workflow.WaitSpec{Event: "never"}}}}
	failing := one("failing", func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errors.New("no")) })
	c, _ := newClient(t, []workflow.Workflow{one("delayed", ok), retried, snoozed, sleeper, waiter, failing})
	enqueue(t, c, workflow.Job{Workflow: "delayed", RunAt: start.Add(48 * time.Hour)})
	for _, wf := range []string{"retried", "snoozed", "sleeper", "waiter", "failing"} {
		enqueue(t, c, workflow.Job{Workflow: wf})
	}
	rep, err := c.RunAll(context.Background())
	if err != nil {
		t.Fatalf("RunAll: %v", err)
	}
	want := memory.RunAllReport{Succeeded: 4, Failed: 1, Waiting: 1}
	if rep != want {
		t.Fatalf("report = %+v, want %+v", rep, want)
	}
}

func TestRunAllReportsAStall(t *testing.T) {
	c, _ := newClient(t, []workflow.Workflow{one("held", ok)})
	if err := c.Pause(context.Background(), workflow.PauseTarget{Workflow: "held"}); err != nil {
		t.Fatal(err)
	}
	enqueue(t, c, workflow.Job{Workflow: "held"})
	if _, err := c.RunAll(context.Background()); !errors.Is(err, memory.ErrRunAllStalled) {
		t.Fatalf("RunAll with a paused run = %v, want ErrRunAllStalled", err)
	}
}

func TestWeightedQueuesFollowThePinnedDraw(t *testing.T) {
	release := make(chan struct{})
	started := make(chan string, 8)
	a, b := one("a", block(started, release)), one("b", block(started, release))
	a.Queue, b.Queue = "qa", "qb"
	gate := one("gate", block(started, release))
	gate.Queue = "qa"
	draws := []float64{0.9, 0.1} // weights qa 1, qb 3 (total 4): 0.9 -> qb, 0.1 -> qa
	var mu sync.Mutex
	rnd := func() float64 {
		mu.Lock()
		defer mu.Unlock()
		if len(draws) == 0 {
			return 0.1
		}
		d := draws[0]
		draws = draws[1:]
		return d
	}
	c, _ := newClient(t, []workflow.Workflow{a, b, gate}, memory.WithWorkers(1), memory.WithJitterSource(rnd),
		memory.WithQueues(workflow.QueueConfig{Name: "qa", Weight: 1}, workflow.QueueConfig{Name: "qb", Weight: 3}))
	enqueue(t, c, workflow.Job{Workflow: "gate", Payload: []byte("gate")})
	if got := <-started; got != "gate" {
		t.Fatalf("first = %s", got)
	}
	enqueue(t, c, workflow.Job{Workflow: "a", Payload: []byte("a")})
	enqueue(t, c, workflow.Job{Workflow: "b", Payload: []byte("b")})
	close(release)
	var order []string
	for range 2 {
		order = append(order, <-started)
	}
	if order[0] != "b" || order[1] != "a" {
		t.Fatalf("order = %v, want [b a] (the draw picked qb first)", order)
	}
}

func TestPayloadLimitAndHealth(t *testing.T) {
	c := memory.New(memory.WithPayloadLimit(3))
	if rep := c.Health(context.Background()); !rep.Live || rep.Ready {
		t.Errorf("before Start: %+v, want live and not ready", rep)
	}
	if err := c.Register(one("w", ok)); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	if rep := c.Health(context.Background()); !rep.Ready {
		t.Errorf("after Start: %+v, want ready", rep)
	}
	if _, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w", Payload: []byte("1234")}); !errors.Is(err, workflow.ErrPayloadTooLarge) {
		t.Errorf("over the limit: %v", err)
	}
	if c.PayloadLimit() != 3 {
		t.Errorf("PayloadLimit = %d", c.PayloadLimit())
	}
	c.Stop()
	if rep := c.Health(context.Background()); rep.Live || rep.Ready {
		t.Errorf("after Stop: %+v, want neither", rep)
	}
}

func TestRescheduleRetiresTheOldTimer(t *testing.T) {
	var mu sync.Mutex
	runs := 0
	c, fc := newClient(t, []workflow.Workflow{one("w", func(context.Context, []byte) ([]byte, error) {
		mu.Lock()
		runs++
		mu.Unlock()
		return nil, nil
	})})
	ref := enqueue(t, c, workflow.Job{Workflow: "w", RunAt: start.Add(time.Hour)})
	blockUntil(t, fc, 1)
	if _, err := c.Reschedule(context.Background(), ref, start.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	fc.Advance(2 * time.Hour) // past the old time, before the new one
	time.Sleep(20 * time.Millisecond)
	if st, _ := c.Status(context.Background(), ref); st != workflow.StatusScheduled {
		t.Fatalf("status after the old time = %s, want still scheduled", st)
	}
	fc.Advance(time.Hour)
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	mu.Lock()
	defer mu.Unlock()
	if runs != 1 {
		t.Fatalf("ran %d times, want 1", runs)
	}
}

func TestLifecycleReportsProgress(t *testing.T) {
	c := memory.New()
	if err := c.Register(one("w", func(ctx context.Context, in []byte) ([]byte, error) {
		return in, workflow.ReportProgress(ctx, workflow.Progress{Percent: 30, Message: "m"})
	})); err != nil {
		t.Fatal(err)
	}
	got := make(chan workflow.LifecycleEvent, 16)
	if err := c.OnLifecycle(func(_ context.Context, ev workflow.LifecycleEvent) { got <- ev }); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	enqueue(t, c, workflow.Job{Workflow: "w", Tenant: "acme"})
	timeout := time.After(pollFor)
	for {
		select {
		case ev := <-got:
			if ev.Kind != workflow.LifecycleProgress {
				continue
			}
			if ev.Progress == nil || ev.Progress.Percent != 30 || ev.Progress.Step != "s" || ev.Tenant != "acme" {
				t.Fatalf("progress event = %+v", ev)
			}
			return
		case <-timeout:
			t.Fatal("no progress event")
		}
	}
}

// TestRunErrorKeepsTheStepMessage pins DX-15 both ways: the error value
// reaching an observer names the run (errors.As), while RunInfo.Error is
// the step's own text, exactly as before the RunError existed.
func TestRunErrorKeepsTheStepMessage(t *testing.T) {
	c := memory.New()
	if err := c.Register(one("w", func(context.Context, []byte) ([]byte, error) {
		return nil, workflow.Permanent(errors.New("smtp 550"))
	})); err != nil {
		t.Fatal(err)
	}
	got := make(chan workflow.RunEvent, 1)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { got <- ev }); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, ref, workflow.StatusFailed)
	page, err := c.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil {
		t.Fatal(err)
	}
	if want := `memory: step "s" failed after 1 attempt(s): smtp 550`; page.Runs[0].Error != want {
		t.Errorf("RunInfo.Error = %q, want %q", page.Runs[0].Error, want)
	}
	ev := <-got
	re, ok := workflow.RunErrorOf(ev.Err)
	if !ok || re.RunID != ref.RunID || re.Step != "s" || re.Attempt != 1 {
		t.Fatalf("RunErrorOf(RunEvent.Err) = %+v, %v; want run %s step s attempt 1", re, ok, ref.RunID)
	}
	if ev.Err.Error() != page.Runs[0].Error {
		t.Errorf("RunEvent.Err text = %q, want RunInfo.Error's", ev.Err)
	}
}

// TestRunAllRightAfterSignal pins that a wait a Signal satisfied no longer
// counts as waiting, even before the waiter goroutine wakes: RunAll must
// run the rest of the run rather than report it Waiting.
func TestRunAllRightAfterSignal(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "wait", Wait: &workflow.WaitSpec{Event: "go"}},
		{Name: "after", After: []string{"wait"}, Run: ok},
	}}
	for range 50 {
		c, _ := newClient(t, []workflow.Workflow{wf})
		ref := enqueue(t, c, workflow.Job{Workflow: "w"})
		waitStatus(t, c, ref, workflow.StatusWaiting)
		if err := c.Signal(context.Background(), workflow.Event{Name: "go"}); err != nil {
			t.Fatal(err)
		}
		rep, err := c.RunAll(context.Background())
		if err != nil || rep.Waiting != 0 || rep.Succeeded != 1 {
			t.Fatalf("RunAll right after Signal = %+v, %v; want the run finished", rep, err)
		}
		c.Stop()
	}
}
