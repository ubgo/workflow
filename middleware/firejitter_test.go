package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func TestFireJitterDecides(t *testing.T) {
	tick := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)
	fired := workflow.FiredLabelsAt(nil, "s1", "digest", tick)
	triggered := workflow.TriggeredLabelsAt(nil, "s1", "digest", tick)
	fixed := middleware.WithJitterFunc(func(workflow.StepInfo, time.Duration) time.Duration { return 20 * time.Second })
	cases := []struct {
		name   string
		labels map[string]string
		now    time.Time
		window time.Duration
		snooze time.Duration // 0: the step runs
	}{
		{"on the tick: wait the whole jitter", fired, tick, time.Minute, 20 * time.Second},
		{"inside the jitter: wait the rest", fired, tick.Add(15 * time.Second), time.Minute, 5 * time.Second},
		{"at the deadline: run", fired, tick.Add(20 * time.Second), time.Minute, 0},
		{"a late run is not delayed further", fired, tick.Add(time.Hour), time.Minute, 0},
		{"a plain enqueue is never touched", nil, tick, time.Minute, 0},
		{"a manual trigger is never touched", triggered, tick, time.Minute, 0},
		{"a zero window is off", fired, tick, 0, 0},
	}
	for _, c := range cases {
		fc := clock.NewFake(c.now)
		ran := false
		h := middleware.FireJitter(c.window, fixed, middleware.WithFireJitterClock(fc))(func(context.Context, []byte) ([]byte, error) {
			ran = true
			return nil, nil
		})
		ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{RunID: "r1", Labels: c.labels})
		_, err := h(ctx, nil)
		d, snoozed := workflow.SnoozeDelay(err)
		switch {
		case c.snooze == 0 && (!ran || err != nil):
			t.Errorf("%s: ran %v err %v, want the step to run", c.name, ran, err)
		case c.snooze != 0 && (ran || !snoozed || d != c.snooze):
			t.Errorf("%s: ran %v snooze %v (%v), want a snooze of %v", c.name, ran, d, err, c.snooze)
		}
	}
	// A jitter function out of range is clamped into [0, window].
	fc := clock.NewFake(tick)
	wild := middleware.WithJitterFunc(func(workflow.StepInfo, time.Duration) time.Duration { return time.Hour })
	h := middleware.FireJitter(time.Minute, wild, middleware.WithFireJitterClock(fc))(func(context.Context, []byte) ([]byte, error) { return nil, nil })
	_, err := h(workflow.WithStepInfo(context.Background(), workflow.StepInfo{Labels: fired}), nil)
	if d, _ := workflow.SnoozeDelay(err); d != time.Minute {
		t.Errorf("an out-of-range jitter snoozed %v, want the window", d)
	}
}

// TestRunIDJitter: the default is stable per run, inside the window, and
// spread across it.
func TestRunIDJitter(t *testing.T) {
	window := time.Minute
	buckets := make([]int, 6)
	for i := range 6000 {
		info := workflow.StepInfo{RunID: workflow.RunID(fmt.Sprintf("run-%d", i))}
		d := middleware.RunIDJitter(info, window)
		if d < 0 || d >= window {
			t.Fatalf("jitter %v outside [0, %v)", d, window)
		}
		if again := middleware.RunIDJitter(info, window); again != d {
			t.Fatalf("jitter of %s moved from %v to %v", info.RunID, d, again)
		}
		buckets[d/(10*time.Second)]++
	}
	for i, n := range buckets {
		if n < 800 || n > 1200 {
			t.Errorf("bucket %d holds %d of 6000 runs", i, n)
		}
	}
	if middleware.RunIDJitter(workflow.StepInfo{RunID: "x"}, 0) != 0 {
		t.Error("a zero window gave a jitter")
	}
}

// TestFireJitterOnMemory: a cron-fired run's first step waits tick +
// jitter on the driver's clock, then its later step passes straight
// through; a run enqueued by hand starts at once.
func TestFireJitterOnMemory(t *testing.T) {
	start := time.Date(2026, 1, 5, 10, 0, 30, 0, time.UTC)
	fc := clock.NewFake(start)
	jitter := middleware.FireJitter(time.Minute, middleware.WithFireJitterClock(fc),
		middleware.WithJitterFunc(func(workflow.StepInfo, time.Duration) time.Duration { return 20 * time.Second }))
	c := memory.New(memory.WithClock(fc), memory.WithMiddleware(jitter))
	if err := middleware.CheckFireJitter(c); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	ran := map[string][]time.Time{}
	record := func(name string) workflow.Handler {
		return func(ctx context.Context, _ []byte) ([]byte, error) {
			mu.Lock()
			ran[name] = append(ran[name], fc.Now())
			mu.Unlock()
			return nil, nil
		}
	}
	got := func(name string) []time.Time {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Time(nil), ran[name]...)
	}
	if err := c.Register(workflow.Workflow{Name: "digest", Steps: []workflow.Step{
		{Name: "a", Run: record("a")},
		{Name: "b", After: []string{"a"}, Run: record("b")},
	}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "digest", Cron: "* * * * *", Location: time.UTC}); err != nil {
		t.Fatal(err)
	}
	if err := fc.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	tick := time.Date(2026, 1, 5, 10, 1, 0, 0, time.UTC)
	fc.Set(tick)
	// The schedule re-arms for 10:02 and the snoozed step arms its own timer.
	if err := fc.BlockUntil(ctx, 2); err != nil {
		t.Fatal(err)
	}
	fc.Advance(19 * time.Second)
	time.Sleep(20 * time.Millisecond)
	if n := len(got("a")); n != 0 {
		t.Fatalf("step a ran %d times before tick + jitter", n)
	}
	if err := fc.BlockUntil(ctx, 2); err != nil {
		t.Fatal(err)
	}
	fc.Advance(time.Second)
	for len(got("b")) == 0 {
		if ctx.Err() != nil {
			t.Fatalf("the run never finished: a %v b %v", got("a"), got("b"))
		}
		time.Sleep(time.Millisecond)
	}
	want := tick.Add(20 * time.Second)
	if a, b := got("a"), got("b"); len(a) != 1 || !a[0].Equal(want) || !b[0].Equal(want) {
		t.Fatalf("a ran at %v and b at %v, want both once at %v", a, b, want)
	}
	// A plain enqueue runs at once.
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "digest"})
	if err != nil {
		t.Fatal(err)
	}
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || ctx.Err() != nil {
			t.Fatalf("status %v %v", st, err)
		}
		if st.IsTerminal() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if a := got("a"); len(a) != 2 || !a[1].Equal(want) {
		t.Fatalf("a plain run was delayed: %v", a)
	}
}

type noSnooze struct{ workflow.Client }

func (n noSnooze) Capabilities() workflow.Caps {
	return n.Client.Capabilities().Without(workflow.CapSnooze)
}

func TestCheckFireJitter(t *testing.T) {
	err := middleware.CheckFireJitter(noSnooze{memory.New()})
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || ue.Missing != workflow.CapSnooze {
		t.Fatalf("CheckFireJitter without CapSnooze = %v", err)
	}
}
