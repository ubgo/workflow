package memory_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// suspendClock: Now jumps on suspend, timers measure running time only,
// scaled down a thousand times.
type suspendClock struct {
	mu     sync.Mutex
	base   time.Time
	start  time.Time
	offset time.Duration
}

func (s *suspendClock) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base.Add(time.Since(s.start)*1000 + s.offset)
}

func (s *suspendClock) suspend(d time.Duration) { s.mu.Lock(); s.offset += d; s.mu.Unlock() }

func (s *suspendClock) NewTimer(d time.Duration) clock.Timer {
	return realTimer{time.NewTimer(d / 1000)}
}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop() bool          { return r.t.Stop() }

// TestWakeCheckedClockAfterSuspend (TM-15, EM-03): a run delayed an hour
// on a laptop that then sleeps two hours runs at the next wake check, not
// an hour after the resume.
func TestWakeCheckedClockAfterSuspend(t *testing.T) {
	sc := &suspendClock{base: time.Date(2026, 11, 20, 22, 0, 0, 0, time.UTC), start: time.Now()}
	c := memory.New(memory.WithClock(clock.WakeChecked(sc, time.Minute)))
	ran := make(chan struct{}, 1)
	if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		ran <- struct{}{}
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", RunAt: sc.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	sc.suspend(2 * time.Hour)
	select {
	case <-ran:
	case <-time.After(time.Second): // a plain timer would wait 3.6 s
		t.Fatal("the delayed run did not run after the wall clock passed its time")
	}
}
