package clock_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow/clock"
)

// suspendClock models a machine that sleeps: Now jumps when the test
// says so, while its timers measure running time only (scaled down a
// thousand times, so an hour is 3.6 s and a minute 60 ms).
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

// TestWakeCheckedFiresByWallClock: the wall clock jumps eight hours (a
// suspend) while the running-time timers did not move; the wrapped timer
// fires at its next check, a plain one would wait out its whole delay.
//
//ds:def id=clock-testwakecheckedfiresbywallclock-qpns5h9b owner=@khanakia stability=stable
func TestWakeCheckedFiresByWallClock(t *testing.T) {
	sc := &suspendClock{base: time.Date(2026, 11, 20, 22, 0, 0, 0, time.UTC), start: time.Now()}
	wrapped := clock.WakeChecked(sc, time.Minute).NewTimer(time.Hour)
	plain := sc.NewTimer(time.Hour)
	sc.suspend(8 * time.Hour)
	select {
	case <-wrapped.C():
	case <-time.After(time.Second): // 60 ms of checks, with room for a slow CI
		t.Fatal("the wake-checked timer did not fire after the wall clock passed its deadline")
	}
	select {
	case <-plain.C():
		t.Fatal("control: a plain timer fired early, so the test proves nothing")
	default:
	}
	plain.Stop()
	if wrapped.Stop() {
		t.Fatal("Stop after firing reported true")
	}
}

func TestWakeCheckedStop(t *testing.T) {
	fake := clock.NewFake(time.Date(2026, 11, 20, 22, 0, 0, 0, time.UTC))
	tm := clock.WakeChecked(fake, time.Minute).NewTimer(time.Hour)
	blockUntil(t, fake, 1)
	if !tm.Stop() {
		t.Fatal("Stop of a pending timer reported false")
	}
	fake.Advance(2 * time.Hour)
	select {
	case <-tm.C():
		t.Fatal("a stopped timer fired")
	case <-time.After(20 * time.Millisecond):
	}
}

func blockUntil(t *testing.T, f *clock.Fake, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.BlockUntil(ctx, n); err != nil {
		t.Fatal(err)
	}
}
