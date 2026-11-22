package clock

import (
	"sync"
	"time"
)

// DefaultWakeCheck is the check interval WakeChecked uses for an interval
// of zero or less.
//
//ds:def id=clock-defaultwakecheck-ytntg994 owner=@khanakia stability=stable
const DefaultWakeCheck = 30 * time.Second

// WakeChecked returns a Clock whose timers fire by the wall clock, not by
// elapsed running time (TM-15). A plain timer measures a monotonic clock,
// which on some platforms stops while the machine sleeps: a laptop that
// sleeps for eight hours fires a timer due one hour into the sleep seven
// hours late, so a cron tick or a delayed job runs that much late. A
// timer from WakeChecked never waits longer than every at a time; each
// time it wakes it compares the wall clock with its deadline (the time it
// was created plus d, as a wall-clock instant) and fires once the
// deadline has passed, so after a suspend it fires at most every late.
//
// It costs one wake per every per pending timer, so pick every to match
// how late a fire may be after a resume (DefaultWakeCheck for a desktop
// app). It wraps any Clock; Now is the inner clock's.
//
//ds:def id=clock-wakechecked-pdvq4r34 owner=@khanakia stability=stable
func WakeChecked(inner Clock, every time.Duration) Clock {
	if every <= 0 {
		every = DefaultWakeCheck
	}
	return wakeClock{inner: inner, every: every}
}

type wakeClock struct {
	inner Clock
	every time.Duration
}

func (w wakeClock) Now() time.Time { return w.inner.Now() }

//ds:def id=clock-wakeclock-newtimer-dbkyykbb owner=@khanakia stability=stable
func (w wakeClock) NewTimer(d time.Duration) Timer {
	// Round(0) drops the monotonic reading, so Sub compares wall clocks:
	// the point of this clock is that wall time includes a suspend.
	deadline := w.inner.Now().Round(0).Add(d)
	t := &wakeTimer{c: make(chan time.Time, 1), stop: make(chan struct{})}
	go t.run(w, deadline)
	return t
}

// wakeTimer is a Timer that re-arms an inner timer in steps of at most
// every until the wall-clock deadline passes.
type wakeTimer struct {
	c    chan time.Time
	stop chan struct{}

	mu   sync.Mutex
	done bool
}

func (t *wakeTimer) C() <-chan time.Time { return t.c }

func (t *wakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return false
	}
	t.done = true
	close(t.stop)
	return true
}

//ds:def id=clock-waketimer-run-j9hnqx62 owner=@khanakia stability=stable
func (t *wakeTimer) run(w wakeClock, deadline time.Time) {
	for {
		left := deadline.Sub(w.inner.Now().Round(0))
		if left <= 0 {
			t.mu.Lock()
			if !t.done {
				t.done = true
				t.c <- w.inner.Now()
			}
			t.mu.Unlock()
			return
		}
		inner := w.inner.NewTimer(min(left, w.every))
		select {
		case <-inner.C():
		case <-t.stop:
			inner.Stop()
			return
		}
	}
}
