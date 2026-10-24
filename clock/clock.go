// Package clock is the time source every scheduling decision reads. Logic
// in this module never calls time.Now or time.NewTimer: it takes a Clock,
// so production passes Real and tests pass a Fake that moves only when told
// to, which makes "sleep three days" run in microseconds and makes every
// run repeatable.
package clock

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Clock is a source of the current time and of timers.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTimer returns a timer that delivers on C once d has passed. A d
	// of zero or less fires at once.
	NewTimer(d time.Duration) Timer
}

// Timer is a one-shot timer.
type Timer interface {
	// C delivers the fire time once. Its buffer holds one value, so a
	// timer never blocks the clock that fires it.
	C() <-chan time.Time
	// Stop prevents the timer from firing. It reports whether it did so,
	// false when the timer had already fired or been stopped.
	Stop() bool
}

// Real is the wall clock. It is the one place in this module that reads
// time.Now.
type Real struct{}

// Now returns time.Now.
func (Real) Now() time.Time { return time.Now() }

// NewTimer wraps time.NewTimer.
func (Real) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop() bool          { return r.t.Stop() }

// Fake is a Clock that moves only on Advance or Set. Timers due at or
// before the new time fire in deadline order, each receiving its deadline.
// It is safe for concurrent use. Use NewFake; the zero value is not ready.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	armed  chan struct{} // closed and replaced whenever a timer is added
}

// NewFake returns a Fake set to start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start, armed: make(chan struct{})}
}

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer returns a timer due at Now()+d. A d of zero or less fires at
// once, as time.NewTimer does.
func (f *Fake) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{f: f, at: f.now.Add(d), c: make(chan time.Time, 1)}
	if d <= 0 {
		t.c <- f.now
		return t
	}
	f.timers = append(f.timers, t)
	close(f.armed)
	f.armed = make(chan struct{})
	return t
}

// Advance moves the clock forward by d and fires every timer now due.
// A negative d is ignored: the fake clock never goes back.
func (f *Fake) Advance(d time.Duration) {
	if d < 0 {
		return
	}
	f.mu.Lock()
	f.setLocked(f.now.Add(d))
	f.mu.Unlock()
}

// Set moves the clock to t, if t is later, and fires every timer now due.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	if t.After(f.now) {
		f.setLocked(t)
	}
	f.mu.Unlock()
}

func (f *Fake) setLocked(t time.Time) {
	f.now = t
	sort.SliceStable(f.timers, func(i, j int) bool { return f.timers[i].at.Before(f.timers[j].at) })
	kept := f.timers[:0]
	for _, tm := range f.timers {
		if tm.at.After(t) {
			kept = append(kept, tm)
			continue
		}
		tm.c <- tm.at
	}
	f.timers = kept
}

// Pending returns the number of timers that have not fired or been
// stopped.
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

// BlockUntil waits until at least n timers are pending, so a test can
// Advance only after the goroutine under test has armed its timer. It
// returns ctx.Err() if ctx ends first.
func (f *Fake) BlockUntil(ctx context.Context, n int) error {
	for {
		f.mu.Lock()
		if len(f.timers) >= n {
			f.mu.Unlock()
			return nil
		}
		armed := f.armed
		f.mu.Unlock()
		select {
		case <-armed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type fakeTimer struct {
	f  *Fake
	at time.Time
	c  chan time.Time
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }

func (t *fakeTimer) Stop() bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	for i, tm := range t.f.timers {
		if tm == t {
			t.f.timers = append(t.f.timers[:i], t.f.timers[i+1:]...)
			return true
		}
	}
	return false
}
