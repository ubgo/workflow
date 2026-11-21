// Command sleep-wake shows why a desktop app wraps its clock in
// clock.WakeChecked. A reminder is due at 23:00 and the laptop sleeps from
// 22:30 to 07:00. A plain timer measures running time, which stops while the
// machine sleeps, so the reminder runs half an hour after the resume; a
// wake-checked timer compares the wall clock with its deadline and runs it
// within one check of the resume.
//
// A simulated laptop clock stands in for the hardware: its wall clock jumps
// when the laptop sleeps, while its timers only count running time.
//
// It is the program behind docs/operations/sleep-and-wake.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

const (
	// check is how often a wake-checked timer compares the wall clock with
	// its deadline in this program; a desktop app would use
	// clock.DefaultWakeCheck (30 seconds).
	check = time.Minute
	// runTimeout bounds the wait for the reminder, generous for a slow CI
	// host.
	runTimeout = 10 * time.Second
	// clockFormat prints times as the guide shows them.
	clockFormat = "15:04"
	// plainRunFor is how long the laptop runs after the resume on the plain
	// clock: the 30 minutes of running time from the lid close (22:30) to
	// the due time (23:00) that its timer still has to count.
	plainRunFor = 30 * time.Minute
)

var (
	// sleepAt is when the laptop lid closes, and the reminder is enqueued.
	sleepAt = time.Date(2026, 11, 20, 22, 30, 0, 0, time.UTC)
	// dueAt is when the reminder should run.
	dueAt = time.Date(2026, 11, 20, 23, 0, 0, 0, time.UTC)
	// wakeAt is when the laptop resumes.
	wakeAt = time.Date(2026, 11, 21, 7, 0, 0, 0, time.UTC)
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sleep-wake:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	fmt.Fprintf(w, "reminder due %s, laptop sleeps %s to %s\n",
		dueAt.Format(clockFormat), sleepAt.Format(clockFormat), wakeAt.Format(clockFormat))
	fmt.Fprintf(w, "default check: %s\n", clock.DefaultWakeCheck)

	// A plain clock: the laptop resumes, then runs for 30 minutes.
	plain := newLaptop(sleepAt)
	at, err := remind(ctx, memory.New(memory.WithClock(plain)), plain, plainRunFor)
	if err != nil {
		return fmt.Errorf("plain clock: %w", err)
	}
	fmt.Fprintf(w, "plain clock:        ran at %s, %s of running time after the resume\n",
		at.Format(clockFormat), at.Sub(wakeAt))

	// A wake-checked clock: the laptop resumes, then runs for one check.
	laptop := newLaptop(sleepAt)
	// snippet:wrap
	clk := clock.WakeChecked(laptop, check) // zero or less means clock.DefaultWakeCheck
	c := memory.New(memory.WithClock(clk))
	// snippet:end
	at, err = remind(ctx, c, laptop, check)
	if err != nil {
		return fmt.Errorf("wake-checked clock: %w", err)
	}
	fmt.Fprintf(w, "wake-checked clock: ran at %s, within one check (%s) of the resume\n",
		at.Format(clockFormat), check)
	return nil
}

// remind enqueues a reminder due at dueAt on c, sleeps the laptop until
// wakeAt, lets it run for after, and returns the wall-clock time the
// reminder's step ran.
func remind(ctx context.Context, c *memory.Client, laptop *laptopClock, after time.Duration) (time.Time, error) {
	ran := make(chan time.Time, 1)
	err := c.Register(workflow.Workflow{Name: "remind", Steps: []workflow.Step{{
		Name: "notify",
		Run: func(context.Context, []byte) ([]byte, error) {
			ran <- laptop.Now()
			return nil, nil
		},
	}}})
	if err != nil {
		return time.Time{}, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return time.Time{}, err
	}
	defer c.Stop()

	// snippet:enqueue
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "remind", RunAt: dueAt}) // due at 23:00
	// snippet:end
	if err != nil {
		return time.Time{}, err
	}
	// Wait until the delayed run has armed its timer, then close the lid.
	if err := laptop.timers.BlockUntil(ctx, 1); err != nil {
		return time.Time{}, err
	}
	laptop.sleep(wakeAt.Sub(sleepAt))
	laptop.timers.Advance(after) // running time after the resume
	select {
	case at := <-ran:
		return at, nil
	case <-time.After(runTimeout):
		return time.Time{}, errors.New("the reminder did not run")
	}
}

// laptopClock models a machine that sleeps: Now is the wall clock, which
// keeps moving while the laptop sleeps, and NewTimer measures running time
// only, as a monotonic timer does on platforms whose monotonic clock stops
// during a suspend.
type laptopClock struct {
	timers *clock.Fake // running time: moves only while the laptop is awake

	mu    sync.Mutex
	slept time.Duration // wall-clock time spent asleep
}

// newLaptop returns a laptop clock whose wall clock reads start.
func newLaptop(start time.Time) *laptopClock {
	return &laptopClock{timers: clock.NewFake(start)}
}

// Now implements clock.Clock: it returns the wall clock, running time
// plus the time spent asleep.
func (l *laptopClock) Now() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.timers.Now().Add(l.slept)
}

// NewTimer implements clock.Clock: it returns a timer that counts running
// time only.
func (l *laptopClock) NewTimer(d time.Duration) clock.Timer { return l.timers.NewTimer(d) }

// sleep moves the wall clock by d while no timer moves.
func (l *laptopClock) sleep(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.slept += d
}
