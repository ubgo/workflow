package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/ubgo/workflow/clock"
)

// ReadyFunc reports whether something the app waits on before starting
// work is ready, such as a clock that has been synchronised. An error
// stops the wait and is returned to the caller.
//
//ds:def id=readyfunc-qcg9688f owner=@khanakia stability=stable
type ReadyFunc func(ctx context.Context) (bool, error)

// DefaultStartWhenPoll is how often StartWhen asks its ReadyFunc.
//
//ds:def id=defaultstartwhenpoll-h7z7x3ch owner=@khanakia stability=stable
const DefaultStartWhenPoll = time.Second

// StartWhenOptions tunes StartWhen.
//
//ds:def id=startwhenoptions-ryd4cehw owner=@khanakia stability=stable
type StartWhenOptions struct {
	// Poll is the pause between two asks; zero means DefaultStartWhenPoll.
	Poll time.Duration

	// Clock times the pauses; nil means clock.Real.
	Clock clock.Clock
}

// StartWhen holds a client's Start until ready reports true, then starts
// it (IO-02). An edge device that boots with its real-time clock at 1970,
// or anything that must not fire schedules before a dependency is up,
// calls it in place of Start: until the device's clock is synchronised a
// scheduler would compute every tick from a wrong now, firing a year of
// misfires or none at all, so nothing runs until the gate opens. Enqueue
// and Signal still return ErrNotStarted meanwhile, exactly as before any
// Start.
//
// It blocks until Start ran (its error is returned), ready failed, or ctx
// ended (ctx.Err()). ctx is also the ctx passed to Start, so it is the
// client's lifetime as with Start. ClockAfter is a ReadyFunc for the
// common clock case.
//
//ds:def id=startwhen-rgx6keu2 owner=@khanakia stability=stable
func StartWhen(ctx context.Context, c Client, ready ReadyFunc, opts StartWhenOptions) error {
	if c == nil || ready == nil {
		return invalidf("StartWhen needs a client and a ReadyFunc")
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	poll := opts.Poll
	if poll <= 0 {
		poll = DefaultStartWhenPoll
	}
	for {
		ok, err := ready(ctx)
		if err != nil {
			return fmt.Errorf("workflow: start when: %w", err)
		}
		if ok {
			return c.Start(ctx)
		}
		t := clk.NewTimer(poll)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C():
		}
	}
}

// ClockAfter returns a ReadyFunc that is ready once clk reads a time at or
// after floor. Pass a floor the device cannot see before a sync, such as
// the build time of the binary: a clock reset to its epoch reads far
// before it, and the first network or GPS sync moves it past. For a sync
// status the OS reports (timedatectl, an NTP client's state) write a
// ReadyFunc that asks it.
//
//ds:def id=clockafter-3nzw3qf4 owner=@khanakia stability=stable
func ClockAfter(clk clock.Clock, floor time.Time) ReadyFunc {
	if clk == nil {
		clk = clock.Real{}
	}
	return func(context.Context) (bool, error) { return !clk.Now().Before(floor), nil }
}
