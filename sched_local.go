package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

// ErrLocalCronStarted is returned by LocalCron.Add after Start and by a
// second Start: entries are fixed once the cron runs.
var ErrLocalCronStarted = errors.New("workflow: local cron already started")

// LocalCronFunc is the code a LocalCron entry runs. at is the tick it
// runs for (the logical time), not the wall time it started.
type LocalCronFunc func(ctx context.Context, at time.Time) error

// LocalCron runs functions on a cron schedule inside this process, on
// every node that starts one: cache warmers, local temp cleanup, a
// per-node metrics flush. It is the opposite of Workflow.Cron and
// Schedule on a shared driver, which fire once across the fleet and may
// run the job on any worker; a LocalCron tick runs here, in-process, with
// no queue, no retries and no persistence.
//
// Each entry runs on its own goroutine, one tick at a time: ticks that
// fall due while the previous run is still going are skipped, and ticks
// missed while the process was down are never caught up. An error or a
// panic goes to the error handler (WithLocalCronErrors) and the entry
// keeps its schedule.
//
// Time comes from the injected clock (WithLocalCronClock, clock.Real by
// default), so a test drives it with clock.Fake.
type LocalCron struct {
	clk     clock.Clock
	parser  cron.Parser
	onError func(name string, at time.Time, err error)

	mu      sync.Mutex
	entries []localEntry
	names   map[string]bool
	started bool
	wg      sync.WaitGroup
	done    chan struct{}
}

type localEntry struct {
	name  string
	sched cron.Schedule
	loc   *time.Location
	fn    LocalCronFunc
}

// LocalCronOption configures a LocalCron.
type LocalCronOption func(*LocalCron)

// WithLocalCronClock sets the clock; nil keeps clock.Real.
func WithLocalCronClock(c clock.Clock) LocalCronOption {
	return func(l *LocalCron) {
		if c != nil {
			l.clk = c
		}
	}
}

// WithLocalCronParser sets the cron dialect (cron.ParseSeconds for a
// seconds field); nil keeps cron.Parse.
func WithLocalCronParser(p cron.Parser) LocalCronOption {
	return func(l *LocalCron) {
		if p != nil {
			l.parser = p
		}
	}
}

// WithLocalCronErrors receives every error and recovered panic of an
// entry's run, with the entry's name and tick. Nil (the default) drops
// them; the handler runs on the entry's goroutine and must not block.
func WithLocalCronErrors(f func(name string, at time.Time, err error)) LocalCronOption {
	return func(l *LocalCron) { l.onError = f }
}

// NewLocalCron returns a LocalCron with no entries.
func NewLocalCron(opts ...LocalCronOption) *LocalCron {
	l := &LocalCron{clk: clock.Real{}, parser: cron.Parse, names: map[string]bool{}, done: make(chan struct{})}
	for _, o := range opts {
		o(l)
	}
	return l
}

// Add registers fn to run at expr in loc. loc is required (time.UTC for
// UTC) and time.Local is refused, for the same reason as on Schedule.
// Names are unique. Every validation error wraps ErrInvalid; Add after
// Start returns ErrLocalCronStarted.
func (l *LocalCron) Add(name, expr string, loc *time.Location, fn LocalCronFunc) error {
	switch {
	case name == "":
		return invalidf("local cron: empty name")
	case fn == nil:
		return invalidf("local cron %q: nil func", name)
	case loc == nil:
		return invalidf("local cron %q: nil location; use time.UTC for UTC", name)
	case loc == time.Local:
		return invalidf("local cron %q: time.Local differs between machines; name the zone", name)
	}
	sched, err := l.parser(expr)
	if err != nil {
		return invalidf("local cron %q: %v", name, err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return ErrLocalCronStarted
	}
	if l.names[name] {
		return invalidf("local cron %q: name already added", name)
	}
	l.names[name] = true
	l.entries = append(l.entries, localEntry{name: name, sched: sched, loc: loc, fn: fn})
	return nil
}

// Start runs every entry until ctx ends, without blocking. Done closes once
// every entry's goroutine, including a run in progress, has returned.
func (l *LocalCron) Start(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return ErrLocalCronStarted
	}
	l.started = true
	for _, e := range l.entries {
		l.wg.Add(1)
		go l.loop(ctx, e)
	}
	go func() {
		l.wg.Wait()
		close(l.done)
	}()
	return nil
}

// Done closes after Start's ctx ends and every entry has stopped.
func (l *LocalCron) Done() <-chan struct{} { return l.done }

// loop waits for each tick of e and runs it. The next tick is computed
// from the clock after a run returns, which is what skips the ticks that
// fell due during a long run.
func (l *LocalCron) loop(ctx context.Context, e localEntry) {
	defer l.wg.Done()
	for {
		now := l.clk.Now()
		next := e.sched.Next(now, e.loc)
		if next.IsZero() {
			return
		}
		timer := l.clk.NewTimer(next.Sub(now))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
		}
		if err := l.run(ctx, e, next); err != nil && l.onError != nil {
			l.onError(e.name, next, err)
		}
	}
}

// run calls the entry's func, turning a panic into an error.
func (l *LocalCron) run(ctx context.Context, e localEntry, at time.Time) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("local cron %q panicked: %v", e.name, r)
		}
	}()
	return e.fn(ctx, at)
}
