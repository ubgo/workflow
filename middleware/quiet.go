package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// quietHorizon is how far QuietHours looks for an allowed minute. Windows
// repeat daily, so two days (plus a DST shift) always reach one when any
// exists.
const quietHorizon = 50 * time.Hour

// QuietHours is enqueue middleware that keeps jobs out of quiet hours,
// such as notifications at night in the recipient's zone: a job whose
// start (Job.RunAt, or now on clk for a job that starts at once; nil clk
// means clock.Real) falls where w does not allow is moved to the first
// minute w allows. A job whose start w allows passes unchanged.
//
// w says when jobs may start, read exactly as a schedule's windows
// (workflow.Windows.Allows): quiet hours are Unless windows
// (Windows{Unless: []Window{{From: "21:00", To: "08:00"}}}), sending
// hours are Between windows, and both may be combined. The wall clock is
// zoneOf(job)'s (nil zoneOf, or a nil zone, means UTC), so a user in
// Tokyo and one in Berlin each keep their own night; the move follows
// that zone across DST, landing on the first minute that exists and is
// allowed.
//
// A moved job is enqueued with RunAt set, which needs CapDelayedStart: a
// driver without it refuses the job (workflow.ErrUnsupported), never
// sends it at night; a job that is not moved needs nothing. A window
// that is not "HH:MM", or one whose From equals its To, or windows that
// allow no minute of the day, refuse the enqueue with workflow.ErrInvalid.
func QuietHours(w workflow.Windows, zoneOf func(workflow.Job) *time.Location, clk clock.Clock) workflow.EnqueueMiddleware {
	if clk == nil {
		clk = clock.Real{}
	}
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if err := checkWindows(w); err != nil {
				return workflow.RunRef{}, err
			}
			var loc *time.Location
			if zoneOf != nil {
				loc = zoneOf(job)
			}
			start := job.RunAt
			if start.IsZero() {
				start = clk.Now()
			}
			if w.Allows(start, loc) {
				return next(ctx, job)
			}
			at, ok := nextAllowed(w, start, loc)
			if !ok {
				return workflow.RunRef{}, fmt.Errorf("%w: QuietHours: the windows allow no minute of the day", workflow.ErrInvalid)
			}
			job.RunAt = at
			return next(ctx, job)
		}
	}
}

// nextAllowed returns the first whole minute after t that w allows on
// loc's wall clock, scanning absolute minutes so a DST gap or overlap is
// walked as it happens.
func nextAllowed(w workflow.Windows, t time.Time, loc *time.Location) (time.Time, bool) {
	m := t.Truncate(time.Minute)
	for end := t.Add(quietHorizon); m.Before(end); {
		m = m.Add(time.Minute)
		if w.Allows(m, loc) {
			return m, true
		}
	}
	return time.Time{}, false
}

// checkWindows refuses a window Allows would silently never match.
func checkWindows(w workflow.Windows) error {
	for _, list := range [][]workflow.Window{w.Between, w.Unless} {
		for _, win := range list {
			from, err1 := time.Parse("15:04", win.From)
			to, err2 := time.Parse("15:04", win.To)
			if err1 != nil || err2 != nil || len(win.From) != 5 || len(win.To) != 5 {
				return fmt.Errorf("%w: QuietHours: window %q-%q is not HH:MM", workflow.ErrInvalid, win.From, win.To)
			}
			if from.Equal(to) {
				return fmt.Errorf("%w: QuietHours: window %s-%s is empty: From equals To", workflow.ErrInvalid, win.From, win.To)
			}
		}
	}
	return nil
}
