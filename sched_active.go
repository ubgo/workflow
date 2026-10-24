package workflow

import (
	"fmt"
	"time"

	"github.com/ubgo/workflow/cron"
)

// Active is the period a schedule is live: ticks t with From <= t < Until
// fire, and ticks outside it are not ticks at all (no TickSkipped, no
// misfire catch-up across the time before From). A zero From means no
// start and a zero Until no end; an Until at or before From is refused,
// since it would mean a schedule that never fires.
type Active struct {
	From, Until time.Time
}

// IsZero reports whether a limits nothing.
func (a Active) IsZero() bool { return a.From.IsZero() && a.Until.IsZero() }

// Contains reports whether the tick at t is inside a.
func (a Active) Contains(t time.Time) bool {
	if !a.From.IsZero() && t.Before(a.From) {
		return false
	}
	return a.Until.IsZero() || t.Before(a.Until)
}

// Ended reports whether no tick after `after` can be inside a, so a driver
// can stop evaluating the schedule.
func (a Active) Ended(after time.Time) bool {
	return !a.Until.IsZero() && !after.Before(a.Until.Add(-time.Nanosecond))
}

// validate refuses an empty period.
func (a Active) validate() error {
	if !a.From.IsZero() && !a.Until.IsZero() && !a.Until.After(a.From) {
		return fmt.Errorf("active period: Until %s is not after From %s", a.Until.Format(time.RFC3339), a.From.Format(time.RFC3339))
	}
	return nil
}

// NextActive is s.Next limited to a: the first fire time strictly after
// `after` that lies inside a, or the zero time when the period has ended
// (or s never fires again). Before From it starts the search just before
// From, so a tick exactly at From fires. Every driver computes ticks of a
// schedule with an Active period through it, so the bounds mean the same
// everywhere.
func NextActive(s cron.Schedule, after time.Time, loc *time.Location, a Active) time.Time {
	if !a.From.IsZero() && after.Before(a.From) {
		after = a.From.Add(-time.Nanosecond)
	}
	t := s.Next(after, loc)
	if t.IsZero() || !a.Contains(t) {
		return time.Time{}
	}
	return t
}
