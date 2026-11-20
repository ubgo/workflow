package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow/cron"
)

// LimitRuns returns s with an Active period holding exactly its first n
// ticks after from ("run this 5 times, then stop"). Active.From becomes
// the first of them and Active.Until lands just after the n-th: one
// second after it, or the next tick when that comes sooner, so the bound
// survives a store that keeps whole seconds. The limit is data on the
// schedule, so it is durable and the same on every node, with no counter
// to keep in step.
//
// It counts ticks, not runs: a tick skipped by ScheduleWhen, Windows,
// a pause, the overlap policy or a misfire still uses up one of the n,
// since the period is fixed when the schedule is made. Pass the clock's
// now as from for a schedule made now: a driver fires only ticks after
// the schedule exists, so a tick exactly at from is not counted. An
// Active.From already on s that is later than from is kept as the
// start (a tick exactly at it counts). parser reads Cron when s.Seconds
// is false (nil means cron.Parse); pass the driver's dialect if it has
// one.
//
// It refuses (ErrInvalid) n below 1, a zero from, an invalid schedule or
// expression, and an expression or an existing Active.Until that has
// fewer than n ticks from from on. The schedule then needs
// CapScheduleActive (memory, sqlstore, redis), which Client.Schedule
// checks. PruneEndedSchedules removes it once the period is over.
func LimitRuns(s Schedule, n int, from time.Time, parser cron.Parser) (Schedule, error) {
	if n < 1 {
		return s, invalidf("LimitRuns: n is %d, want at least 1", n)
	}
	if from.IsZero() {
		return s, invalidf("LimitRuns: from is zero")
	}
	if err := ValidateSchedule(s); err != nil {
		return s, err
	}
	sched, err := ParseCron(s.Cron, s.Seconds, parser)
	if err != nil {
		return s, fmt.Errorf("%w: LimitRuns: %w", ErrInvalid, err)
	}
	return limitTicks(s, sched, n, from)
}

// limitTicks sets s.Active to hold the first n ticks of sched after
// `after` (from Active.From on when that is later).
func limitTicks(s Schedule, sched cron.Schedule, n int, after time.Time) (Schedule, error) {
	if !s.Active.From.IsZero() && s.Active.From.After(after) {
		after = s.Active.From.Add(-time.Nanosecond)
	}
	var first, at time.Time
	at = after
	for i := 0; i < n; i++ {
		at = sched.Next(at, s.Location)
		if at.IsZero() || !s.Active.Contains(at) {
			return s, invalidf("LimitRuns: %q has only %d ticks after %s, want %d", s.Cron, i, after.Format(time.RFC3339), n)
		}
		if i == 0 {
			first = at
		}
	}
	until := at.Add(time.Second)
	if next := sched.Next(at, s.Location); !next.IsZero() && next.Before(until) {
		until = next
	}
	s.Active = Active{From: first, Until: until}
	return s, nil
}

// OneShot returns s set to fire once, at at: an Active period holding
// that one tick, in place of any Active s had. With an empty Cron it writes one
// from at on s.Location's wall clock ("30 9 14 10 *", or six fields with
// Seconds when at has seconds); with a Cron, at must be one of its ticks.
// Either way it refuses (ErrInvalid) an at the expression does not fire
// at, which includes a wall time that DST skips or repeats (a fall-back
// repeat fires only on its first occurrence), so a one-shot never fires
// at another time than the one asked. at must be whole seconds.
//
// It reads Cron with the standard dialect (cron.Parse, cron.ParseSeconds
// with Seconds); for a custom dialect call LimitRuns(s, 1,
// at.Add(-time.Nanosecond), parser) and check the tick. Needs CapScheduleActive, like LimitRuns. The
// schedule stays listed after its tick until it is unscheduled:
// PruneEndedSchedules removes ended ones.
func OneShot(s Schedule, at time.Time) (Schedule, error) {
	if at.IsZero() || at.Nanosecond() != 0 {
		return s, invalidf("OneShot: at must be a non-zero time in whole seconds")
	}
	if s.Location == nil || s.Location == time.Local {
		return s, invalidf("OneShot: schedule for %q needs a Location (not nil or time.Local)", s.Workflow)
	}
	if s.Cron == "" {
		local := at.In(s.Location)
		if local.Second() != 0 {
			s.Cron = fmt.Sprintf("%d %d %d %d %d *", local.Second(), local.Minute(), local.Hour(), local.Day(), int(local.Month()))
			s.Seconds = true
		} else {
			s.Cron = fmt.Sprintf("%d %d %d %d *", local.Minute(), local.Hour(), local.Day(), int(local.Month()))
		}
	}
	if err := ValidateSchedule(s); err != nil {
		return s, err
	}
	sched, err := ParseCron(s.Cron, s.Seconds, nil)
	if err != nil {
		return s, fmt.Errorf("%w: OneShot: %w", ErrInvalid, err)
	}
	if got := sched.Next(at.Add(-time.Nanosecond), s.Location); !got.Equal(at) {
		return s, invalidf("OneShot: %q has no tick at %s (the next is %s)", s.Cron, at.Format(time.RFC3339), got.Format(time.RFC3339))
	}
	s.Active = Active{} // the one tick is the whole period
	return limitTicks(s, sched, 1, at.Add(-time.Nanosecond))
}

// PruneOption tunes PruneEndedSchedules.
type PruneOption func(*pruneOptions)

type pruneOptions struct{ grace time.Duration }

// PruneGrace sets how long after Active.Until a schedule is kept, so the
// scheduler has fired its last tick before it is removed (a scheduler
// reaches a tick a little late, and a misfire catch-up later still).
// The default is MisfireGrace; a negative d is zero.
func PruneGrace(d time.Duration) PruneOption {
	return func(o *pruneOptions) { o.grace = max(d, 0) }
}

// PruneEndedSchedules unschedules every schedule whose Active period has
// ended (Active.Until plus the grace is at or before now), such as the
// ones LimitRuns and OneShot made, and returns their IDs in listing
// order. Schedules without an Until and declared Workflow.Cron entries
// (which only Register replaces) are left alone. A schedule removed
// meanwhile by someone else is skipped. On another error it stops and
// returns the IDs pruned so far with the error; calling it again is
// safe. Call it from a periodic job (a daily cron) or at boot.
//
// It needs the client to list schedules (ScheduleLister and
// CapScheduleList: memory, sqlstore, redis, Hatchet) and refuses with
// ErrUnsupported otherwise. now comes from the caller's clock.
func PruneEndedSchedules(ctx context.Context, c Client, now time.Time, opts ...PruneOption) ([]ScheduleID, error) {
	o := pruneOptions{grace: MisfireGrace}
	for _, opt := range opts {
		opt(&o)
	}
	lister, ok := c.(ScheduleLister)
	if !ok || !c.Capabilities().Has(CapScheduleList) {
		return nil, &UnsupportedError{Field: "PruneEndedSchedules", Missing: CapScheduleList}
	}
	list, err := lister.Schedules(ctx)
	if err != nil {
		return nil, err
	}
	var pruned []ScheduleID
	for _, info := range list {
		if info.Declared || info.Active.Until.IsZero() || now.Before(info.Active.Until.Add(o.grace)) {
			continue
		}
		if err := c.Unschedule(ctx, info.ID); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return pruned, fmt.Errorf("prune schedule %s: %w", info.ID, err)
		}
		pruned = append(pruned, info.ID)
	}
	return pruned, nil
}
