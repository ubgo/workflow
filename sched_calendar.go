package workflow

import (
	"context"
	"time"
)

// ExclusionCalendar says which instants a schedule must not fire at:
// public holidays, a company shutdown, weekends. It is the question a
// schedule asks before each tick (SkipExcluded); the answers shipped
// here are a date set, a weekday set and a function, and an app plugs in
// its own (a holiday API, a table it maintains) by implementing the one
// method. Excluded must be safe for concurrent use and return quickly: it
// runs on the firing node before every tick.
type ExclusionCalendar interface {
	Excluded(t time.Time) bool
}

// ExcludeFunc adapts a function to ExclusionCalendar. A nil function
// excludes nothing.
type ExcludeFunc func(t time.Time) bool

// Excluded implements ExclusionCalendar.
func (f ExcludeFunc) Excluded(t time.Time) bool { return f != nil && f(t) }

// dateSet is ExcludeDates' calendar.
type dateSet struct {
	loc   *time.Location
	dates map[string]bool
}

func (d dateSet) Excluded(t time.Time) bool { return d.dates[t.In(d.loc).Format(time.DateOnly)] }

// ExcludeDates returns a calendar excluding whole days, given as
// "2026-12-25", on loc's wall clock: an instant is excluded when its date
// in loc is one of them, so a 23:30 UTC tick is on the next day in
// Berlin. loc is required and time.Local is refused, as for
// Schedule.Location; usually it is the schedule's own zone. A date that
// does not parse is refused. Errors wrap ErrInvalid.
func ExcludeDates(loc *time.Location, dates ...string) (ExclusionCalendar, error) {
	if err := calendarZone("ExcludeDates", loc); err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(dates))
	for _, d := range dates {
		t, err := time.Parse(time.DateOnly, d)
		if err != nil || t.Format(time.DateOnly) != d {
			return nil, invalidf("ExcludeDates: %q is not a date in YYYY-MM-DD form", d)
		}
		set[d] = true
	}
	return dateSet{loc: loc, dates: set}, nil
}

// weekdaySet is ExcludeWeekdays' calendar.
type weekdaySet struct {
	loc  *time.Location
	days [7]bool
}

func (w weekdaySet) Excluded(t time.Time) bool { return w.days[t.In(w.loc).Weekday()] }

// ExcludeWeekdays returns a calendar excluding the given weekdays on
// loc's wall clock (ExcludeWeekdays(berlin, time.Saturday, time.Sunday)
// keeps a schedule to working days). loc is required and time.Local is
// refused; a weekday outside Sunday to Saturday is refused. Errors wrap
// ErrInvalid.
func ExcludeWeekdays(loc *time.Location, days ...time.Weekday) (ExclusionCalendar, error) {
	if err := calendarZone("ExcludeWeekdays", loc); err != nil {
		return nil, err
	}
	w := weekdaySet{loc: loc}
	for _, d := range days {
		if d < time.Sunday || d > time.Saturday {
			return nil, invalidf("ExcludeWeekdays: %d is not a weekday", d)
		}
		w.days[d] = true
	}
	return w, nil
}

func calendarZone(name string, loc *time.Location) error {
	if loc == nil || loc == time.Local {
		return invalidf("%s needs an explicit location, not nil or time.Local", name)
	}
	return nil
}

// anyCalendar is AnyExcluded's calendar.
type anyCalendar []ExclusionCalendar

func (a anyCalendar) Excluded(t time.Time) bool {
	for _, c := range a {
		if c != nil && c.Excluded(t) {
			return true
		}
	}
	return false
}

// AnyExcluded returns a calendar excluding an instant when any of cals
// does (holidays plus weekends). Nil calendars are ignored; with none it
// excludes nothing.
func AnyExcluded(cals ...ExclusionCalendar) ExclusionCalendar {
	return anyCalendar(append([]ExclusionCalendar(nil), cals...))
}

// SkipExcluded returns a Workflow.ScheduleWhen that skips every tick
// whose time (Tick.At, the tick's logical time, not the firing node's
// clock) cal excludes. A skipped tick is reported as TickSkipped with
// TickReasonWhen, and the misfire policy never brings it back: an
// excluded day is not a missed one. A nil cal fires every tick. Combine
// it with other conditions through AllWhen:
//
//	wf.ScheduleWhen = workflow.AllWhen(
//		workflow.InEnvironment(env, "production"),
//		workflow.SkipExcluded(holidays),
//	)
//
// It needs CapScheduleWhen (memory, sqlstore, redis); a driver without it
// refuses the workflow at Register or Start, never fires the excluded
// ticks silently. It applies to every schedule of the workflow, declared
// Cron and Client.Schedule alike; switch on Tick.Name inside an
// ExcludeFunc for a calendar per schedule.
func SkipExcluded(cal ExclusionCalendar) func(context.Context, Tick) bool {
	return func(_ context.Context, t Tick) bool {
		return cal == nil || !cal.Excluded(t.At)
	}
}
