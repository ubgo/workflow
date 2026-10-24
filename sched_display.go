package workflow

import (
	"time"

	"github.com/ubgo/workflow/cron"
)

// NextRun is one upcoming fire time shown two ways: on the schedule's
// wall clock and in UTC, so an operator in another zone reads both the
// time the schedule means and the instant it happens.
type NextRun struct {
	// Local is the fire time in the schedule's zone.
	Local time.Time
	// UTC is the same instant in UTC.
	UTC time.Time
}

// String renders r as "2026-10-15 09:00:00 CEST (07:00:00 UTC)"; the date
// in parentheses is added when the UTC day differs.
func (r NextRun) String() string {
	const day, clock = "2006-01-02", "15:04:05"
	utc := r.UTC.Format(clock)
	if r.UTC.Format(day) != r.Local.Format(day) {
		utc = r.UTC.Format(day + " " + clock)
	}
	return r.Local.Format(day+" "+clock+" MST") + " (" + utc + " UTC)"
}

// NextRunsQuery describes the schedule whose upcoming fire times
// NextRuns lists.
type NextRunsQuery struct {
	// Schedule is the parsed expression (cron.Parse, or the driver's
	// parser).
	Schedule cron.Schedule
	// Location is the schedule's zone; nil means UTC.
	Location *time.Location
	// After is the instant to list from (exclusive): the caller's clock,
	// never read here.
	After time.Time
	// Count is how many fire times to list; at most MaxNextRuns.
	Count int
	// Windows and Active drop the fire times the schedule would skip, so
	// the list shows runs that will really start.
	Windows Windows
	Active  Active
}

// MaxNextRuns caps NextRunsQuery.Count, and maxNextRunsScan the ticks
// examined: a window that rejects almost every tick must not turn a
// display call into a long loop.
const (
	MaxNextRuns     = 100
	maxNextRunsScan = 10000
)

// NextRuns lists the next q.Count fire times of q.Schedule that pass its
// Windows and Active period, each in the schedule's zone and in UTC (a
// dashboard's "next run" column). It returns fewer when the schedule ends
// or the scan limit is reached. It is a pure function: the reference time
// comes from q.After.
func NextRuns(q NextRunsQuery) []NextRun {
	if q.Schedule == nil || q.Count <= 0 {
		return nil
	}
	n := min(q.Count, MaxNextRuns)
	loc := q.Location
	if loc == nil {
		loc = time.UTC
	}
	out := make([]NextRun, 0, n)
	after := q.After
	for scanned := 0; len(out) < n && scanned < maxNextRunsScan; scanned++ {
		t := NextActive(q.Schedule, after, loc, q.Active)
		if t.IsZero() {
			break
		}
		after = t
		if !q.Windows.Allows(t, loc) {
			continue
		}
		out = append(out, NextRun{Local: t.In(loc), UTC: t.UTC()})
	}
	return out
}
