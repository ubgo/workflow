// Package cron parses cron expressions and computes their fire times.
//
// It is a parser and next-run calculator that import only the standard library, read no clock
// (every call takes the reference instant as an argument) and evaluate every
// schedule in an explicit time zone.
//
// # Syntax
//
// A standard five-field expression is "minute hour day-of-month month
// day-of-week". Each field is a comma-separated list of items; an item is
// "*", a value "5", or a range "1-10", optionally followed by a step "/n"
// ("*/15", "1-10/2", "5/20" meaning 5-max/20). Months accept JAN-DEC and
// weekdays SUN-SAT, case-insensitive. Sunday is 0 or 7.
//
// Macros: @yearly and @annually ("0 0 1 1 *"), @monthly ("0 0 1 * *"),
// @weekly ("0 0 * * 0"), @daily and @midnight ("0 0 * * *"), @hourly
// ("0 * * * *"), and "@every <duration>" for a fixed interval.
//
// # Day matching (the Vixie rule)
//
// When both day-of-month and day-of-week are restricted (neither field
// starts with "*"), a day matches if EITHER field matches: "0 0 1 * MON"
// fires on the 1st of every month and on every Monday. When either field
// starts with "*" (including "*/2"), both must match, as in Vixie cron.
//
// # Daylight saving time
//
// Wall-clock schedules are evaluated in the zone passed to Next. Two rules
// decide what happens when the wall clock jumps, chosen by the hour field:
//
//   - Fixed-time schedules (the hour field does not match all 24 hours, e.g.
//     "30 2 * * *"): a wall time that does not exist on that day (the
//     spring-forward gap) fires ONCE at the first valid instant after the
//     gap; a wall time that occurs twice (the fall-back overlap) fires ONCE,
//     on its first occurrence. A daily job therefore runs exactly once per
//     day.
//   - Periodic schedules (the hour field matches every hour, e.g.
//     "0 * * * *" or "* * * * *"): they follow real time. A wall time inside
//     the gap is not a real instant and does not fire; a wall time inside the
//     overlap fires on both occurrences. No real matching instant is skipped
//     or fired twice.
//
// "@every" intervals ignore the wall clock entirely, so DST never moves them.
//
// Builder writes expressions from named frequencies
// (Daily().At("9:00").Weekdays()); see builder.go.
package cron

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Parse is a Parser; the assertion keeps the two in step.
var _ Parser = Parse

// Schedule is a parsed schedule. Implementations are immutable and safe for
// concurrent use.
type Schedule interface {
	// Next returns the first fire time strictly after `after`, expressed in
	// loc. A nil loc means UTC, never the host zone, so the same schedule
	// behaves identically on every machine. It returns the zero time when no
	// fire time exists within searchYears of `after` (for example "0 0 30 2
	// *", February 30th); callers must check IsZero.
	Next(after time.Time, loc *time.Location) time.Time

	// Between returns the fire times t with from < t <= to, ascending, at
	// most limit of them (limit <= 0 returns nil). The half-open range lets
	// a caller chain windows (last checked, now] without double-counting a
	// fire; limit bounds memory when a misfire window is unexpectedly long.
	Between(from, to time.Time, loc *time.Location, limit int) []time.Time

	// String returns the expression as parsed (trimmed), so a schedule can
	// be stored and later re-parsed into an equivalent schedule.
	String() string
}

// searchYears bounds how far Next looks ahead before giving up. Eight years
// is the longest gap between two February 29ths (e.g. 2096 to 2104, since
// 2100 is not a leap year); one more year of margin covers any satisfiable
// expression, so a zero result means the expression can never fire.
const searchYears = 9

// minEvery is the shortest "@every" interval. Fire times are computed at
// one-second resolution, and a sub-second interval would turn a scheduler
// loop into a busy loop.
const minEvery = time.Second

// ParseError reports an invalid expression and names the offending field,
// so a typo fails loudly at registration instead of silently at run time.
type ParseError struct {
	Expr   string // the full expression as given
	Field  string // "minute", "hour", "day-of-month", "month", "day-of-week", "macro" or "expression"
	Value  string // the offending text within the field
	Reason string // what is wrong with it
}

// Error implements error.
func (e *ParseError) Error() string {
	if e.Value == "" {
		return fmt.Sprintf("cron: invalid %s in %q: %s", e.Field, e.Expr, e.Reason)
	}
	return fmt.Sprintf("cron: invalid %s %q in %q: %s", e.Field, e.Value, e.Expr, e.Reason)
}

// field describes one of the five positional fields.
type field struct {
	name     string
	min, max int
	names    map[string]int // accepted symbolic names, upper case
}

var (
	monthNames = map[string]int{"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
	dowNames   = map[string]int{"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}

	// Day-of-week allows 7 so "MON-SUN" and "5-7" work; 7 is folded onto 0.
	fields = [5]field{
		{name: "minute", min: 0, max: 59},
		{name: "hour", min: 0, max: 23},
		{name: "day-of-month", min: 1, max: 31},
		{name: "month", min: 1, max: 12, names: monthNames},
		{name: "day-of-week", min: 0, max: 7, names: dowNames},
	}

	macros = map[string]string{
		"@yearly":   "0 0 1 1 *",
		"@annually": "0 0 1 1 *",
		"@monthly":  "0 0 1 * *",
		"@weekly":   "0 0 * * 0",
		"@daily":    "0 0 * * *",
		"@midnight": "0 0 * * *",
		"@hourly":   "0 * * * *",
	}
)

const everyPrefix = "@every"

// allHours is the hour bitset of a schedule that matches every hour; such a
// schedule follows real time across DST transitions (see the package doc).
const allHours = uint64(1)<<24 - 1

// Parser turns an expression into a Schedule. Parse is the default every
// driver uses, which is what makes one expression mean the same on every
// driver. A driver that accepts a custom Parser (memory.WithCronParser)
// lets an app bring another dialect (the extended "L", "W" and "#"
// fields, a seconds field, "H" hashing): the app gives up that promise for the
// expressions only its parser understands, since an engine with its own
// cron dialect still refuses them. A Parser must return a Schedule that
// keeps the interface's contract (Next never returns a time at or before
// after; nil loc means UTC) and must be safe for concurrent use.
type Parser func(expr string) (Schedule, error)

// Parse parses a five-field cron expression, a macro, or "@every
// <duration>". Every error is a *ParseError naming the bad field. It is
// the default Parser.
func Parse(expr string) (Schedule, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil, &ParseError{Expr: expr, Field: "expression", Reason: "empty expression"}
	}
	if strings.HasPrefix(trimmed, "@") {
		return parseMacro(expr, trimmed)
	}
	return parseFields(expr, trimmed, trimmed)
}

func parseMacro(expr, trimmed string) (Schedule, error) {
	parts := strings.Fields(trimmed)
	name := strings.ToLower(parts[0])
	if name == everyPrefix {
		if len(parts) != 2 {
			return nil, &ParseError{Expr: expr, Field: "macro", Value: trimmed, Reason: "want \"@every <duration>\", e.g. \"@every 90s\""}
		}
		d, err := time.ParseDuration(parts[1])
		if err != nil {
			return nil, &ParseError{Expr: expr, Field: "macro", Value: parts[1], Reason: err.Error()}
		}
		if d < minEvery || d%time.Second != 0 {
			return nil, &ParseError{Expr: expr, Field: "macro", Value: parts[1], Reason: "interval must be a whole number of seconds, at least 1s"}
		}
		return &every{expr: trimmed, step: int64(d / time.Second)}, nil
	}
	std, ok := macros[name]
	if !ok || len(parts) != 1 {
		return nil, &ParseError{Expr: expr, Field: "macro", Value: parts[0], Reason: "unknown macro"}
	}
	return parseFields(expr, trimmed, std)
}

func parseFields(expr, trimmed, std string) (Schedule, error) {
	parts := strings.Fields(std)
	if len(parts) != len(fields) {
		return nil, &ParseError{Expr: expr, Field: "expression", Reason: fmt.Sprintf("want 5 fields (minute hour day-of-month month day-of-week), got %d", len(parts))}
	}
	s := &spec{expr: trimmed}
	var bits [5]uint64
	for i, f := range fields {
		b, err := parseField(parts[i], f)
		if err != nil {
			err.Expr = expr
			return nil, err
		}
		bits[i] = b
	}
	s.minute, s.hour, s.dom, s.month = bits[0], bits[1], bits[2], bits[3]
	// Fold Sunday-as-7 onto 0 so matching only checks bits 0-6.
	s.dow = bits[4] | bits[4]>>7&1
	s.dow &^= 1 << 7
	s.domStar = strings.HasPrefix(parts[2], "*")
	s.dowStar = strings.HasPrefix(parts[4], "*")
	return s, nil
}

// parseField returns the bitset of values the field matches.
func parseField(text string, f field) (uint64, *ParseError) {
	fail := func(value, reason string) *ParseError {
		return &ParseError{Field: f.name, Value: value, Reason: reason}
	}
	var bits uint64
	for _, item := range strings.Split(text, ",") {
		if item == "" {
			return 0, fail(text, "empty list item")
		}
		rng, stepText, hasStep := strings.Cut(item, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return 0, fail(item, "step must be a positive integer")
			}
			step = n
		}
		var lo, hi int
		switch {
		case rng == "*":
			lo, hi = f.min, f.max
			if f.max == 7 { // day-of-week: "*" is 0-6, 7 would duplicate Sunday
				hi = 6
			}
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			var err error
			if lo, err = parseValue(a, f); err != nil {
				return 0, fail(item, err.Error())
			}
			if hi, err = parseValue(b, f); err != nil {
				return 0, fail(item, err.Error())
			}
			if lo > hi {
				return 0, fail(item, fmt.Sprintf("range start %d is after end %d", lo, hi))
			}
		default:
			v, err := parseValue(rng, f)
			if err != nil {
				return 0, fail(item, err.Error())
			}
			lo, hi = v, v
			if hasStep { // "5/20" means 5-max/20
				hi = f.max
			}
		}
		for v := lo; v <= hi; v += step {
			bits |= 1 << uint(v)
		}
	}
	return bits, nil
}

func parseValue(text string, f field) (int, error) {
	if v, ok := f.names[strings.ToUpper(text)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(text)
	if err != nil {
		return 0, errors.New("not a number or a known name")
	}
	if v < f.min || v > f.max {
		return 0, fmt.Errorf("value %d out of range %d-%d", v, f.min, f.max)
	}
	return v, nil
}

// spec is a five-field wall-clock schedule. Each field is a bitset of the
// values it matches; dow uses bits 0-6 with Sunday at 0.
type spec struct {
	expr                          string
	minute, hour, dom, month, dow uint64
	domStar, dowStar              bool // field started with "*": the Vixie OR rule does not apply
}

func (s *spec) String() string { return s.expr }

func (s *spec) dayMatches(day time.Time) bool {
	domOK := s.dom&(1<<uint(day.Day())) != 0
	dowOK := s.dow&(1<<uint(day.Weekday())) != 0
	if s.domStar || s.dowStar {
		return domOK && dowOK
	}
	return domOK || dowOK
}

// Next implements Schedule. Days are walked as civil dates (in UTC, where
// AddDate never meets a DST shift) and each matching wall time is mapped to
// real instants by resolve, which applies the DST rules.
func (s *spec) Next(after time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	local := after.In(loc)
	// Start a day early: a gap at midnight can move a wall time across the
	// date line of the reference instant.
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	end := day.AddDate(searchYears, 0, 0)
	for ; !day.After(end); day = day.AddDate(0, 0, 1) {
		if s.month&(1<<uint(day.Month())) == 0 {
			// Jump to the last day of this month; the loop step lands on the 1st of the next.
			day = time.Date(day.Year(), day.Month()+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
			continue
		}
		best, ok := s.firstOnDay(day, after, loc)
		if !ok {
			continue
		}
		// Fire times of a later wall day are later instants, except across
		// an overlap that straddles midnight; checking the next day as well
		// covers that case.
		if t, ok := s.firstOnDay(day.AddDate(0, 0, 1), after, loc); ok && t.Before(best) {
			best = t
		}
		return best.In(loc)
	}
	return time.Time{}
}

// firstOnDay returns the earliest fire instant strictly after `after` among
// the wall times of the civil day `day`; false when the day does not match.
func (s *spec) firstOnDay(day, after time.Time, loc *time.Location) (time.Time, bool) {
	if s.month&(1<<uint(day.Month())) == 0 || !s.dayMatches(day) {
		return time.Time{}, false
	}
	periodic := s.hour == allHours
	var best time.Time
	found := false
	for h := 0; h < 24; h++ {
		if s.hour&(1<<uint(h)) == 0 {
			continue
		}
		for m := 0; m < 60; m++ {
			if s.minute&(1<<uint(m)) == 0 {
				continue
			}
			for _, t := range resolve(day.Year(), day.Month(), day.Day(), h, m, loc, periodic) {
				if t.After(after) && (!found || t.Before(best)) {
					best, found = t, true
				}
			}
		}
	}
	return best, found
}

// resolve maps a wall time in loc to the instants it fires at.
//
// Invariant: the result has 0, 1 or 2 ascending instants. An ordinary wall
// time yields one. A wall time in a spring-forward gap yields the instant
// the gap ends for a fixed-time schedule, nothing for a periodic one. A wall
// time in a fall-back overlap yields its first occurrence for a fixed-time
// schedule, both occurrences for a periodic one.
func resolve(y int, mo time.Month, d, h, mi int, loc *time.Location, periodic bool) []time.Time {
	wall := time.Date(y, mo, d, h, mi, 0, 0, time.UTC).Unix()
	// The offsets in force a day either side cover any transition near this
	// wall time (offsets never exceed a day, and zones change at most once
	// within a couple of days).
	before := offsetAt(wall-86400, loc)
	after := offsetAt(wall+86400, loc)
	var out []time.Time
	for _, off := range uniqueOffsets(before, offsetAt(wall, loc), after) {
		t := wall - int64(off)
		if offsetAt(t, loc) == off {
			out = append(out, time.Unix(t, 0))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	switch {
	case len(out) == 0:
		if periodic {
			return nil
		}
		// Applying the pre-gap offset lands inside the post-gap period;
		// that period's start is the first valid instant after the gap.
		start, _ := time.Unix(wall-int64(before), 0).In(loc).ZoneBounds()
		return []time.Time{start}
	case len(out) > 1 && !periodic:
		return out[:1]
	}
	return out
}

func offsetAt(unix int64, loc *time.Location) int {
	_, off := time.Unix(unix, 0).In(loc).Zone()
	return off
}

func uniqueOffsets(offs ...int) []int {
	var out []int
	for _, o := range offs {
		dup := false
		for _, p := range out {
			dup = dup || p == o
		}
		if !dup {
			out = append(out, o)
		}
	}
	return out
}

// Between implements Schedule.
func (s *spec) Between(from, to time.Time, loc *time.Location, limit int) []time.Time {
	return between(s, from, to, loc, limit)
}

func between(s Schedule, from, to time.Time, loc *time.Location, limit int) []time.Time {
	var out []time.Time
	for t := s.Next(from, loc); len(out) < limit && !t.IsZero() && !t.After(to); t = s.Next(t, loc) {
		out = append(out, t)
	}
	return out
}

// every is a fixed interval ("@every 90s"). Fire times are the multiples of
// the interval since the Unix epoch, so they are stable across restarts and
// nodes, and an interval never moves with DST: "@every 1h" is always 3600
// seconds apart.
type every struct {
	expr string
	step int64 // seconds, >= 1
}

func (e *every) String() string { return e.expr }

// Next implements Schedule. loc only sets the zone of the returned value.
func (e *every) Next(after time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	sec := after.Unix() // floors, also for negative instants with nanoseconds
	k := sec / e.step
	if sec%e.step != 0 && sec < 0 {
		k-- // floor division
	}
	return time.Unix((k+1)*e.step, 0).In(loc)
}

// Between implements Schedule.
func (e *every) Between(from, to time.Time, loc *time.Location, limit int) []time.Time {
	return between(e, from, to, loc, limit)
}
