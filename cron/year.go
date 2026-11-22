package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// The bounds of the year field. 1970 is where Unix time starts; 2199 gives
// a schedule two centuries of room while keeping the year set a small
// fixed bitset. A schedule with "*" in the year field stops after 2199.
const (
	//ds:def id=cron-minyear-2dge929e owner=@khanakia stability=stable
	MinYear = 1970
	//ds:def id=cron-maxyear-25rqcrwu owner=@khanakia stability=stable
	MaxYear = 2199
)

// yearField is the year field of the seven-field dialect.
var yearField = field{name: "year", min: MinYear, max: MaxYear}

// ParseYear is a Parser for the seven-field dialect with a seconds field
// first and a year field last: "second minute hour day-of-month month
// day-of-week year" ("0 0 9 1 JAN * 2027" fires at 09:00 on 1 January
// 2027 only, "0 30 8 * * MON-FRI 2026-2028" on weekdays for three years).
// The year field takes the same items as the others ("*", a year, a list,
// a range, a step) over MinYear-MaxYear, and the six fields before it mean
// exactly what they mean to ParseSeconds; a macro or "@every" is read by
// ParseSeconds and fires every year.
//
// A schedule whose last year has passed never fires again: Next returns
// the zero time after its last fire, the same "never" a 30 February
// schedule gives, so a driver stops it instead of looping. That is the
// point of a year field: a campaign, a migration window or a one-off
// event written as a cron.
//
// The field order is the one other seven-field schedulers and EventBridge
// cron() use, but not their dialect: day-of-week numbers are this
// package's (Sunday is 0 or 7, where those dialects count 1-7 from
// Sunday), and their "?" is refused rather than read as "*", so a pasted
// expression with numeric weekdays fails at parse time instead of firing
// a day off. Day names (MON-FRI) mean the same in both.
//
// It is opt-in and never auto-detected. Pass it where a driver takes a
// Parser (memory.WithCronParser, the SQL and Redis drivers'
// WithCronParser) and leave Schedule.Seconds unset: the seconds flag
// selects ParseSeconds and would refuse seven fields. An engine with its
// own cron dialect (Hatchet) still refuses the expressions.
//
// DST is handled as by ParseSeconds; a fire time belongs to the year it
// falls in on the schedule's zone.
//
//ds:def id=cron-parseyear-rv99u3rm owner=@khanakia stability=stable
func ParseYear(expr string) (Schedule, error) {
	trimmed := strings.TrimSpace(expr)
	if strings.HasPrefix(trimmed, "@") || trimmed == "" {
		return ParseSeconds(expr)
	}
	parts := strings.Fields(trimmed)
	if len(parts) != len(fields)+2 {
		return nil, &ParseError{Expr: expr, Field: "expression", Reason: fmt.Sprintf("want 7 fields (second minute hour day-of-month month day-of-week year), got %d", len(parts))}
	}
	for _, i := range []int{3, 5} {
		if strings.Contains(parts[i], "?") {
			return nil, &ParseError{Expr: expr, Field: fields[i-1].name, Value: parts[i], Reason: `"?" is not part of this dialect; write "*" (and day-of-week numbers as 0-6, Sunday 0)`}
		}
	}
	years, perr := parseYearField(parts[6])
	if perr != nil {
		perr.Expr = expr
		return nil, perr
	}
	inner, err := ParseSeconds(strings.Join(parts[:6], " "))
	if err != nil {
		var pe *ParseError
		if errors.As(err, &pe) {
			pe.Expr = expr
		}
		return nil, err
	}
	return &yearSpec{expr: trimmed, years: years, inner: inner.(*secSpec)}, nil
}

// ParseYear is a Parser.
var _ Parser = ParseYear

// yearBits is a set of years: bit y-MinYear is set when year y matches.
type yearBits [(MaxYear - MinYear + 64) / 64]uint64

func (b *yearBits) set(y int) { b[(y-MinYear)/64] |= 1 << uint((y-MinYear)%64) }
func (b yearBits) has(y int) bool {
	return y >= MinYear && y <= MaxYear && b[(y-MinYear)/64]&(1<<uint((y-MinYear)%64)) != 0
}

// parseYearField reads the year field with the grammar of parseField. It
// is separate because a uint64 bitset holds 64 values and the year range
// holds 230.
func parseYearField(text string) (yearBits, *ParseError) {
	fail := func(value, reason string) *ParseError {
		return &ParseError{Field: yearField.name, Value: value, Reason: reason}
	}
	var bits yearBits
	for _, item := range strings.Split(text, ",") {
		if item == "" {
			return bits, fail(text, "empty list item")
		}
		rng, stepText, hasStep := strings.Cut(item, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return bits, fail(item, "step must be a positive integer")
			}
			step = n
		}
		lo, hi := yearField.min, yearField.max
		switch {
		case rng == "*":
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			var err error
			if lo, err = parseValue(a, yearField); err != nil {
				return bits, fail(item, err.Error())
			}
			if hi, err = parseValue(b, yearField); err != nil {
				return bits, fail(item, err.Error())
			}
			if lo > hi {
				return bits, fail(item, fmt.Sprintf("range start %d is after end %d", lo, hi))
			}
		default:
			v, err := parseValue(rng, yearField)
			if err != nil {
				return bits, fail(item, err.Error())
			}
			lo = v
			if !hasStep { // "2026/2" means 2026-max/2
				hi = v
			}
		}
		for v := lo; v <= hi; v += step {
			bits.set(v)
		}
	}
	return bits, nil
}

// yearSpec is a six-field spec limited to a set of years.
type yearSpec struct {
	expr  string
	years yearBits
	inner *secSpec
}

func (s *yearSpec) String() string { return s.expr }

// Next implements Schedule. It walks the matching years from the one
// `after` falls in: in each, the inner schedule's first fire after the
// later of `after` and the year's start is the answer when it falls in
// that year. When it falls in a later year, every year in between has no
// fire either, so the walk jumps there. Past the last matching year the
// result is the zero time.
func (s *yearSpec) Next(after time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	first := after.In(loc).Year()
	for y := max(first, MinYear); y <= MaxYear; y++ {
		if !s.years.has(y) {
			continue
		}
		from := after
		if y > first {
			from = time.Date(y, time.January, 1, 0, 0, 0, 0, loc).Add(-time.Nanosecond)
		}
		t := s.inner.Next(from, loc)
		if t.IsZero() {
			return time.Time{} // the six fields never fire at all
		}
		ty := t.In(loc).Year()
		if ty == y {
			return t
		}
		y = ty - 1 // no fire in y..ty-1; the loop step lands on ty
	}
	return time.Time{}
}

// Between implements Schedule.
func (s *yearSpec) Between(from, to time.Time, loc *time.Location, limit int) []time.Time {
	return between(s, from, to, loc, limit)
}
