package cron

import (
	"fmt"
	"strings"
	"time"
)

// ParseSeconds is a Parser for the six-field dialect with a seconds field
// first: "second minute hour day-of-month month day-of-week" ("*/10 * * *
// * *" fires every ten seconds, "30 0 9 * * MON-FRI" at 09:00:30 on
// weekdays). The other five fields, the macros and "@every" mean exactly
// what they mean to Parse; a macro fires at second 0.
//
// It is opt-in and never auto-detected, because six fields are ambiguous:
// some dialects put seconds first, while others put a year last. Pass it where a driver takes a Parser
// (memory.WithCronParser and the SQL driver's WithCronParser); an engine
// with its own five-field dialect (Hatchet) still refuses the
// expressions, so a schedule written for it is no longer portable to
// every driver.
//
// DST follows the minute the seconds belong to: a fixed-time minute that
// falls in a spring-forward gap fires at the first instant after the gap
// plus its seconds, and the periodic rule (the hour field matches every
// hour) fires both occurrences of an overlapping minute.
func ParseSeconds(expr string) (Schedule, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil, &ParseError{Expr: expr, Field: "expression", Reason: "empty expression"}
	}
	if strings.HasPrefix(trimmed, "@") {
		inner, err := parseMacro(expr, trimmed)
		if err != nil {
			return nil, err
		}
		if e, ok := inner.(*every); ok {
			return e, nil
		}
		return &secSpec{expr: trimmed, second: 1, inner: inner.(*spec)}, nil
	}
	parts := strings.Fields(trimmed)
	if len(parts) != len(fields)+1 {
		return nil, &ParseError{Expr: expr, Field: "expression", Reason: fmt.Sprintf("want 6 fields (second minute hour day-of-month month day-of-week), got %d", len(parts))}
	}
	sec, perr := parseField(parts[0], secondField)
	if perr != nil {
		perr.Expr = expr
		return nil, perr
	}
	inner, err := parseFields(expr, trimmed, strings.Join(parts[1:], " "))
	if err != nil {
		return nil, err
	}
	return &secSpec{expr: trimmed, second: sec, inner: inner.(*spec)}, nil
}

// ParseSeconds is a Parser.
var _ Parser = ParseSeconds

// secondField is the seconds field of the six-field dialect.
var secondField = field{name: "second", min: 0, max: 59}

// secSpec is a five-field spec with a seconds bitset in front: it fires at
// each matching second of each minute the inner spec fires at.
type secSpec struct {
	expr   string
	second uint64 // bits 0-59
	inner  *spec
}

func (s *secSpec) String() string { return s.expr }

// Next implements Schedule. A minute the inner spec fires at is an instant
// m (DST already applied); its fire times are m+s for each second s in the
// bitset. The minute containing `after` is checked first, because a later
// second of it may still be due; otherwise the next minute the inner spec
// fires at gives its first second.
func (s *secSpec) Next(after time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	start := after.Truncate(time.Minute)
	if s.inner.Next(start.Add(-time.Second), loc).Equal(start) {
		if t, ok := s.secondAfter(start, after); ok {
			return t.In(loc)
		}
	}
	for m := s.inner.Next(after, loc); !m.IsZero(); m = s.inner.Next(m, loc) {
		if t, ok := s.secondAfter(m, after); ok {
			return t.In(loc)
		}
	}
	return time.Time{}
}

// secondAfter returns the first minute+second strictly after `after`.
func (s *secSpec) secondAfter(minute, after time.Time) (time.Time, bool) {
	for sec := 0; sec < 60; sec++ {
		if s.second&(1<<uint(sec)) == 0 {
			continue
		}
		if t := minute.Add(time.Duration(sec) * time.Second); t.After(after) {
			return t, true
		}
	}
	return time.Time{}, false
}

// Between implements Schedule.
func (s *secSpec) Between(from, to time.Time, loc *time.Location, limit int) []time.Time {
	return between(s, from, to, loc, limit)
}
