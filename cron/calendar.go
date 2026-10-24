package cron

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ErrCalendar is wrapped by every error Calendar returns, so a caller can
// tell a bad structured spec from a parse error of a hand-written
// expression.
var ErrCalendar = errors.New("cron: invalid calendar")

// Calendar is a structured calendar spec: the fire times as lists of field
// values instead of a cron string ("hour 9, minutes 0 and 30, Monday to
// Friday"). An empty list means every value of that field, except Minute,
// where empty means minute 0, so Calendar{Hour: []int{9}} fires once at
// 09:00 rather than sixty times. It compiles to the five-field expression
// a person would write (Expr), never a new format, so every driver takes
// it, and a stored schedule stays plain text.
//
// Day matching is AND, the way people read a calendar: DayOfMonth and
// DayOfWeek together would need "the 13th AND a Friday", which a cron
// expression cannot say (cron ORs two restricted day fields), so setting
// both is refused rather than compiled into an expression that fires on
// every 13th and every Friday.
type Calendar struct {
	// Minute lists minutes 0-59; empty means minute 0.
	Minute []int
	// Hour lists hours 0-23; empty means every hour.
	Hour []int
	// DayOfMonth lists days 1-31; empty means every day.
	DayOfMonth []int
	// Month lists months; empty means every month.
	Month []time.Month
	// DayOfWeek lists weekdays; empty means every day of the week.
	DayOfWeek []time.Weekday
}

// Expr returns the five-field expression of c, with consecutive values
// written as ranges ("1-5") and lists in ascending order. Every error
// wraps ErrCalendar and names the field.
func (c Calendar) Expr() (string, error) {
	if len(c.DayOfMonth) > 0 && len(c.DayOfWeek) > 0 {
		return "", fmt.Errorf("%w: DayOfMonth and DayOfWeek both set: cron would fire on either, not on both; use one and check the other in the job", ErrCalendar)
	}
	minute := c.Minute
	if len(minute) == 0 {
		minute = []int{0}
	}
	months := make([]int, len(c.Month))
	for i, m := range c.Month {
		months[i] = int(m)
	}
	days := make([]int, len(c.DayOfWeek))
	for i, d := range c.DayOfWeek {
		days[i] = int(d)
	}
	parts := make([]string, 0, len(fields))
	for i, vals := range [][]int{minute, c.Hour, c.DayOfMonth, months, days} {
		f := fields[i]
		hi := f.max
		if hi == 7 { // day-of-week: time.Weekday is 0-6
			hi = 6
		}
		text, err := listExpr(vals, f.min, hi)
		if err != nil {
			return "", fmt.Errorf("%w: %s: %w", ErrCalendar, f.name, err)
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, " "), nil
}

// Schedule compiles c and parses the result with Parse.
func (c Calendar) Schedule() (Schedule, error) {
	expr, err := c.Expr()
	if err != nil {
		return nil, err
	}
	return Parse(expr)
}

// listExpr writes a field's values as a cron list: "*" for none, runs of
// consecutive values as "a-b", duplicates dropped.
func listExpr(vals []int, lo, hi int) (string, error) {
	if len(vals) == 0 {
		return wild, nil
	}
	sorted := slices.Clone(vals)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	if sorted[0] < lo || sorted[len(sorted)-1] > hi {
		return "", fmt.Errorf("values must be %d-%d, got %v", lo, hi, vals)
	}
	var items []string
	for i := 0; i < len(sorted); {
		j := i
		for j+1 < len(sorted) && sorted[j+1] == sorted[j]+1 {
			j++
		}
		switch {
		case j == i:
			items = append(items, strconv.Itoa(sorted[i]))
		default:
			items = append(items, strconv.Itoa(sorted[i])+"-"+strconv.Itoa(sorted[j]))
		}
		i = j + 1
	}
	return strings.Join(items, ","), nil
}
