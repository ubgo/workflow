package cron

import (
	"errors"
	"testing"
	"time"
)

func TestParseExtendedNext(t *testing.T) {
	d := func(y int, mo time.Month, day int) time.Time { return utc(y, mo, day, 0, 0) }
	cases := []struct {
		name, expr string
		after      time.Time
		want       []time.Time
	}{
		{"last day", "0 0 L * *", d(2026, 1, 15), []time.Time{d(2026, 1, 31), d(2026, 2, 28), d(2026, 3, 31)}},
		{"last day in a leap year", "0 0 L 2 *", d(2027, 6, 1), []time.Time{d(2028, 2, 29), d(2029, 2, 28)}},
		{"two days before the last", "0 0 L-2 * *", d(2026, 1, 15), []time.Time{d(2026, 1, 29), d(2026, 2, 26)}},
		{"L-30 fires only in 31-day months", "0 0 L-30 * *", d(2026, 1, 15), []time.Time{d(2026, 3, 1), d(2026, 5, 1)}},
		{"15W: Sunday moves to Monday", "0 0 15W * *", d(2026, 2, 1), []time.Time{d(2026, 2, 16), d(2026, 3, 16), d(2026, 4, 15)}},
		{"1W: Saturday the 1st moves to Monday the 3rd", "0 0 1W * *", d(2026, 7, 15), []time.Time{d(2026, 8, 3), d(2026, 9, 1)}},
		{"31W: a Sunday 31st moves back to Friday, short months skip", "0 0 31W * *", d(2026, 5, 1), []time.Time{d(2026, 5, 29), d(2026, 7, 31)}},
		{"LW: last weekday", "0 0 LW * *", d(2026, 1, 1), []time.Time{d(2026, 1, 30), d(2026, 2, 27), d(2026, 3, 31)}},
		{"first Monday", "0 0 * * MON#1", d(2026, 1, 1), []time.Time{d(2026, 1, 5), d(2026, 2, 2), d(2026, 3, 2)}},
		{"last Friday", "0 0 * * FRIL", d(2026, 1, 1), []time.Time{d(2026, 1, 30), d(2026, 2, 27), d(2026, 3, 27)}},
		{"last Friday by number", "0 0 * * 5L", d(2026, 1, 1), []time.Time{d(2026, 1, 30)}},
		{"last Friday on the 31st, not the 24th", "0 0 * * FRIL", d(2026, 7, 1), []time.Time{d(2026, 7, 31)}},
		{"fifth Thursday skips months without one", "0 0 * * 4#5", d(2026, 1, 1), []time.Time{d(2026, 1, 29), d(2026, 4, 30)}},
		{"Sunday as 7", "0 0 * * 7#1", d(2026, 1, 1), []time.Time{d(2026, 1, 4)}},
		{"mixed list", "0 0 1,L * *", d(2026, 1, 15), []time.Time{d(2026, 1, 31), d(2026, 2, 1), d(2026, 2, 28)}},
		{"Vixie OR across a special day item", "0 0 L * FRI", d(2026, 1, 25), []time.Time{d(2026, 1, 30), d(2026, 1, 31), d(2026, 2, 6)}},
		{"special day and dow star ANDs", "0 0 L * *", d(2026, 4, 1), []time.Time{d(2026, 4, 30)}},
		{"special dow with dom star ANDs", "0 0 * 3 MON#2", d(2026, 1, 1), []time.Time{d(2026, 3, 9), d(2027, 3, 8)}},
	}
	for _, c := range cases {
		s, err := ParseExtended(c.expr)
		if err != nil {
			t.Fatalf("%s: ParseExtended(%q): %v", c.name, c.expr, err)
		}
		at := c.after
		for i, want := range c.want {
			got := s.Next(at, time.UTC)
			if !got.Equal(want) {
				t.Errorf("%s: step %d Next(%v) = %v, want %v", c.name, i, at, got, want)
				break
			}
			at = got
		}
		if s.String() != c.expr {
			t.Errorf("%s: String() = %q", c.name, s.String())
		}
	}
}

func TestParseExtendedDST(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	london := mustLoad(t, "Europe/London")
	cases := []struct {
		name, expr string
		loc        *time.Location
		after      time.Time
		want       []time.Time
	}{
		{"NY gap on the second Sunday of March fires once at gap end", "30 2 * * SUN#2", ny, utc(2026, 3, 1, 0, 0),
			[]time.Time{utc(2026, 3, 8, 7, 0), utc(2026, 4, 12, 6, 30)}},
		{"NY overlap on the first Sunday of November fires once", "30 1 * * 0#1", ny, utc(2026, 10, 31, 12, 0),
			[]time.Time{utc(2026, 11, 1, 5, 30), utc(2026, 12, 6, 6, 30)}},
		{"London gap on L-2 of March", "30 1 L-2 3 *", london, utc(2026, 3, 1, 0, 0),
			[]time.Time{utc(2026, 3, 29, 1, 0), utc(2027, 3, 29, 0, 30)}},
		{"London overlap on the last Sunday of October", "30 1 * * SUNL", london, utc(2026, 10, 1, 0, 0),
			[]time.Time{utc(2026, 10, 25, 0, 30), utc(2026, 11, 29, 1, 30)}},
		{"NY periodic on the gap day skips the missing hour", "0 * * * SUN#2", ny, utc(2026, 3, 8, 5, 30),
			[]time.Time{utc(2026, 3, 8, 6, 0), utc(2026, 3, 8, 7, 0)}},
	}
	for _, c := range cases {
		s, err := ParseExtended(c.expr)
		if err != nil {
			t.Fatal(err)
		}
		at := c.after
		for i, want := range c.want {
			got := s.Next(at, c.loc)
			if !got.Equal(want) || got.Location() != c.loc {
				t.Errorf("%s: step %d Next(%v) = %v, want %v in %v", c.name, i, at.UTC(), got, want, c.loc)
				break
			}
			at = got
		}
	}
}

func TestParseExtendedErrors(t *testing.T) {
	cases := []struct{ expr, field string }{
		{"", "expression"},
		{"0 0 L *", "expression"},
		{"0 0 L-31 * *", "day-of-month"},
		{"0 0 L-x * *", "day-of-month"},
		{"0 0 0W * *", "day-of-month"},
		{"0 0 32W * *", "day-of-month"},
		{"0 0 W * *", "day-of-month"},
		{"0 0 1#2 * *", "day-of-month"},
		{"0 0 5L * *", "day-of-month"},
		{"0 0 * * MON#6", "day-of-week"},
		{"0 0 * * MON#0", "day-of-week"},
		{"0 0 * * 8#1", "day-of-week"},
		{"0 0 * * L", "day-of-week"},
		{"0 0 * * XL", "day-of-week"},
		{"0 0 * * 5W", "day-of-week"},
		{"60 0 L * *", "minute"},
		{"0 24 * * FRIL", "hour"},
		{"0 0 L 13 *", "month"},
		{"0 0 L,40 * *", "day-of-month"},
		{"@nope", "macro"},
	}
	for _, c := range cases {
		_, err := ParseExtended(c.expr)
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Errorf("ParseExtended(%q) = %v, want *ParseError", c.expr, err)
			continue
		}
		if pe.Field != c.field || pe.Expr != c.expr {
			t.Errorf("ParseExtended(%q): field %q expr %q, want %q (%v)", c.expr, pe.Field, pe.Expr, c.field, err)
		}
	}
}

// TestParseExtendedPlainIsParse: an expression without a special item is
// exactly what Parse returns, so switching a driver to Extended changes no
// existing schedule.
func TestParseExtendedPlainIsParse(t *testing.T) {
	for _, expr := range []string{"0 9 * * MON-FRI", "*/15 * * * *", "0 0 * * WED", "@daily", "@every 90s", "0 0 1 * MON"} {
		a, err := ParseExtended(expr)
		if err != nil {
			t.Fatalf("%q: %v", expr, err)
		}
		b := mustParse(t, expr)
		if _, ext := a.(*extSpec); ext {
			t.Errorf("%q parsed as an extended spec", expr)
		}
		from := utc(2026, 1, 1, 0, 0)
		ga, gb := a.Between(from, from.AddDate(0, 2, 0), time.UTC, 500), b.Between(from, from.AddDate(0, 2, 0), time.UTC, 500)
		if len(ga) != len(gb) || len(ga) == 0 || !ga[len(ga)-1].Equal(gb[len(gb)-1]) {
			t.Errorf("%q: Extended and Parse disagree", expr)
		}
	}
	var _ Parser = Extended
	if _, err := Parse("0 0 L * *"); err == nil {
		t.Fatal("the standard dialect accepted L: the extended dialect must stay opt-in")
	}
}
