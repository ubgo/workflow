package cron

import (
	"errors"
	"testing"
	"time"
)

func TestParseSecondsErrors(t *testing.T) {
	cases := []struct{ expr, field string }{
		{"", "expression"},
		{"* * * * *", "expression"},
		{"* * * * * * *", "expression"},
		{"60 * * * * *", "second"},
		{"*/0 * * * * *", "second"},
		{"0 60 * * * *", "minute"},
		{"0 * * * * 8", "day-of-week"},
		{"@foo", "macro"},
	}
	for _, c := range cases {
		_, err := ParseSeconds(c.expr)
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Errorf("ParseSeconds(%q) = %v, want *ParseError", c.expr, err)
			continue
		}
		if pe.Field != c.field {
			t.Errorf("ParseSeconds(%q) field = %q, want %q", c.expr, pe.Field, c.field)
		}
	}
}

func TestParseSecondsNext(t *testing.T) {
	sec := func(y int, mo time.Month, d, h, mi, s int) time.Time {
		return time.Date(y, mo, d, h, mi, s, 0, time.UTC)
	}
	cases := []struct {
		expr  string
		after time.Time
		want  time.Time
		why   string
	}{
		{"*/10 * * * * *", sec(2026, 1, 1, 0, 0, 0), sec(2026, 1, 1, 0, 0, 10), "next ten seconds"},
		{"*/10 * * * * *", sec(2026, 1, 1, 0, 0, 55), sec(2026, 1, 1, 0, 1, 0), "rolls into the next minute"},
		{"30 0 9 * * MON-FRI", sec(2026, 1, 2, 9, 0, 29), sec(2026, 1, 2, 9, 0, 30), "later second of the current minute"},
		{"30 0 9 * * MON-FRI", sec(2026, 1, 2, 9, 0, 30), sec(2026, 1, 5, 9, 0, 30), "Friday to Monday"},
		{"15,45 * * * * *", sec(2026, 1, 1, 0, 0, 20), sec(2026, 1, 1, 0, 0, 45), "second item of a list"},
		{"@daily", sec(2026, 1, 1, 0, 0, 0), sec(2026, 1, 2, 0, 0, 0), "macro fires at second 0"},
		{"@every 15s", sec(2026, 1, 1, 0, 0, 1), sec(2026, 1, 1, 0, 0, 15), "every is unchanged"},
	}
	for _, c := range cases {
		s, err := ParseSeconds(c.expr)
		if err != nil {
			t.Fatalf("ParseSeconds(%q): %v", c.expr, err)
		}
		if got := s.Next(c.after, nil); !got.Equal(c.want) {
			t.Errorf("%s: %q Next(%v) = %v, want %v", c.why, c.expr, c.after, got, c.want)
		}
		if s.String() != c.expr {
			t.Errorf("String() = %q, want %q", s.String(), c.expr)
		}
	}
}

// TestParseSecondsDST: in New York on 2026-03-08, 02:30 does not exist.
// A fixed-time schedule fires at the gap's end plus its seconds, once.
func TestParseSecondsDST(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	s, err := ParseSeconds("20 30 2 * * *")
	if err != nil {
		t.Fatal(err)
	}
	after := time.Date(2026, 3, 8, 0, 0, 0, 0, ny)
	got := s.Next(after, ny)
	want := time.Date(2026, 3, 8, 3, 0, 20, 0, ny)
	if !got.Equal(want) {
		t.Fatalf("gap: Next = %v, want %v", got, want)
	}
	if next := s.Next(got, ny); next.Day() != 9 {
		t.Fatalf("gap fired twice: second fire %v", next)
	}
}

// TestParseSecondsNeverAutoDetected keeps Parse at five fields: a
// six-field expression is ambiguous and only ParseSeconds reads it.
func TestParseSecondsNeverAutoDetected(t *testing.T) {
	if _, err := Parse("0 0 9 * * *"); err == nil {
		t.Fatal("Parse accepted six fields")
	}
}
