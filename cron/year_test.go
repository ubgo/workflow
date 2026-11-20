package cron

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestParseYearErrors(t *testing.T) {
	cases := []struct{ expr, field string }{
		{"", "expression"},
		{"0 0 9 * * *", "expression"},
		{"0 0 9 * * * 2026 1", "expression"},
		{"0 0 9 * * * 1969", "year"},
		{"0 0 9 * * * 2200", "year"},
		{"0 0 9 * * * 2028-2026", "year"},
		{"0 0 9 * * * */0", "year"},
		{"0 0 9 * * * 2026,", "year"},
		{"0 0 9 * * * twenty", "year"},
		{"0 0 12 ? * MON 2026", "day-of-month"},
		{"0 0 12 1 * ? 2026", "day-of-week"},
		{"60 0 9 * * * 2026", "second"},
		{"0 0 25 * * * 2026", "hour"},
		{"@foo", "macro"},
	}
	for _, c := range cases {
		_, err := ParseYear(c.expr)
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Errorf("ParseYear(%q) = %v, want *ParseError", c.expr, err)
			continue
		}
		if pe.Field != c.field {
			t.Errorf("ParseYear(%q) field = %q, want %q", c.expr, pe.Field, c.field)
		}
		if c.expr != "" && pe.Expr != c.expr {
			t.Errorf("ParseYear(%q) error names %q, want the whole expression", c.expr, pe.Expr)
		}
	}
}

func TestParseYearNext(t *testing.T) {
	at := func(y int, mo time.Month, d, h, mi, s int) time.Time {
		return time.Date(y, mo, d, h, mi, s, 0, time.UTC)
	}
	cases := []struct {
		expr  string
		after time.Time
		want  time.Time
		why   string
	}{
		{"0 0 9 1 JAN * 2027", at(2026, 6, 1, 0, 0, 0), at(2027, 1, 1, 9, 0, 0), "one fire in a later year"},
		{"0 0 9 1 JAN * 2027", at(2027, 1, 1, 9, 0, 0), time.Time{}, "nothing after the last fire"},
		{"0 30 8 * * MON-FRI 2026-2028", at(2026, 1, 2, 8, 30, 0), at(2026, 1, 5, 8, 30, 0), "a weekday range inside the years"},
		{"0 30 8 * * MON-FRI 2026-2028", at(2028, 12, 29, 8, 30, 0), time.Time{}, "Friday 29 December 2028 is the last fire"},
		{"0 0 0 1 1 * 2030/5", at(2026, 1, 1, 0, 0, 0), at(2030, 1, 1, 0, 0, 0), "a step starting at a year"},
		{"0 0 0 1 1 * 2030/5", at(2030, 1, 1, 0, 0, 0), at(2035, 1, 1, 0, 0, 0), "the step's next year"},
		{"0 0 0 1 1 * 2026,2040", at(2026, 1, 1, 0, 0, 0), at(2040, 1, 1, 0, 0, 0), "a list jumps the years between"},
		{"*/15 * * * * * *", at(2026, 1, 1, 0, 0, 0), at(2026, 1, 1, 0, 0, 15), "every year, seconds still apply"},
		{"0 0 0 1 1 * *", at(2199, 1, 1, 0, 0, 0), time.Time{}, "a star year ends after MaxYear"},
		{"0 0 0 1 1 * *", at(1960, 1, 1, 0, 0, 0), at(1970, 1, 1, 0, 0, 0), "before MinYear the first year is MinYear"},
		{"0 0 12 * * * 2026", at(2025, 12, 31, 23, 0, 0), at(2026, 1, 1, 12, 0, 0), "the year's first fire"},
		{"@daily", at(2026, 1, 1, 0, 0, 0), at(2026, 1, 2, 0, 0, 0), "a macro fires every year"},
	}
	for _, c := range cases {
		s, err := ParseYear(c.expr)
		if err != nil {
			t.Fatalf("ParseYear(%q): %v", c.expr, err)
		}
		if got := s.Next(c.after, nil); !got.Equal(c.want) {
			t.Errorf("%s: %q Next(%v) = %v, want %v", c.why, c.expr, c.after, got, c.want)
		}
		if s.String() != c.expr {
			t.Errorf("String() = %q, want %q", s.String(), c.expr)
		}
	}
}

// TestParseYearEnds lists every fire of a schedule limited to two years:
// Between stops at the last one however wide the window.
func TestParseYearEnds(t *testing.T) {
	s, err := ParseYear("0 0 9 1 */6 * 2026-2027")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	got := s.Between(from, from.AddDate(100, 0, 0), time.UTC, 100)
	want := []time.Time{
		time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC),
		time.Date(2027, 1, 1, 9, 0, 0, 0, time.UTC),
		time.Date(2027, 7, 1, 9, 0, 0, 0, time.UTC),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Between = %v, want %v", got, want)
	}
}

// TestParseYearLeapDay: 29 February exists in 2028 and 2032 but not in
// 2100, a century year that is not a leap year, so a schedule limited to
// 2100 never fires and one limited to leap years skips the others.
func TestParseYearLeapDay(t *testing.T) {
	leap, err := ParseYear("0 0 12 29 FEB * 2027-2032")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got := leap.Between(from, from.AddDate(20, 0, 0), time.UTC, 10)
	want := []time.Time{
		time.Date(2028, 2, 29, 12, 0, 0, 0, time.UTC),
		time.Date(2032, 2, 29, 12, 0, 0, 0, time.UTC),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("leap days = %v, want %v", got, want)
	}
	never, err := ParseYear("0 0 12 29 FEB * 2100")
	if err != nil {
		t.Fatal(err)
	}
	if next := never.Next(from, time.UTC); !next.IsZero() {
		t.Fatalf("29 February 2100 fired at %v; 2100 is not a leap year", next)
	}
}

// TestParseYearDST: in Berlin on 2027-03-28, 02:30 does not exist. The
// fixed-time rule of ParseSeconds applies: the fire moves to the end of
// the gap, once, and the year limit still holds after it.
func TestParseYearDST(t *testing.T) {
	berlin := mustLoad(t, "Europe/Berlin")
	s, err := ParseYear("10 30 2 28 MAR * 2027")
	if err != nil {
		t.Fatal(err)
	}
	got := s.Next(time.Date(2027, 1, 1, 0, 0, 0, 0, berlin), berlin)
	want := time.Date(2027, 3, 28, 3, 0, 10, 0, berlin)
	if !got.Equal(want) {
		t.Fatalf("gap: Next = %v, want %v", got, want)
	}
	if got.Location() != berlin {
		t.Fatalf("Next returned zone %v, want Berlin", got.Location())
	}
	if next := s.Next(got, berlin); !next.IsZero() {
		t.Fatalf("fired again at %v after the only year", next)
	}
}

// TestParseYearZoneDecidesTheYear: New Year's Eve 23:30 in New York is
// already 1 January in UTC; the year is the one on the schedule's zone.
func TestParseYearZoneDecidesTheYear(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	s, err := ParseYear("0 30 23 31 DEC * 2026")
	if err != nil {
		t.Fatal(err)
	}
	got := s.Next(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), ny)
	want := time.Date(2026, 12, 31, 23, 30, 0, 0, ny)
	if !got.Equal(want) {
		t.Fatalf("Next = %v, want %v", got, want)
	}
}

// TestParseYearNeverAutoDetected keeps the other parsers at their field
// counts: seven fields are read only by ParseYear.
func TestParseYearNeverAutoDetected(t *testing.T) {
	for name, p := range map[string]Parser{"Parse": Parse, "ParseSeconds": ParseSeconds} {
		if _, err := p("0 0 9 * * * 2026"); err == nil {
			t.Errorf("%s accepted seven fields", name)
		}
	}
}

func TestFieldsOf(t *testing.T) {
	cases := []struct {
		expr  string
		parse Parser
		want  Fields
	}{
		{"0 9 * * MON-FRI", Parse, Fields{
			Second: []int{0}, Minute: []int{0}, Hour: []int{9},
			DayOfMonth: seq(1, 31), Month: seq(1, 12), DayOfWeek: []int{1, 2, 3, 4, 5},
		}},
		{"0 0 1 * 7", Parse, Fields{
			Second: []int{0}, Minute: []int{0}, Hour: []int{0},
			DayOfMonth: []int{1}, Month: seq(1, 12), DayOfWeek: []int{0}, DayOr: true,
		}},
		{"*/20 0 9 * JAN,JUL * 2026-2027", ParseYear, Fields{
			Second: []int{0, 20, 40}, Minute: []int{0}, Hour: []int{9},
			DayOfMonth: seq(1, 31), Month: []int{1, 7}, DayOfWeek: seq(0, 6), Year: []int{2026, 2027},
		}},
		{"30 * * * * *", ParseSeconds, Fields{
			Second: []int{30}, Minute: seq(0, 59), Hour: seq(0, 23),
			DayOfMonth: seq(1, 31), Month: seq(1, 12), DayOfWeek: seq(0, 6),
		}},
	}
	for _, c := range cases {
		s, err := c.parse(c.expr)
		if err != nil {
			t.Fatalf("%q: %v", c.expr, err)
		}
		got, ok := FieldsOf(s)
		if !ok || !reflect.DeepEqual(got, c.want) {
			t.Errorf("FieldsOf(%q) = %+v, %v; want %+v", c.expr, got, ok, c.want)
		}
	}
	every, _ := Parse("@every 90s")
	if _, ok := FieldsOf(every); ok {
		t.Error("FieldsOf(@every) reported fields; an interval has none")
	}
}

func seq(lo, hi int) []int {
	var out []int
	for v := lo; v <= hi; v++ {
		out = append(out, v)
	}
	return out
}
