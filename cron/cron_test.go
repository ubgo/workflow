package cron

import (
	"errors"
	"testing"
	"time"
)

func mustLoad(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("load %s: %v", name, err)
	}
	return loc
}

func mustParse(t *testing.T, expr string) Schedule {
	t.Helper()
	s, err := Parse(expr)
	if err != nil {
		t.Fatalf("Parse(%q): %v", expr, err)
	}
	return s
}

func utc(y int, mo time.Month, d, h, mi int) time.Time {
	return time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		expr, field string
	}{
		{"", "expression"},
		{"   ", "expression"},
		{"* * * *", "expression"},
		{"* * * * * *", "expression"},
		{"60 * * * *", "minute"},
		{"-1 * * * *", "minute"},
		{"* 24 * * *", "hour"},
		{"* * 0 * *", "day-of-month"},
		{"* * 32 * *", "day-of-month"},
		{"* * * 13 *", "month"},
		{"* * * FOO *", "month"},
		{"* * * * 8", "day-of-week"},
		{"* * * * FRI-MON", "day-of-week"},
		{"*/0 * * * *", "minute"},
		{"*/x * * * *", "minute"},
		{"5-1 * * * *", "minute"},
		{"1,,2 * * * *", "minute"},
		{"a * * * *", "minute"},
		{"@every", "macro"},
		{"@every 500ms", "macro"},
		{"@every 1500ms", "macro"},
		{"@every -1s", "macro"},
		{"@every soon", "macro"},
		{"@foo", "macro"},
		{"@daily extra", "macro"},
	}
	for _, c := range cases {
		_, err := Parse(c.expr)
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Errorf("Parse(%q) error = %v, want *ParseError", c.expr, err)
			continue
		}
		if pe.Field != c.field {
			t.Errorf("Parse(%q) field = %q, want %q (%v)", c.expr, pe.Field, c.field, err)
		}
		if pe.Expr != c.expr {
			t.Errorf("Parse(%q) Expr = %q", c.expr, pe.Expr)
		}
	}
}

func TestParseFieldBits(t *testing.T) {
	cases := []struct {
		expr                          string
		minute, hour, dom, month, dow uint64
	}{
		{"1-10/3 * * * *", 1<<1 | 1<<4 | 1<<7 | 1<<10, allHours, 1<<32 - 2, 1<<13 - 2, 1<<7 - 1},
		{"5/20 0 1 JAN sun", 1<<5 | 1<<25 | 1<<45, 1, 1 << 1, 1 << 1, 1},
		{"0,30 9-17 * jan-Mar MON-FRI", 1 | 1<<30, 0x3fe00, 1<<32 - 2, 0xe, 0x3e},
		{"0 0 * * 7", 1, 1, 1<<32 - 2, 1<<13 - 2, 1},
		{"0 0 * * 5-7", 1, 1, 1<<32 - 2, 1<<13 - 2, 1 | 1<<5 | 1<<6},
		{"*/15 */6 */10 */3 */2", 1 | 1<<15 | 1<<30 | 1<<45, 1 | 1<<6 | 1<<12 | 1<<18, 1<<1 | 1<<11 | 1<<21 | 1<<31, 1<<1 | 1<<4 | 1<<7 | 1<<10, 1 | 1<<2 | 1<<4 | 1<<6},
	}
	for _, c := range cases {
		s := mustParse(t, c.expr).(*spec)
		got := [5]uint64{s.minute, s.hour, s.dom, s.month, s.dow}
		want := [5]uint64{c.minute, c.hour, c.dom, c.month, c.dow}
		if got != want {
			t.Errorf("%q bits = %#x, want %#x", c.expr, got, want)
		}
	}
}

func TestMacros(t *testing.T) {
	for macro, std := range macros {
		a := mustParse(t, macro).(*spec)
		b := mustParse(t, std).(*spec)
		a.expr, b.expr = "", ""
		if *a != *b {
			t.Errorf("%s != %s", macro, std)
		}
	}
	// Case-insensitive, and the original text is kept for round-tripping.
	if s := mustParse(t, " @Daily "); s.String() != "@Daily" {
		t.Errorf("String() = %q", s.String())
	}
}

func TestNextUTC(t *testing.T) {
	cases := []struct {
		name, expr  string
		after, want time.Time
	}{
		{"every minute", "* * * * *", utc(2026, 10, 10, 12, 0), utc(2026, 10, 10, 12, 1)},
		{"strictly after, mid-minute", "* * * * *", utc(2026, 10, 10, 12, 0).Add(30 * time.Second), utc(2026, 10, 10, 12, 1)},
		{"step", "*/15 * * * *", utc(2026, 10, 10, 12, 15), utc(2026, 10, 10, 12, 30)},
		{"range step", "1-10/3 * * * *", utc(2026, 10, 10, 12, 7), utc(2026, 10, 10, 12, 10)},
		{"range step wraps hour", "1-10/3 * * * *", utc(2026, 10, 10, 12, 10), utc(2026, 10, 10, 13, 1)},
		{"end of month", "0 0 * * *", utc(2026, 4, 30, 0, 0), utc(2026, 5, 1, 0, 0)},
		{"end of year", "59 23 31 12 *", utc(2026, 12, 31, 23, 59), utc(2027, 12, 31, 23, 59)},
		{"new year", "@yearly", utc(2026, 12, 31, 23, 59), utc(2027, 1, 1, 0, 0)},
		{"31st skips short months", "0 0 31 * *", utc(2026, 4, 1, 0, 0), utc(2026, 5, 31, 0, 0)},
		{"leap day", "0 0 29 2 *", utc(2026, 1, 1, 0, 0), utc(2028, 2, 29, 0, 0)},
		{"leap day skips 2100", "0 0 29 2 *", utc(2096, 3, 1, 0, 0), utc(2104, 2, 29, 0, 0)},
		{"weekdays", "0 9 * * MON-FRI", utc(2026, 10, 9, 10, 0), utc(2026, 10, 12, 9, 0)},
		{"sunday as 7", "0 0 * * 7", utc(2026, 10, 10, 0, 0), utc(2026, 10, 11, 0, 0)},
		{"sunday as 0", "0 0 * * 0", utc(2026, 10, 10, 0, 0), utc(2026, 10, 11, 0, 0)},
		{"weekly", "@weekly", utc(2026, 10, 10, 0, 0), utc(2026, 10, 11, 0, 0)},
		{"monthly", "@monthly", utc(2026, 10, 10, 0, 0), utc(2026, 11, 1, 0, 0)},
		{"hourly", "@hourly", utc(2026, 10, 10, 0, 0), utc(2026, 10, 10, 1, 0)},
		// Vixie rule: dom and dow both restricted -> either matches.
		{"OR: dom first", "0 0 13 * FRI", utc(2026, 10, 10, 0, 0), utc(2026, 10, 13, 0, 0)},
		{"OR: dow first", "0 0 13 * FRI", utc(2026, 10, 13, 0, 0), utc(2026, 10, 16, 0, 0)},
		{"OR: first of month or monday", "0 0 1 * MON", utc(2026, 10, 27, 0, 0), utc(2026, 11, 1, 0, 0)},
		// dom starts with "*" -> both must match: odd days that are Fridays.
		{"AND: star step dom", "0 0 */2 * FRI", utc(2026, 10, 10, 0, 0), utc(2026, 10, 23, 0, 0)},
		{"AND: star dow", "0 0 13 * *", utc(2026, 10, 13, 0, 0), utc(2026, 11, 13, 0, 0)},
		{"friday 13th via AND impossible in OR form", "0 0 13 * */1", utc(2026, 10, 10, 0, 0), utc(2026, 10, 13, 0, 0)},
	}
	for _, c := range cases {
		got := mustParse(t, c.expr).Next(c.after, time.UTC)
		if !got.Equal(c.want) {
			t.Errorf("%s: %q Next(%v) = %v, want %v", c.name, c.expr, c.after, got, c.want)
		}
	}
}

func TestNextNeverFires(t *testing.T) {
	for _, expr := range []string{"0 0 30 2 *", "0 0 31 4 *", "0 0 31 6,9,11 *"} {
		if got := mustParse(t, expr).Next(utc(2026, 1, 1, 0, 0), time.UTC); !got.IsZero() {
			t.Errorf("%q Next = %v, want zero", expr, got)
		}
	}
}

func TestNilLocationIsUTC(t *testing.T) {
	s := mustParse(t, "0 9 * * *")
	got := s.Next(utc(2026, 10, 10, 0, 0), nil)
	if !got.Equal(utc(2026, 10, 10, 9, 0)) || got.Location() != time.UTC {
		t.Errorf("Next(nil loc) = %v", got)
	}
}

func TestNextReturnsZone(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	got := mustParse(t, "0 9 * * *").Next(utc(2026, 10, 10, 0, 0), ny)
	if got.Location() != ny || got.Hour() != 9 || !got.Equal(utc(2026, 10, 10, 13, 0)) {
		t.Errorf("Next = %v", got)
	}
}

// TestDST pins the written DST rules on concrete 2026 transitions.
//
//	New York: 2026-03-08 02:00 EST -> 03:00 EDT (07:00Z); 2026-11-01 02:00 EDT -> 01:00 EST (06:00Z)
//	London:   2026-03-29 01:00 GMT -> 02:00 BST (01:00Z); 2026-10-25 02:00 BST -> 01:00 GMT (01:00Z)
//	Lord Howe (30-minute shift): 2026-04-05 02:00 +11 -> 01:30 +1030 (04-04 15:00Z); 2026-10-04 02:00 +1030 -> 02:30 +11 (10-03 15:30Z)
func TestDST(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	london := mustLoad(t, "Europe/London")
	lh := mustLoad(t, "Australia/Lord_Howe")
	cases := []struct {
		name, expr string
		loc        *time.Location
		after      time.Time
		want       []time.Time // successive Next results
	}{
		{"NY gap: fixed time fires once at gap end", "30 2 * * *", ny, utc(2026, 3, 7, 12, 0),
			[]time.Time{utc(2026, 3, 8, 7, 0), utc(2026, 3, 9, 6, 30)}},
		{"NY gap: whole gap hour collapses to one fire", "* 2 * * *", ny, utc(2026, 3, 8, 5, 0),
			[]time.Time{utc(2026, 3, 8, 7, 0), utc(2026, 3, 9, 6, 0)}},
		{"NY gap: gap time and 03:00 dedupe", "0,30 2,3 * * *", ny, utc(2026, 3, 8, 5, 0),
			[]time.Time{utc(2026, 3, 8, 7, 0), utc(2026, 3, 8, 7, 30), utc(2026, 3, 9, 6, 0)}},
		{"NY overlap: fixed time fires on first occurrence only", "30 1 * * *", ny, utc(2026, 10, 31, 12, 0),
			[]time.Time{utc(2026, 11, 1, 5, 30), utc(2026, 11, 2, 6, 30)}},
		{"NY overlap: reference between occurrences does not refire", "30 1 * * *", ny, utc(2026, 11, 1, 6, 0),
			[]time.Time{utc(2026, 11, 2, 6, 30)}},
		{"NY gap: hourly skips the missing hour", "0 * * * *", ny, utc(2026, 3, 8, 5, 30),
			[]time.Time{utc(2026, 3, 8, 6, 0), utc(2026, 3, 8, 7, 0), utc(2026, 3, 8, 8, 0)}},
		{"NY overlap: hourly fires both occurrences", "0 * * * *", ny, utc(2026, 11, 1, 4, 30),
			[]time.Time{utc(2026, 11, 1, 5, 0), utc(2026, 11, 1, 6, 0), utc(2026, 11, 1, 7, 0)}},
		{"NY overlap: half-hourly in real-time order", "0,30 * * * *", ny, utc(2026, 11, 1, 4, 45),
			[]time.Time{utc(2026, 11, 1, 5, 0), utc(2026, 11, 1, 5, 30), utc(2026, 11, 1, 6, 0), utc(2026, 11, 1, 6, 30), utc(2026, 11, 1, 7, 0)}},
		{"London gap: 01:30 fires at 02:00 BST", "30 1 * * *", london, utc(2026, 3, 28, 12, 0),
			[]time.Time{utc(2026, 3, 29, 1, 0), utc(2026, 3, 30, 0, 30)}},
		{"London overlap: 01:30 fires once, BST occurrence", "30 1 * * *", london, utc(2026, 10, 24, 12, 0),
			[]time.Time{utc(2026, 10, 25, 0, 30), utc(2026, 10, 26, 1, 30)}},
		{"Lord Howe gap: 02:15 fires at 02:30 +11", "15 2 * * *", lh, utc(2026, 10, 3, 0, 0),
			[]time.Time{utc(2026, 10, 3, 15, 30), utc(2026, 10, 4, 15, 15)}},
		{"Lord Howe gap: 02:45 exists", "45 2 * * *", lh, utc(2026, 10, 3, 0, 0),
			[]time.Time{utc(2026, 10, 3, 15, 45)}},
		{"Lord Howe overlap: 01:45 fires once at +11", "45 1 * * *", lh, utc(2026, 4, 4, 0, 0),
			[]time.Time{utc(2026, 4, 4, 14, 45), utc(2026, 4, 5, 15, 15)}},
		{"Lord Howe overlap: quarter-hourly fires both", "*/15 * * * *", lh, utc(2026, 4, 4, 14, 20),
			[]time.Time{utc(2026, 4, 4, 14, 30), utc(2026, 4, 4, 14, 45), utc(2026, 4, 4, 15, 0), utc(2026, 4, 4, 15, 15), utc(2026, 4, 4, 15, 30)}},
	}
	for _, c := range cases {
		s := mustParse(t, c.expr)
		at := c.after
		for i, want := range c.want {
			got := s.Next(at, c.loc)
			if !got.Equal(want) {
				t.Errorf("%s: step %d Next(%v) = %v (%v), want %v", c.name, i, at.UTC(), got.UTC(), got, want.In(c.loc))
				break
			}
			at = got
		}
	}
}

// TestDSTPeriodicIsEvenInRealTime checks that schedules matching every hour
// never skip or double a real instant across a whole year of transitions.
func TestDSTPeriodicIsEvenInRealTime(t *testing.T) {
	for _, zone := range []string{"America/New_York", "Europe/London", "Australia/Lord_Howe"} {
		loc := mustLoad(t, zone)
		for _, c := range []struct {
			expr string
			gap  time.Duration
		}{{"*/15 * * * *", 15 * time.Minute}, {"0,30 * * * *", 30 * time.Minute}} {
			for _, window := range [][2]time.Time{
				{utc(2026, 3, 7, 0, 0), utc(2026, 3, 10, 0, 0)},
				{utc(2026, 3, 27, 0, 0), utc(2026, 3, 30, 0, 0)},
				{utc(2026, 4, 3, 0, 0), utc(2026, 4, 6, 0, 0)},
				{utc(2026, 10, 2, 0, 0), utc(2026, 10, 5, 0, 0)},
				{utc(2026, 10, 24, 0, 0), utc(2026, 10, 27, 0, 0)},
				{utc(2026, 10, 31, 0, 0), utc(2026, 11, 3, 0, 0)},
			} {
				got := mustParse(t, c.expr).Between(window[0], window[1], loc, 1000)
				for i := 1; i < len(got); i++ {
					if d := got[i].Sub(got[i-1]); d != c.gap {
						t.Errorf("%s %q: %v -> %v is %v apart, want %v", zone, c.expr, got[i-1], got[i], d, c.gap)
					}
				}
			}
		}
	}
}

// TestDSTFixedOncePerDay checks that a daily job fires exactly once on every
// local date of a year, including both transition days.
func TestDSTFixedOncePerDay(t *testing.T) {
	for _, zone := range []string{"America/New_York", "Europe/London", "Australia/Lord_Howe"} {
		loc := mustLoad(t, zone)
		for _, expr := range []string{"30 2 * * *", "30 1 * * *", "45 1 * * *", "15 2 * * *"} {
			from := time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
			to := time.Date(2027, 1, 1, 0, 0, 0, 0, loc)
			got := mustParse(t, expr).Between(from, to, loc, 1000)
			if len(got) != 365 {
				t.Errorf("%s %q: %d fires in 2026, want 365", zone, expr, len(got))
			}
			seen := map[string]bool{}
			for _, f := range got {
				key := f.Format("2006-01-02")
				if seen[key] {
					t.Errorf("%s %q: two fires on %s", zone, expr, key)
				}
				seen[key] = true
			}
		}
	}
}

func TestEvery(t *testing.T) {
	ny := mustLoad(t, "America/New_York")
	s := mustParse(t, "@every 90s")
	if got := s.Next(time.Unix(0, 0), nil); !got.Equal(time.Unix(90, 0)) {
		t.Errorf("Next(epoch) = %v", got)
	}
	if got := s.Next(time.Unix(89, 999), nil); !got.Equal(time.Unix(90, 0)) {
		t.Errorf("Next(89.000000999) = %v", got)
	}
	if got := s.Next(time.Unix(-1, 500), nil); !got.Equal(time.Unix(0, 0)) {
		t.Errorf("Next(-0.9999995) = %v", got)
	}
	if got := s.Next(time.Unix(-91, 0), nil); !got.Equal(time.Unix(-90, 0)) {
		t.Errorf("Next(-91) = %v", got)
	}
	// An hourly interval stays 3600s apart across the NY spring-forward gap.
	h := mustParse(t, "@every 1h")
	got := h.Between(utc(2026, 3, 8, 4, 0), utc(2026, 3, 8, 10, 0), ny, 100)
	if len(got) != 6 {
		t.Fatalf("Between = %v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i].Sub(got[i-1]) != time.Hour {
			t.Errorf("interval %v -> %v", got[i-1], got[i])
		}
	}
	if got[0].Location() != ny {
		t.Errorf("zone = %v", got[0].Location())
	}
	if h.String() != "@every 1h" {
		t.Errorf("String() = %q", h.String())
	}
}

func TestBetween(t *testing.T) {
	s := mustParse(t, "0 * * * *")
	from, to := utc(2026, 10, 10, 0, 0), utc(2026, 10, 10, 3, 0)
	got := s.Between(from, to, time.UTC, 100)
	want := []time.Time{utc(2026, 10, 10, 1, 0), utc(2026, 10, 10, 2, 0), utc(2026, 10, 10, 3, 0)}
	if len(got) != len(want) {
		t.Fatalf("Between = %v, want %v (from exclusive, to inclusive)", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Errorf("Between[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	if got := s.Between(from, to, time.UTC, 2); len(got) != 2 {
		t.Errorf("limit 2 returned %d", len(got))
	}
	if got := s.Between(from, to, time.UTC, 0); got != nil {
		t.Errorf("limit 0 returned %v", got)
	}
	if got := mustParse(t, "0 0 30 2 *").Between(from, to.AddDate(5, 0, 0), time.UTC, 10); got != nil {
		t.Errorf("never-firing Between = %v", got)
	}
}

func TestStringRoundTrip(t *testing.T) {
	for _, expr := range []string{"*/15 9-17 * JAN-MAR MON-FRI", "@hourly", "@every 2m30s", "0 0 13 * FRI"} {
		a := mustParse(t, expr)
		b := mustParse(t, a.String())
		from := utc(2026, 10, 10, 0, 0)
		if !a.Next(from, time.UTC).Equal(b.Next(from, time.UTC)) {
			t.Errorf("%q does not round-trip", expr)
		}
	}
}
