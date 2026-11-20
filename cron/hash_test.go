package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestHashShapes(t *testing.T) {
	cases := []struct {
		expr  string
		check func(fields []string) bool
	}{
		{"H * * * *", func(f []string) bool { return inRange(f[0], 0, 59) && f[1] == "*" }},
		{"H H * * *", func(f []string) bool { return inRange(f[0], 0, 59) && inRange(f[1], 0, 23) }},
		{"H(0-29) 9 * * *", func(f []string) bool { return inRange(f[0], 0, 29) }},
		{"0 0 H * *", func(f []string) bool { return inRange(f[2], 1, 28) }},
		{"0 0 * H *", func(f []string) bool { return inRange(f[3], 1, 12) }},
		{"0 0 * * H", func(f []string) bool { return inRange(f[4], 0, 6) }},
		{"0 0 * * H(MON-FRI)", func(f []string) bool { return inRange(f[4], 1, 5) }},
		{"0 0 * * THU", func(f []string) bool { return f[4] == "THU" }},
		{"H/15 * * * *", func(f []string) bool { return stepFrom(f[0], 0, 14, "59/15") }},
		{"H(10-40)/5 * * * *", func(f []string) bool { return stepFrom(f[0], 10, 14, "40/5") }},
		{"H/100 * * * *", func(f []string) bool { return stepFrom(f[0], 0, 59, "59/100") }},
		{"0,H * * * *", func(f []string) bool { return strings.HasPrefix(f[0], "0,") && inRange(f[0][2:], 0, 59) }},
	}
	for _, c := range cases {
		for _, seed := range []string{"digest", "billing", "report-42"} {
			got, err := Hash(c.expr, seed)
			if err != nil {
				t.Fatalf("Hash(%q, %q): %v", c.expr, seed, err)
			}
			if _, err := Parse(got); err != nil {
				t.Fatalf("Hash(%q, %q) = %q does not parse: %v", c.expr, seed, got, err)
			}
			if !c.check(strings.Fields(got)) {
				t.Errorf("Hash(%q, %q) = %q has the wrong shape", c.expr, seed, got)
			}
			if again, _ := Hash(c.expr, seed); again != got {
				t.Errorf("Hash(%q, %q) is not stable: %q then %q", c.expr, seed, got, again)
			}
		}
	}
	if got, _ := Hash("  @daily ", "x"); got != "@daily" {
		t.Errorf("a macro came back as %q", got)
	}
	if got, _ := Hash(" 5 4 * * * ", "x"); got != "5 4 * * *" {
		t.Errorf("an unhashed expression came back as %q", got)
	}
}

func inRange(text string, lo, hi int) bool {
	v, err := strconv.Atoi(text)
	return err == nil && v >= lo && v <= hi
}

// stepFrom checks "start-rest" with start in [lo, hi].
func stepFrom(text string, lo, hi int, rest string) bool {
	start, tail, ok := strings.Cut(text, "-")
	return ok && tail == rest && inRange(start, lo, hi)
}

// TestHashSpread: many names hashed onto the minute field land all over
// the hour, not on a handful of minutes, and the two fields of one seed
// move independently.
func TestHashSpread(t *testing.T) {
	const names = 6000
	minutes := map[string]int{}
	same := 0
	for i := range names {
		got, err := Hash("H H * * *", fmt.Sprintf("tenant-%d/digest", i))
		if err != nil {
			t.Fatal(err)
		}
		f := strings.Fields(got)
		minutes[f[0]]++
		if f[0] == f[1] {
			same++
		}
	}
	if len(minutes) != 60 {
		t.Fatalf("%d names used %d of 60 minutes", names, len(minutes))
	}
	for m, n := range minutes {
		// 100 expected per minute; a fair hash stays well inside 50-150.
		if n < 50 || n > 150 {
			t.Errorf("minute %s holds %d of %d names", m, n, names)
		}
	}
	if same > names/10 {
		t.Errorf("minute equals hour for %d of %d names: the fields are not independent", same, names)
	}
}

func TestHashErrors(t *testing.T) {
	cases := []struct{ expr, field string }{
		{"", "expression"},
		{"H * * *", "expression"},
		{"H(0-60) * * * *", "minute"},
		{"H(30-10) * * * *", "minute"},
		{"H(5) * * * *", "minute"},
		{"H(1-5 * * * *", "minute"},
		{"H/0 * * * *", "minute"},
		{"HX * * * *", "minute"},
		{"0 H(0-24) * * *", "hour"},
		{"0 0 H(0-5) * *", "day-of-month"},
		{"0 0 * H/x *", "month"},
		{"0 0 * * H(SUN-FOO)", "day-of-week"},
		{"60 H * * *", "minute"},
		{"@nope", "macro"},
	}
	for _, c := range cases {
		_, err := Hash(c.expr, "seed")
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Errorf("Hash(%q) = %v, want *ParseError", c.expr, err)
			continue
		}
		if pe.Field != c.field || pe.Expr != c.expr {
			t.Errorf("Hash(%q) field %q expr %q, want %q (%v)", c.expr, pe.Field, pe.Expr, c.field, err)
		}
	}
}
