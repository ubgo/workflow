package cron

import (
	"errors"
	"testing"
	"time"
)

func TestCalendarExpr(t *testing.T) {
	cases := []struct {
		cal  Calendar
		want string
	}{
		{Calendar{}, "0 * * * *"},
		{Calendar{Hour: []int{9}}, "0 9 * * *"},
		{Calendar{Minute: []int{30, 0}, Hour: []int{9}, DayOfWeek: []time.Weekday{time.Friday, time.Monday, time.Tuesday, time.Wednesday, time.Thursday}}, "0,30 9 * * 1-5"},
		{Calendar{Hour: []int{1, 2, 3, 7, 7}, DayOfMonth: []int{1, 15}}, "0 1-3,7 1,15 * *"},
		{Calendar{Hour: []int{0}, DayOfMonth: []int{1}, Month: []time.Month{time.January, time.April, time.July, time.October}}, "0 0 1 1,4,7,10 *"},
		{Calendar{DayOfWeek: []time.Weekday{time.Sunday, time.Saturday}}, "0 * * * 0,6"},
	}
	for _, c := range cases {
		got, err := c.cal.Expr()
		if err != nil {
			t.Fatalf("%+v: %v", c.cal, err)
		}
		if got != c.want {
			t.Errorf("%+v: Expr = %q, want %q", c.cal, got, c.want)
		}
		if _, err := c.cal.Schedule(); err != nil {
			t.Errorf("%+v: Schedule: %v", c.cal, err)
		}
	}
}

func TestCalendarErrors(t *testing.T) {
	bad := []Calendar{
		{Minute: []int{60}},
		{Hour: []int{-1}},
		{DayOfMonth: []int{0}},
		{Month: []time.Month{13}},
		{DayOfWeek: []time.Weekday{7}},
		{DayOfMonth: []int{13}, DayOfWeek: []time.Weekday{time.Friday}},
	}
	for _, c := range bad {
		if _, err := c.Expr(); !errors.Is(err, ErrCalendar) {
			t.Errorf("%+v: err = %v, want ErrCalendar", c, err)
		}
	}
}

// TestCalendarFires checks the compiled schedule fires where the calendar
// says: 09:00 and 09:30 on weekdays only.
func TestCalendarFires(t *testing.T) {
	s, err := Calendar{Minute: []int{0, 30}, Hour: []int{9}, DayOfWeek: []time.Weekday{1, 2, 3, 4, 5}}.Schedule()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Between(utc(2026, 1, 2, 0, 0), utc(2026, 1, 6, 0, 0), nil, 10) // Fri 2nd to Tue 6th
	want := []time.Time{utc(2026, 1, 2, 9, 0), utc(2026, 1, 2, 9, 30), utc(2026, 1, 5, 9, 0), utc(2026, 1, 5, 9, 30)}
	if len(got) != len(want) {
		t.Fatalf("Between = %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("Between[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
