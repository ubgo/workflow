package cron_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow/cron"
)

// TestBuilder_Frequencies pins every frequency to the expression a
// person would write for it, and proves each one parses.
func TestBuilder_Frequencies(t *testing.T) {
	cases := []struct {
		b    cron.Builder
		want string
	}{
		{cron.EverySeconds(1), "@every 1s"},
		{cron.EverySeconds(30), "@every 30s"},
		{cron.EveryMinute(), "* * * * *"},
		{cron.EveryMinutes(1), "* * * * *"},
		{cron.EveryMinutes(5), "*/5 * * * *"},
		{cron.EveryMinutes(30), "*/30 * * * *"},
		{cron.Hourly(), "0 * * * *"},
		{cron.HourlyAt(17), "17 * * * *"},
		{cron.EveryHours(1), "0 * * * *"},
		{cron.EveryHours(2), "0 */2 * * *"},
		{cron.EveryHours(12), "0 */12 * * *"},
		{cron.Daily(), "0 0 * * *"},
		{cron.DailyAt("13:00"), "0 13 * * *"},
		{cron.DailyAt("9:05"), "5 9 * * *"},
		{cron.TwiceDaily(13, 1), "0 1,13 * * *"},
		{cron.TwiceDailyAt(1, 13, 15), "15 1,13 * * *"},
		{cron.Weekly(), "0 0 * * 0"},
		{cron.WeeklyOn(time.Monday, "8:00"), "0 8 * * 1"},
		{cron.Monthly(), "0 0 1 * *"},
		{cron.MonthlyOn(4, "15:00"), "0 15 4 * *"},
		{cron.TwiceMonthly(16, 1, "13:00"), "0 13 1,16 * *"},
		{cron.Quarterly(), "0 0 1 1,4,7,10 *"},
		{cron.QuarterlyOn(4, "14:00"), "0 14 4 1,4,7,10 *"},
		{cron.Yearly(), "0 0 1 1 *"},
		{cron.YearlyOn(time.June, 1, "17:00"), "0 17 1 6 *"},
		{cron.YearlyOn(time.February, 29, "0:00"), "0 0 29 2 *"},
		{cron.Daily().At("23:59"), "59 23 * * *"},
		{cron.EveryMinutes(15).Weekdays(), "*/15 * * * 1-5"},
		{cron.DailyAt("9:00").Weekends(), "0 9 * * 0,6"},
		{cron.Hourly().Days(time.Friday, time.Monday, time.Monday), "0 * * * 1,5"},
		{cron.Weekly().Days(time.Saturday).At("6:30"), "30 6 * * 6"},
	}
	for _, tc := range cases {
		got, err := tc.b.Expr()
		if err != nil || got != tc.want {
			t.Errorf("Expr = %q, %v; want %q", got, err, tc.want)
			continue
		}
		if _, err := cron.Parse(got); err != nil {
			t.Errorf("%q does not parse: %v", got, err)
		}
	}
}

// TestBuilder_Refusals covers every call that would otherwise write a
// wrong or surprising expression.
func TestBuilder_Refusals(t *testing.T) {
	cases := []struct {
		name string
		b    cron.Builder
		want string
	}{
		{"zero builder", cron.Builder{}, "zero Builder"},
		{"seconds not dividing 60", cron.EverySeconds(7), "EverySeconds(7)"},
		{"seconds zero", cron.EverySeconds(0), "EverySeconds(0)"},
		{"minutes not dividing 60", cron.EveryMinutes(7), "EveryMinutes(7)"},
		{"minutes 60", cron.EveryMinutes(60), "EveryMinutes(60)"},
		{"hours not dividing 24", cron.EveryHours(5), "EveryHours(5)"},
		{"hourly minute", cron.HourlyAt(60), "HourlyAt(60)"},
		{"twice daily same hour", cron.TwiceDaily(3, 3), "same"},
		{"twice daily hour", cron.TwiceDaily(3, 24), "0-23"},
		{"twice daily minute", cron.TwiceDailyAt(1, 2, 60), "minute"},
		{"monthly day", cron.MonthlyOn(32, "1:00"), "MonthlyOn(32)"},
		{"twice monthly same day", cron.TwiceMonthly(5, 5, "1:00"), "same"},
		{"quarterly day", cron.QuarterlyOn(0, "1:00"), "QuarterlyOn(0)"},
		{"30 February", cron.YearlyOn(time.February, 30, "1:00"), "1-29"},
		{"month 13", cron.YearlyOn(13, 1, "1:00"), "month"},
		{"At on minutes", cron.EveryMinutes(5).At("13:00"), "daily or coarser"},
		{"At on hourly", cron.Hourly().At("13:00"), "daily or coarser"},
		{"At on twice daily", cron.TwiceDaily(1, 13).At("13:00"), "daily or coarser"},
		{"At on seconds", cron.EverySeconds(5).At("13:00"), "daily or coarser"},
		{"At hour 24", cron.DailyAt("24:00"), "hour"},
		{"At minute 60", cron.DailyAt("9:60"), "minute"},
		{"At one-digit minute", cron.DailyAt("9:5"), "HH:MM"},
		{"At no colon", cron.DailyAt("0900"), "HH:MM"},
		{"At signed", cron.DailyAt("+9:05"), "digits"},
		{"days on seconds", cron.EverySeconds(5).Weekdays(), "@every"},
		{"days on monthly", cron.Monthly().Days(time.Monday), "day of the month"},
		{"weekdays on yearly", cron.Yearly().Weekdays(), "day of the month"},
		{"no days", cron.Daily().Days(), "at least one"},
		{"bad weekday", cron.Daily().Days(7), "not a weekday"},
	}
	for _, tc := range cases {
		got, err := tc.b.Expr()
		if !errors.Is(err, cron.ErrBuilder) || !strings.Contains(err.Error(), tc.want) || got != "" {
			t.Errorf("%s: Expr = %q, %v; want ErrBuilder naming %q", tc.name, got, err, tc.want)
		}
	}
}

func TestBuilder_FirstErrorWinsAndValuesAreImmutable(t *testing.T) {
	_, err := cron.EveryMinutes(7).At("bad").Days().Expr()
	if err == nil || !strings.Contains(err.Error(), "EveryMinutes(7)") {
		t.Fatalf("err = %v, want the first call named", err)
	}
	base := cron.Daily()
	nine := base.At("9:00")
	if got := base.MustExpr(); got != "0 0 * * *" {
		t.Fatalf("At changed the base: %q", got)
	}
	if got := nine.Weekdays().MustExpr(); got != "0 9 * * 1-5" || nine.MustExpr() != "0 9 * * *" {
		t.Fatalf("Weekdays changed its receiver: %q / %q", got, nine.MustExpr())
	}
}

func TestBuilder_MustExprPanicsOnError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustExpr did not panic on an invalid builder")
		} else if err, ok := r.(error); !ok || !errors.Is(err, cron.ErrBuilder) {
			t.Fatalf("panic value = %v", r)
		}
	}()
	_ = cron.EveryMinutes(7).MustExpr()
}

// TestBuilder_ScheduleFires checks the built expressions fire when the
// names say, in an explicit zone.
func TestBuilder_ScheduleFires(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	friday := time.Date(2026, 1, 9, 10, 0, 0, 0, ny) // a Friday, 10:00
	s, err := cron.DailyAt("9:00").Weekdays().Schedule()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := s.Next(friday, ny), time.Date(2026, 1, 12, 9, 0, 0, 0, ny); !got.Equal(want) {
		t.Fatalf("weekdays 09:00 after Friday 10:00 = %v, want Monday %v", got, want)
	}
	s, err = cron.EverySeconds(15).Schedule()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := s.Next(friday, ny), friday.Add(15*time.Second); !got.Equal(want) {
		t.Fatalf("every 15s = %v, want %v", got, want)
	}
	if _, err := cron.EveryMinutes(7).Schedule(); !errors.Is(err, cron.ErrBuilder) {
		t.Fatalf("Schedule on an invalid builder: %v", err)
	}
}
