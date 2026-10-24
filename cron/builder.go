package cron

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ErrBuilder is wrapped by every error a Builder returns, so a caller can
// tell a bad frequency call from a parse error of a hand-written
// expression.
var ErrBuilder = errors.New("cron: invalid builder")

// leapYear is any leap year: YearlyOn measures a month's length in it, so
// February accepts the 29th (the cron then fires only in leap years).
const leapYear = 2024

// Builder writes a cron expression from named frequencies (Daily,
// EveryMinutes(5), Weekdays, At("13:00")). It emits the same expression a person would write, never a
// new format, so every driver keeps parsing one syntax and the result can
// be stored, logged and read back as text. Most frequencies emit five
// plain fields, which every driver takes (Hatchet refuses macros);
// EverySeconds emits "@every <n>s", which the memory and SQL drivers take
// and Hatchet refuses.
//
// A Builder is an immutable value: each method returns a new one, so a
// base frequency can be shared and refined. Invalid input does not panic
// and does not produce a wrong expression: the first error is kept and
// returned by Expr, wrapping ErrBuilder. The zero Builder is invalid; start
// from one of the frequency functions.
//
// The frequencies and their expressions:
//
//	EverySeconds(n)               @every <n>s       (n divides 60)
//	EveryMinute()                 * * * * *
//	EveryMinutes(5)               */5 * * * *       (n divides 60)
//	Hourly()                      0 * * * *
//	HourlyAt(17)                  17 * * * *
//	EveryHours(2)                 0 */2 * * *       (n is 2, 3, 4, 6, 8 or 12)
//	Daily()                       0 0 * * *
//	DailyAt("13:00")              0 13 * * *
//	TwiceDaily(1, 13)             0 1,13 * * *
//	TwiceDailyAt(1, 13, 15)       15 1,13 * * *
//	Weekly()                      0 0 * * 0
//	WeeklyOn(1, "8:00")           0 8 * * 1
//	Monthly()                     0 0 1 * *
//	MonthlyOn(4, "15:00")         0 15 4 * *
//	TwiceMonthly(1, 16, "13:00")  0 13 1,16 * *
//	Quarterly()                   0 0 1 1,4,7,10 *
//	QuarterlyOn(4, "14:00")       0 14 4 1,4,7,10 *
//	Yearly()                      0 0 1 1 *
//	YearlyOn(6, 1, "17:00")       0 17 1 6 *
//
// and the constraints:
//
//	.At("13:00")             sets minute and hour
//	.Weekdays()              day of week 1-5
//	.Weekends()              day of week 0,6
//	.Days(time.Monday, ...)  day of week list
//
// The last day of the month has no five-field form and is not offered;
// time windows and conditions belong to a schedule, not to the expression.
type Builder struct {
	minute, hour, dom, month, dow string
	// every is the "@every" interval; when set, the fields are unused and
	// no constraint applies.
	every string
	// fixedTime marks a frequency that fires at one time of day (daily or
	// coarser), the only ones At may change: on an hourly or minute
	// frequency At would silently drop the repetition.
	fixedTime bool
	err       error
}

// wild is the cron wildcard; quarterDays are the months a quarter starts
// in.
const (
	wild        = "*"
	quarterDays = "1,4,7,10"
)

// EverySeconds fires every n seconds as "@every <n>s". n must divide 60 so the ticks
// fall at the same seconds of every minute; for another interval write
// "@every 90s" by hand. Constraints (At, Days) are refused: an interval
// ignores the wall clock.
func EverySeconds(n int) Builder {
	if n < 1 || n > 59 || 60%n != 0 {
		return failed("EverySeconds(%d): n must divide 60 (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)", n)
	}
	return Builder{every: fmt.Sprintf("@every %ds", n)}
}

// EveryMinute fires at the start of every minute: "* * * * *".
func EveryMinute() Builder { return builderFields(wild, wild, wild, wild, wild, false) }

// EveryMinutes fires every n minutes from minute 0 of each hour:
// "*/n * * * *". n must divide 60, because "*/7" fires at :56 and again
// at :00, four minutes later; for an uneven step write the expression by
// hand. EveryMinutes(1) is EveryMinute.
func EveryMinutes(n int) Builder {
	if n < 1 || n > 59 || 60%n != 0 {
		return failed("EveryMinutes(%d): n must divide 60 (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)", n)
	}
	if n == 1 {
		return EveryMinute()
	}
	return builderFields("*/"+strconv.Itoa(n), wild, wild, wild, wild, false)
}

// Hourly fires at minute 0 of every hour: "0 * * * *".
func Hourly() Builder { return HourlyAt(0) }

// HourlyAt fires at the given minute (0-59) of every hour.
func HourlyAt(minute int) Builder {
	if minute < 0 || minute > 59 {
		return failed("HourlyAt(%d): minute must be 0-59", minute)
	}
	return builderFields(strconv.Itoa(minute), wild, wild, wild, wild, false)
}

// EveryHours fires at minute 0 every n hours from midnight: "0 */n * * *".
// n must divide 24 so the step is even across midnight.
func EveryHours(n int) Builder {
	if n < 1 || n > 23 || 24%n != 0 {
		return failed("EveryHours(%d): n must divide 24 (1, 2, 3, 4, 6, 8, 12)", n)
	}
	if n == 1 {
		return Hourly()
	}
	return builderFields("0", "*/"+strconv.Itoa(n), wild, wild, wild, false)
}

// Daily fires at midnight: "0 0 * * *".
func Daily() Builder { return builderFields("0", "0", wild, wild, wild, true) }

// DailyAt fires every day at hhmm ("13:00", "9:05").
func DailyAt(hhmm string) Builder { return Daily().At(hhmm) }

// TwiceDaily fires at minute 0 of two hours (0-23) every day.
func TwiceDaily(first, second int) Builder { return TwiceDailyAt(first, second, 0) }

// TwiceDailyAt fires at the given minute of two hours every day.
func TwiceDailyAt(first, second, minute int) Builder {
	switch {
	case first < 0 || first > 23 || second < 0 || second > 23:
		return failed("TwiceDailyAt(%d, %d, %d): hours must be 0-23", first, second, minute)
	case first == second:
		return failed("TwiceDailyAt(%d, %d, %d): the two hours are the same", first, second, minute)
	case minute < 0 || minute > 59:
		return failed("TwiceDailyAt(%d, %d, %d): minute must be 0-59", first, second, minute)
	}
	return builderFields(strconv.Itoa(minute), joinInts([]int{first, second}), wild, wild, wild, false)
}

// Weekly fires at midnight on Sunday: "0 0 * * 0". Days changes the day.
func Weekly() Builder { return builderFields("0", "0", wild, wild, "0", true) }

// WeeklyOn fires once a week on day at hhmm.
func WeeklyOn(day time.Weekday, hhmm string) Builder { return Weekly().Days(day).At(hhmm) }

// Monthly fires at midnight on the 1st: "0 0 1 * *".
func Monthly() Builder { return builderFields("0", "0", "1", wild, wild, true) }

// MonthlyOn fires on day (1-31) of every month at hhmm. Months without
// that day are skipped, as in cron: MonthlyOn(31, ...) fires seven times a
// year.
func MonthlyOn(day int, hhmm string) Builder {
	if day < 1 || day > 31 {
		return failed("MonthlyOn(%d): day must be 1-31", day)
	}
	return builderFields("0", "0", strconv.Itoa(day), wild, wild, true).At(hhmm)
}

// TwiceMonthly fires on two days of every month at hhmm.
func TwiceMonthly(first, second int, hhmm string) Builder {
	switch {
	case first < 1 || first > 31 || second < 1 || second > 31:
		return failed("TwiceMonthly(%d, %d): days must be 1-31", first, second)
	case first == second:
		return failed("TwiceMonthly(%d, %d): the two days are the same", first, second)
	}
	return builderFields("0", "0", joinInts([]int{first, second}), wild, wild, true).At(hhmm)
}

// Quarterly fires at midnight on the 1st of January, April, July and
// October: "0 0 1 1,4,7,10 *".
func Quarterly() Builder { return builderFields("0", "0", "1", quarterDays, wild, true) }

// QuarterlyOn fires on day (1-31) of the first month of each quarter at
// hhmm.
func QuarterlyOn(day int, hhmm string) Builder {
	if day < 1 || day > 31 {
		return failed("QuarterlyOn(%d): day must be 1-31", day)
	}
	return builderFields("0", "0", strconv.Itoa(day), quarterDays, wild, true).At(hhmm)
}

// Yearly fires at midnight on 1 January: "0 0 1 1 *".
func Yearly() Builder { return builderFields("0", "0", "1", "1", wild, true) }

// YearlyOn fires once a year on month and day at hhmm. A day the month
// never has (30 February) is refused rather than producing a schedule
// that never fires; 29 February is accepted and fires in leap years only.
func YearlyOn(month time.Month, day int, hhmm string) Builder {
	if month < time.January || month > time.December {
		return failed("YearlyOn(%d, %d): month must be 1-12", month, day)
	}
	if last := time.Date(leapYear, month+1, 0, 0, 0, 0, 0, time.UTC).Day(); day < 1 || day > last {
		return failed("YearlyOn(%s, %d): day must be 1-%d", month, day, last)
	}
	return builderFields("0", "0", strconv.Itoa(day), strconv.Itoa(int(month)), wild, true).At(hhmm)
}

// At sets the time of day, hhmm as "13:00" or "9:05" (24-hour clock). It
// applies to frequencies that fire once on a day (Daily, Weekly, Monthly,
// Quarterly, Yearly and their On forms); on an hourly, minute or second
// frequency it is refused, because replacing the minute and hour would
// silently drop the repetition: use DailyAt, HourlyAt or TwiceDailyAt.
func (b Builder) At(hhmm string) Builder {
	if b.err != nil {
		return b
	}
	if !b.fixedTime {
		return b.fail("At(%q): only a daily or coarser frequency has a time of day to set", hhmm)
	}
	h, m, err := parseHHMM(hhmm)
	if err != nil {
		return b.fail("At(%q): %v", hhmm, err)
	}
	b.hour, b.minute = strconv.Itoa(h), strconv.Itoa(m)
	return b
}

// Weekdays limits the frequency to Monday to Friday.
func (b Builder) Weekdays() Builder { return b.withDow("Weekdays()", "1-5") }

// Weekends limits the frequency to Saturday and Sunday.
func (b Builder) Weekends() Builder { return b.withDow("Weekends()", "0,6") }

// Days limits the frequency to the given weekdays. On Weekly it picks the day the week fires on.
// It is refused on a frequency that also fixes the day of the month
// (Monthly, Quarterly, Yearly and their On forms): cron fires when EITHER
// day field matches there (the Vixie rule), so Monthly().Days(Monday)
// would fire on the 1st and on every Monday, not on Mondays that are the
// 1st.
func (b Builder) Days(days ...time.Weekday) Builder {
	if len(days) == 0 {
		return b.fail("Days(): name at least one day")
	}
	nums := make([]int, 0, len(days))
	for _, d := range days {
		if d < time.Sunday || d > time.Saturday {
			return b.fail("Days(%d): not a weekday", d)
		}
		nums = append(nums, int(d))
	}
	return b.withDow("Days()", joinInts(nums))
}

func (b Builder) withDow(call, dow string) Builder {
	switch {
	case b.err != nil:
		return b
	case b.every != "":
		return b.fail("%s: an @every interval ignores the calendar; use a five-field frequency", call)
	case b.dom != wild:
		return b.fail("%s: the frequency also fixes the day of the month, and cron would fire when either day matches", call)
	}
	b.dow = dow
	return b
}

// Expr returns the expression, checked by Parse, or the first error a
// call made, wrapping ErrBuilder.
func (b Builder) Expr() (string, error) {
	if b.err != nil {
		return "", b.err
	}
	expr := b.every
	if expr == "" {
		if b.minute == "" {
			return "", fmt.Errorf("%w: the zero Builder has no frequency; start from Daily(), EveryMinutes(n) or another frequency function", ErrBuilder)
		}
		expr = strings.Join([]string{b.minute, b.hour, b.dom, b.month, b.dow}, " ")
	}
	if _, err := Parse(expr); err != nil {
		return "", fmt.Errorf("%w: built %q: %w", ErrBuilder, expr, err)
	}
	return expr, nil
}

// MustExpr is Expr for a frequency written as constants in the program
// (Workflow.Cron, a package-level schedule), where an error is a bug: it
// panics with Expr's error. Use Expr for input that comes from users or
// configuration.
func (b Builder) MustExpr() string {
	expr, err := b.Expr()
	if err != nil {
		panic(err)
	}
	return expr
}

// Schedule parses the expression, for computing fire times (Next,
// Between) without a driver.
func (b Builder) Schedule() (Schedule, error) {
	expr, err := b.Expr()
	if err != nil {
		return nil, err
	}
	return Parse(expr)
}

func builderFields(minute, hour, dom, month, dow string, fixedTime bool) Builder {
	return Builder{minute: minute, hour: hour, dom: dom, month: month, dow: dow, fixedTime: fixedTime}
}

func failed(format string, args ...any) Builder { return Builder{}.fail(format, args...) }

// fail keeps the first error: later calls on a failed Builder change
// nothing, so the message names the call that went wrong.
func (b Builder) fail(format string, args ...any) Builder {
	if b.err == nil {
		b.err = fmt.Errorf("%w: %s", ErrBuilder, fmt.Sprintf(format, args...))
	}
	return b
}

// parseHHMM reads "13:00" or "9:05": an hour of one or two digits (0-23)
// and a minute of exactly two (00-59).
func parseHHMM(s string) (hour, minute int, err error) {
	hs, ms, ok := strings.Cut(s, ":")
	if !ok || len(hs) < 1 || len(hs) > 2 || len(ms) != 2 {
		return 0, 0, errors.New(`want "HH:MM" on a 24-hour clock, e.g. "13:00" or "9:05"`)
	}
	if !allDigits(hs) || !allDigits(ms) {
		return 0, 0, errors.New(`want digits only, e.g. "13:00" or "9:05"`)
	}
	h, herr := strconv.Atoi(hs)
	m, merr := strconv.Atoi(ms)
	switch {
	case herr != nil || h < 0 || h > 23:
		return 0, 0, fmt.Errorf("hour %q must be 0-23", hs)
	case merr != nil || m < 0 || m > 59:
		return 0, 0, fmt.Errorf("minute %q must be 00-59", ms)
	}
	return h, m, nil
}

// joinInts sorts and de-duplicates nums and joins them with commas, so the
// same set always gives the same expression.
func joinInts(nums []int) string {
	nums = slices.Clone(nums)
	slices.Sort(nums)
	nums = slices.Compact(nums)
	parts := make([]string, len(nums))
	for i, n := range nums {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
