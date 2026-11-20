package cron

import (
	"errors"
	"testing"
)

func TestDescribe(t *testing.T) {
	cases := []struct{ expr, want string }{
		{"0 9 * * MON-FRI", "At 09:00, Monday through Friday"},
		{"*/15 * * * *", "Every 15 minutes"},
		{"5 * * * *", "At minute 5 past every hour"},
		{"0 * * * *", "Every hour"},
		{"0 */2 * * *", "Every 2 hours"},
		{"* * * * *", "Every minute"},
		{"0,30 9,17 * * *", "At 09:00, 09:30, 17:00 and 17:30"},
		{"0 9-17 * * *", "At minute 0 past every hour, between 09:00 and 17:59"},
		{"*/10 9-17 * * 1-5", "Every 10 minutes, between 09:00 and 17:59, Monday through Friday"},
		{"0 1,2,3,4,5,6,7,8,9 * * *", "At minute 0 past hours 01, 02, 03, 04, 05, 06, 07, 08 and 09"},
		{"0 8-18/2 * * *", "At minute 0, every 2 hours between 08:00 and 18:59"},
		{"*/5 */3 * * *", "Every 5 minutes, during every 3rd hour"},
		{"0-10 * * * *", "Every minute from 0 through 10"},
		{"5/20 * * * *", "Every 20 minutes from minute 5 through 59"},
		{"0,15,45 * * * *", "At minutes 0, 15 and 45 past every hour"},
		{"0 0 1 * MON", "At 00:00, on day 1 of the month or on Monday"},
		{"0 0 * * 0", "At 00:00, only on Sunday"},
		{"0 0 * * 7", "At 00:00, only on Sunday"},
		{"0 0 * * 5-7", "At 00:00, Friday through Sunday"},
		{"15 10 * * 6,0", "At 10:15, only on Saturday and Sunday"},
		{"0 9 * * 1,3,5", "At 09:00, only on Monday, Wednesday and Friday"},
		{"0 0 1,15 * *", "At 00:00, on days 1 and 15 of the month"},
		{"0 0 1-7 * *", "At 00:00, on days 1 through 7 of the month"},
		{"0 0 */2 * *", "At 00:00, every 2nd day of the month"},
		{"0 0 1 1 *", "At 00:00, on day 1 of the month, only in January"},
		{"0 0 1 */3 *", "At 00:00, on day 1 of the month, every 3 months"},
		{"0 0 * 1,4,7,10 *", "At 00:00, only in January, April, July and October"},
		{"0 0 * 3-5 *", "At 00:00, March through May"},
		{"0 0 L * *", "At 00:00, on the last day of the month"},
		{"0 0 L-2 * *", "At 00:00, 2 days before the last day of the month"},
		{"0 0 15W * *", "At 00:00, on the weekday nearest day 15 of the month"},
		{"0 0 LW * *", "At 00:00, on the last weekday of the month"},
		{"0 0 1,L * *", "At 00:00, on day 1 of the month and on the last day of the month"},
		{"0 9 * * MON#1", "At 09:00, on the first Monday of the month"},
		{"0 9 * * FRIL", "At 09:00, on the last Friday of the month"},
		{"0 0 L * FRI", "At 00:00, on the last day of the month or on Friday"},
		{"@daily", "At 00:00"},
		{"@hourly", "Every hour"},
		{"@weekly", "At 00:00, only on Sunday"},
		{"@monthly", "At 00:00, on day 1 of the month"},
		{"@yearly", "At 00:00, on day 1 of the month, only in January"},
		{"@every 1h30m", "Every 1 hour and 30 minutes"},
		{"@every 90s", "Every 1 minute and 30 seconds"},
		{"@every 2h", "Every 2 hours"},
		{"  0 9 * * *  ", "At 09:00"},
	}
	for _, c := range cases {
		got, err := Describe(c.expr)
		if err != nil || got != c.want {
			t.Errorf("Describe(%q) = %q, %v; want %q", c.expr, got, err, c.want)
		}
	}
}

func TestDescribeSeconds(t *testing.T) {
	cases := []struct{ expr, want string }{
		{"*/10 * * * * *", "Every 10 seconds"},
		{"30 0 9 * * MON-FRI", "At 09:00:30, Monday through Friday"},
		{"0 */5 * * * *", "Every 5 minutes"},
		{"30 */5 * * * *", "At second 30, every 5 minutes"},
		{"*/10 0 9 * * *", "Every 10 seconds, during 09:00"},
		{"0 0 9 * * *", "At 09:00"},
		{"15,45 * * * * *", "At seconds 15 and 45"},
		{"@every 10s", "Every 10 seconds"},
		{"@daily", "At 00:00"},
	}
	for _, c := range cases {
		got, err := DescribeSeconds(c.expr)
		if err != nil || got != c.want {
			t.Errorf("DescribeSeconds(%q) = %q, %v; want %q", c.expr, got, err, c.want)
		}
	}
}

func TestDescribeErrors(t *testing.T) {
	for _, expr := range []string{"", "60 * * * *", "* * * *", "@nope", "0 0 L-40 * *"} {
		var pe *ParseError
		if _, err := Describe(expr); !errors.As(err, &pe) {
			t.Errorf("Describe(%q) = %v, want *ParseError", expr, err)
		}
	}
	var pe *ParseError
	if _, err := DescribeSeconds("0 9 * * *"); !errors.As(err, &pe) {
		t.Errorf("DescribeSeconds of five fields = %v, want *ParseError", err)
	}
}
