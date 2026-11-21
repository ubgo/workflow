package cron

import (
	"testing"
	"time"
)

// BenchmarkNext measures one Next call for common expressions, in UTC and
// in a zone with DST, from an instant in the middle of a minute.
func BenchmarkNext(b *testing.B) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		b.Skip(err)
	}
	after := time.Date(2026, 3, 10, 10, 30, 17, 0, time.UTC)
	for _, bc := range []struct {
		name, expr string
		loc        *time.Location
	}{
		{"every-minute-UTC", "* * * * *", time.UTC},
		{"every-minute-Berlin", "* * * * *", berlin},
		{"every-5-min-Berlin", "*/5 * * * *", berlin},
		{"daily-0900-Berlin", "0 9 * * *", berlin},
		{"weekdays-1730-UTC", "30 17 * * MON-FRI", time.UTC},
		{"monthly-1st-UTC", "0 0 1 * *", time.UTC},
	} {
		s, err := Parse(bc.expr)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(bc.name, func(b *testing.B) {
			for b.Loop() {
				_ = s.Next(after, bc.loc)
			}
		})
	}
}

// BenchmarkBetween1000 is a backfill plan: the next 1000 ticks of an
// every-minute schedule.
func BenchmarkBetween1000(b *testing.B) {
	s, err := Parse("* * * * *")
	if err != nil {
		b.Fatal(err)
	}
	from := time.Date(2026, 3, 10, 10, 30, 17, 0, time.UTC)
	for b.Loop() {
		_ = s.Between(from, from.Add(1000*time.Minute), time.UTC, 1000)
	}
}
