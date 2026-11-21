package cron

import (
	"math/rand/v2"
	"testing"
	"time"
)

// oracleNext is spec.Next as it was before firstFire pruned the walk: it
// resolves every matching wall time of each day. It is the reference the
// pruned walk must match instant for instant.
func oracleNext(s *spec, after time.Time, loc *time.Location) time.Time {
	local := after.In(loc)
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	end := day.AddDate(searchYears, 0, 0)
	first := func(day time.Time) (time.Time, bool) {
		if s.month&(1<<uint(day.Month())) == 0 || !s.dayMatches(day) {
			return time.Time{}, false
		}
		periodic := s.hour == allHours
		var best time.Time
		found := false
		for h := 0; h < 24; h++ {
			if s.hour&(1<<uint(h)) == 0 {
				continue
			}
			for m := 0; m < 60; m++ {
				if s.minute&(1<<uint(m)) == 0 {
					continue
				}
				for _, t := range resolve(day.Year(), day.Month(), day.Day(), h, m, loc, periodic) {
					if t.After(after) && (!found || t.Before(best)) {
						best, found = t, true
					}
				}
			}
		}
		return best, found
	}
	for ; !day.After(end); day = day.AddDate(0, 0, 1) {
		if s.month&(1<<uint(day.Month())) == 0 {
			day = time.Date(day.Year(), day.Month()+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
			continue
		}
		best, ok := first(day)
		if !ok {
			continue
		}
		if t, ok := first(day.AddDate(0, 0, 1)); ok && t.Before(best) {
			best = t
		}
		return best.In(loc)
	}
	return time.Time{}
}

// TestNextMatchesOracle checks the pruned walk against oracleNext on random
// instants, many of them within hours of a zone transition, in zones with
// ordinary DST, half-hour DST, DST across midnight, skipped and repeated
// days, and a two-hour jump.
func TestNextMatchesOracle(t *testing.T) {
	zones := []string{"UTC", "Europe/Berlin", "America/New_York", "America/Sao_Paulo", "America/Havana", "America/Santiago",
		"Australia/Lord_Howe", "Pacific/Chatham", "Pacific/Apia", "Africa/Casablanca", "Antarctica/Troll", "Asia/Gaza", "Asia/Kolkata"}
	exprs := []string{"* * * * *", "*/7 * * * *", "30 2 * * *", "0,30 0-3 * * *", "59 23 * * *", "0 0 * * *", "15 * * * *",
		"* 2 * * *", "0 0 * * 0", "*/15 1,2,3 * * 1-5", "0 12 29 2 *", "45 0 1 * *"}
	rng := rand.New(rand.NewPCG(1, 2))
	lo := time.Date(2008, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	hi := time.Date(2032, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	checks := 0
	for _, zn := range zones {
		loc, err := time.LoadLocation(zn)
		if err != nil {
			t.Logf("skipping zone %s: %v", zn, err)
			continue
		}
		// Instants near every transition in the range, then random ones.
		var near []time.Time
		for at := time.Unix(lo, 0).In(loc); ; {
			_, end := at.ZoneBounds()
			if end.IsZero() || end.Unix() > hi {
				break
			}
			near = append(near, end)
			at = end
		}
		for _, ex := range exprs {
			sch, err := Parse(ex)
			if err != nil {
				t.Fatal(err)
			}
			s := sch.(*spec)
			var probes []time.Time
			for range min(len(near), 12) { // a sample of transitions: the oracle is slow under -race
				tr := near[rng.IntN(len(near))]
				probes = append(probes, tr.Add(time.Duration(rng.Int64N(int64(6*time.Hour)))-3*time.Hour))
			}
			for range 12 {
				probes = append(probes, time.Unix(lo+rng.Int64N(hi-lo), rng.Int64N(int64(time.Second))))
			}
			for _, p := range probes {
				// Walk a few ticks forward from each probe.
				at := p
				for range 3 {
					want := oracleNext(s, at, loc)
					got := s.Next(at, loc)
					checks++
					if !got.Equal(want) {
						t.Fatalf("%s in %s after %s: Next = %s, the full walk gives %s", ex, zn, at.In(loc), got, want)
					}
					if want.IsZero() {
						break
					}
					at = want
				}
			}
		}
	}
	t.Logf("%d Next calls matched the full walk", checks)
}
