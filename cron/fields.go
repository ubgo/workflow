package cron

// Fields is a parsed expression as the values each field matches, for code
// that writes the schedule in another scheduler's format (a Kubernetes
// CronJob, a systemd timer). Translating the values instead of the text
// is what keeps such a writer honest: names, macros, steps and Sunday as 7
// are already resolved, and the day rule is stated, not re-derived from
// the text.
//
// Every list is ascending with no duplicates.
type Fields struct {
	// Second lists seconds 0-59; a five-field expression fires at second 0.
	Second []int
	// Minute lists minutes 0-59.
	Minute []int
	// Hour lists hours 0-23.
	Hour []int
	// DayOfMonth lists days 1-31.
	DayOfMonth []int
	// Month lists months 1-12.
	Month []int
	// DayOfWeek lists weekdays 0-6, Sunday 0.
	DayOfWeek []int
	// Year lists the matching years for a ParseYear expression; nil means
	// every year with no end.
	Year []int
	// DayOr is the day rule: true when both day fields were restricted
	// (neither started with "*"), so a day matches when EITHER matches
	// (the Vixie rule); false when both must match.
	DayOr bool
}

// FieldsOf returns the fields of a schedule from Parse, ParseSeconds or
// ParseYear (a macro included). It reports false for an "@every" interval,
// which counts seconds from the Unix epoch and has no fields, and for a
// schedule from another Parser, unless that schedule offers its own by
// implementing Fields() Fields.
func FieldsOf(s Schedule) (Fields, bool) {
	switch v := s.(type) {
	case *spec:
		return v.fields([]int{0}), true
	case *secSpec:
		return v.inner.fields(bitList(v.second, 0, 59)), true
	case *yearSpec:
		f := v.inner.inner.fields(bitList(v.inner.second, 0, 59))
		for y := MinYear; y <= MaxYear; y++ {
			if v.years.has(y) {
				f.Year = append(f.Year, y)
			}
		}
		return f, true
	case interface{ Fields() Fields }:
		return v.Fields(), true
	}
	return Fields{}, false
}

// fields lists s's values, with seconds given by the caller.
func (s *spec) fields(second []int) Fields {
	return Fields{
		Second:     second,
		Minute:     bitList(s.minute, 0, 59),
		Hour:       bitList(s.hour, 0, 23),
		DayOfMonth: bitList(s.dom, 1, 31),
		Month:      bitList(s.month, 1, 12),
		DayOfWeek:  bitList(s.dow, 0, 6),
		DayOr:      !s.domStar && !s.dowStar,
	}
}

// bitList returns the set bits of b in lo-hi, ascending.
func bitList(b uint64, lo, hi int) []int {
	var out []int
	for v := lo; v <= hi; v++ {
		if b&(1<<uint(v)) != 0 {
			out = append(out, v)
		}
	}
	return out
}
