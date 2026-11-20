package cron

import (
	"strconv"
	"strings"
	"time"
)

// Extended is the Parser of the extended dialect (ParseExtended), ready to
// pass where a driver takes a cron.Parser:
//
//	c := memory.New(memory.WithCronParser(cron.Extended))
//
// It is opt-in: a schedule written in it runs only on drivers built with
// it (memory, sqlstore and redis through their WithCronParser); an engine
// with its own five-field dialect (Hatchet) refuses the expressions.
var Extended Parser = ParseExtended

// ParseExtended parses the standard five fields (everything Parse
// accepts, macros and "@every" included) plus the special day items most
// enterprise schedulers know:
//
// Day of month:
//   - "L": the last day of the month.
//   - "L-n": n days before the last day (n 0-30; "L-1" is the day before
//     the last). A month too short for it does not fire.
//   - "nW": the weekday (Monday to Friday) nearest day n (1-31) of the
//     month, never crossing into another month: a Saturday 1st fires on
//     Monday the 3rd, a Sunday that ends the month fires on the Friday
//     before. A month without day n does not fire.
//   - "LW": the last weekday of the month.
//
// Day of week:
//   - "d#n": the n-th weekday d of the month (n 1-5; "MON#1", "5#3"). A
//     month without a fifth d does not fire for "#5".
//   - "dL": the last weekday d of the month ("FRIL", "5L").
//
// Special items mix with ordinary ones in a list ("1,15,L", "MON#1,FRIL"),
// and a field holding one counts as restricted for the Vixie OR rule (see
// the package doc): "0 0 L * FRI" fires on the last day of every month
// and on every Friday. Next-run maths and the DST rules are Parse's.
// Every error is a *ParseError naming the field. An expression without a
// special item parses to the same schedule Parse returns.
func ParseExtended(expr string) (Schedule, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" || strings.HasPrefix(trimmed, "@") {
		return Parse(expr)
	}
	parts := strings.Fields(trimmed)
	if len(parts) != len(fields) {
		return Parse(expr) // the same field-count error
	}
	domPlain, domSpecial, perr := splitDOM(parts[2])
	if perr == nil {
		var dowPlain string
		var dowSpecial []dowItem
		dowPlain, dowSpecial, perr = splitDOW(parts[4])
		if perr == nil {
			if len(domSpecial) == 0 && len(dowSpecial) == 0 {
				return Parse(expr)
			}
			return buildExt(expr, trimmed, parts, domPlain, domSpecial, dowPlain, dowSpecial)
		}
	}
	perr.Expr = expr
	return nil, perr
}

func buildExt(expr, trimmed string, parts []string, domPlain string, domSpecial []domItem, dowPlain string, dowSpecial []dowItem) (Schedule, error) {
	s := &spec{expr: trimmed}
	var bits [5]uint64
	for i, f := range fields {
		text := parts[i]
		switch i {
		case 2:
			text = domPlain
		case 4:
			text = dowPlain
		}
		if text == "" { // only special items: the plain half matches nothing
			continue
		}
		b, err := parseField(text, f)
		if err != nil {
			err.Expr = expr
			return nil, err
		}
		bits[i] = b
	}
	s.minute, s.hour, s.dom, s.month = bits[0], bits[1], bits[2], bits[3]
	s.dow = bits[4] | bits[4]>>7&1
	s.dow &^= 1 << 7
	// A special item restricts its field, so "*" only counts when the
	// field is nothing but a star item.
	s.domStar = len(domSpecial) == 0 && strings.HasPrefix(parts[2], "*")
	s.dowStar = len(dowSpecial) == 0 && strings.HasPrefix(parts[4], "*")
	return &extSpec{spec: s, dom: domSpecial, dow: dowSpecial}, nil
}

// domKind is the kind of a special day-of-month item.
type domKind int

const (
	domLast        domKind = iota // "L" and "L-n"
	domNearest                    // "nW"
	domLastWeekday                // "LW"
)

type domItem struct {
	kind domKind
	n    int // days before the last for domLast, the day for domNearest
}

// dowItem is "d#n" (nth 1-5) or "dL" (nth 0: the last).
type dowItem struct {
	day time.Weekday
	nth int
}

// splitDOM separates the special items of a day-of-month field from the
// ordinary ones, which it rejoins for parseField.
func splitDOM(text string) (string, []domItem, *ParseError) {
	f := fields[2]
	var plain []string
	var special []domItem
	for _, item := range strings.Split(text, ",") {
		up := strings.ToUpper(item)
		switch {
		case up == "L":
			special = append(special, domItem{kind: domLast})
		case up == "LW":
			special = append(special, domItem{kind: domLastWeekday})
		case strings.HasPrefix(up, "L-"):
			n, err := strconv.Atoi(up[2:])
			if err != nil || n < 0 || n > 30 {
				return "", nil, &ParseError{Field: f.name, Value: item, Reason: "L-n needs n from 0 to 30"}
			}
			special = append(special, domItem{kind: domLast, n: n})
		case strings.HasSuffix(up, "W"):
			n, err := strconv.Atoi(up[:len(up)-1])
			if err != nil || n < 1 || n > 31 {
				return "", nil, &ParseError{Field: f.name, Value: item, Reason: "nW needs a day from 1 to 31"}
			}
			special = append(special, domItem{kind: domNearest, n: n})
		case strings.Contains(up, "L") || strings.Contains(up, "#"):
			return "", nil, &ParseError{Field: f.name, Value: item, Reason: "not a day-of-month item (L, L-n, nW and LW are the special ones)"}
		default:
			plain = append(plain, item)
		}
	}
	return strings.Join(plain, ","), special, nil
}

// splitDOW separates the "d#n" and "dL" items of a day-of-week field.
func splitDOW(text string) (string, []dowItem, *ParseError) {
	f := fields[4]
	var plain []string
	var special []dowItem
	for _, item := range strings.Split(text, ",") {
		up := strings.ToUpper(item)
		switch {
		case strings.Contains(up, "#"):
			d, n, _ := strings.Cut(up, "#")
			day, err := parseWeekday(d)
			if err != nil {
				return "", nil, &ParseError{Field: f.name, Value: item, Reason: err.Error()}
			}
			nth, aerr := strconv.Atoi(n)
			if aerr != nil || nth < 1 || nth > 5 {
				return "", nil, &ParseError{Field: f.name, Value: item, Reason: "d#n needs n from 1 to 5"}
			}
			special = append(special, dowItem{day: day, nth: nth})
		case up == "L":
			return "", nil, &ParseError{Field: f.name, Value: item, Reason: "a bare L is ambiguous here; write dL (\"FRIL\", \"5L\") for the last such weekday of the month"}
		case strings.HasSuffix(up, "L"):
			day, err := parseWeekday(up[:len(up)-1])
			if err != nil {
				return "", nil, &ParseError{Field: f.name, Value: item, Reason: err.Error()}
			}
			special = append(special, dowItem{day: day})
		default:
			plain = append(plain, item)
		}
	}
	return strings.Join(plain, ","), special, nil
}

// parseWeekday reads one weekday, a number 0-7 (7 is Sunday) or a name.
func parseWeekday(text string) (time.Weekday, error) {
	v, err := parseValue(text, fields[4])
	if err != nil {
		return 0, err
	}
	return time.Weekday(v % 7), nil
}

// extSpec is a five-field spec plus special day items. The ordinary items
// keep their bitsets in spec; a day matches a field when a bit or a
// special item matches it.
type extSpec struct {
	*spec
	dom []domItem
	dow []dowItem
}

// lastDay is the number of days in day's month.
func lastDay(day time.Time) int {
	return time.Date(day.Year(), day.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// nearestWeekday is the weekday of day's month nearest day n, or 0 when the
// month has no day n.
func nearestWeekday(day time.Time, n int) int {
	last := lastDay(day)
	if n > last {
		return 0
	}
	switch time.Date(day.Year(), day.Month(), n, 0, 0, 0, 0, time.UTC).Weekday() {
	case time.Saturday:
		if n == 1 {
			return 3 // Monday the 3rd: never the month before
		}
		return n - 1
	case time.Sunday:
		if n == last {
			return n - 2 // Friday: never the month after
		}
		return n + 1
	}
	return n
}

// lastWeekday is the last Monday-to-Friday day of day's month.
func lastWeekday(day time.Time) int {
	last := lastDay(day)
	switch time.Date(day.Year(), day.Month(), last, 0, 0, 0, 0, time.UTC).Weekday() {
	case time.Saturday:
		return last - 1
	case time.Sunday:
		return last - 2
	}
	return last
}

func (s *extSpec) dayMatches(day time.Time) bool {
	d := day.Day()
	domOK := s.spec.dom&(1<<uint(d)) != 0
	for _, it := range s.dom {
		if domOK {
			break
		}
		switch it.kind {
		case domLast:
			domOK = d == lastDay(day)-it.n
		case domNearest:
			domOK = d == nearestWeekday(day, it.n)
		case domLastWeekday:
			domOK = d == lastWeekday(day)
		}
	}
	dowOK := s.spec.dow&(1<<uint(day.Weekday())) != 0
	for _, it := range s.dow {
		if dowOK {
			break
		}
		if day.Weekday() != it.day {
			continue
		}
		if it.nth == 0 {
			dowOK = d+7 > lastDay(day)
		} else {
			dowOK = (d-1)/7+1 == it.nth
		}
	}
	if s.domStar || s.dowStar {
		return domOK && dowOK
	}
	return domOK || dowOK
}

// Next implements Schedule with the walk of spec.Next and the special day
// items; resolve applies the DST rules.
func (s *extSpec) Next(after time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	local := after.In(loc)
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	end := day.AddDate(searchYears, 0, 0)
	for ; !day.After(end); day = day.AddDate(0, 0, 1) {
		if s.month&(1<<uint(day.Month())) == 0 {
			day = time.Date(day.Year(), day.Month()+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
			continue
		}
		best, ok := s.firstOnDay(day, after, loc)
		if !ok {
			continue
		}
		if t, ok := s.firstOnDay(day.AddDate(0, 0, 1), after, loc); ok && t.Before(best) {
			best = t
		}
		return best.In(loc)
	}
	return time.Time{}
}

func (s *extSpec) firstOnDay(day, after time.Time, loc *time.Location) (time.Time, bool) {
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

// Between implements Schedule.
func (s *extSpec) Between(from, to time.Time, loc *time.Location, limit int) []time.Time {
	return between(s, from, to, loc, limit)
}

// String implements Schedule.
func (s *extSpec) String() string { return s.expr }
