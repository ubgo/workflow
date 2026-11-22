package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Describe returns an English description of a five-field expression, a
// macro or "@every <duration>", for a settings page or an admin list:
//
//	"0 9 * * MON-FRI"  At 09:00, Monday through Friday
//	"*/15 * * * *"     Every 15 minutes
//	"5 * * * *"        At minute 5 past every hour
//	"0 0 L * *"        At 00:00, on the last day of the month
//	"@every 1h30m"     Every 1 hour and 30 minutes
//
// It accepts the extended day items of ParseExtended too ("L", "L-n",
// "nW", "LW", "d#n", "dL"). When both day fields are restricted the
// description says "or", as cron fires on either (the Vixie rule). Times
// are the schedule's wall clock, 24-hour; the text names no zone, since
// the expression carries none. An invalid expression returns the parser's
// *ParseError.
//
//ds:def id=cron-describe-e5rw59ey owner=@khanakia stability=stable
func Describe(expr string) (string, error) {
	trimmed := strings.TrimSpace(expr)
	if _, err := ParseExtended(expr); err != nil {
		return "", err
	}
	if strings.HasPrefix(trimmed, "@") {
		return describeMacro(trimmed)
	}
	return describeFields(strings.Fields(trimmed), false), nil
}

// DescribeSeconds is Describe for the six-field dialect of ParseSeconds
// (seconds first): "*/10 * * * * *" is "Every 10 seconds", "30 0 9 * *
// MON-FRI" is "At 09:00:30, Monday through Friday". Macros and "@every"
// read as they do in Describe.
//
//ds:def id=cron-describeseconds-vjkqt6sr owner=@khanakia stability=stable
func DescribeSeconds(expr string) (string, error) {
	trimmed := strings.TrimSpace(expr)
	if _, err := ParseSeconds(expr); err != nil {
		return "", err
	}
	if strings.HasPrefix(trimmed, "@") {
		return describeMacro(trimmed)
	}
	return describeFields(strings.Fields(trimmed), true), nil
}

func describeMacro(trimmed string) (string, error) {
	parts := strings.Fields(trimmed)
	if strings.ToLower(parts[0]) == everyPrefix {
		d, _ := time.ParseDuration(parts[1]) // validated by the parser
		return "Every " + durationWords(d), nil
	}
	return describeFields(strings.Fields(macros[strings.ToLower(parts[0])]), false), nil
}

// durationWords writes d as "1 hour and 30 minutes".
func durationWords(d time.Duration) string {
	var out []string
	for _, u := range []struct {
		d    time.Duration
		name string
	}{{24 * time.Hour, "day"}, {time.Hour, "hour"}, {time.Minute, "minute"}, {time.Second, "second"}} {
		if n := int64(d / u.d); n > 0 {
			out = append(out, plural(int(n), u.name))
			d -= time.Duration(n) * u.d
		}
	}
	return joinList(out)
}

// citem is one list item of a field, as parseField reads it.
type citem struct {
	star    bool
	lo, hi  int
	step    int  // 1 without a step
	ranged  bool // "a-b" or "a/n" (a through the field's max)
	special string
}

func (it citem) single() bool { return !it.star && !it.ranged && it.special == "" }

// readItems splits an already validated field into items.
func readItems(text string, f field) []citem {
	var out []citem
	for _, item := range strings.Split(text, ",") {
		rng, stepText, hasStep := strings.Cut(item, "/")
		it := citem{step: 1}
		if hasStep {
			it.step, _ = strconv.Atoi(stepText)
		}
		switch {
		case rng == "*":
			it.star = true
			it.lo, it.hi = f.min, f.max
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			it.lo, _ = parseValue(a, f)
			it.hi, _ = parseValue(b, f)
			it.ranged = true
		default:
			it.lo, _ = parseValue(rng, f)
			it.hi = it.lo
			if hasStep {
				it.hi, it.ranged = f.max, true
			}
		}
		if f.max == 7 && it.single() { // day-of-week: Sunday-as-7 is 0
			it.lo %= 7
			it.hi = it.lo
		}
		out = append(out, it)
	}
	return out
}

// singles returns the values when every item is a plain value.
func singles(items []citem) ([]int, bool) {
	var out []int
	for _, it := range items {
		if !it.single() {
			return nil, false
		}
		out = append(out, it.lo)
	}
	return out, true
}

func isStar(items []citem) bool { return len(items) == 1 && items[0].star && items[0].step == 1 }

func describeFields(parts []string, seconds bool) string {
	sec := []citem{{lo: 0, hi: 0, step: 1}}
	if seconds {
		sec = readItems(parts[0], secondField)
		parts = parts[1:]
	}
	minute := readItems(parts[0], fields[0])
	hour := readItems(parts[1], fields[1])
	out := describeTime(sec, minute, hour, seconds)
	if day := describeDays(parts[2], parts[4]); day != "" {
		out += ", " + day
	}
	if month := describeMonths(parts[3]); month != "" {
		out += ", " + month
	}
	r := []rune(out)
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}

// maxTimes is how many clock times "At 09:00, 12:00 and 17:00" lists
// before the description switches to field phrases.
const maxTimes = 8

func describeTime(sec, minute, hour []citem, seconds bool) string {
	secs, sOK := singles(sec)
	mins, mOK := singles(minute)
	hours, hOK := singles(hour)
	clock := func(h, m, s int) string {
		if s != 0 {
			return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
		}
		return fmt.Sprintf("%02d:%02d", h, m)
	}
	times := func(s int) []string {
		var out []string
		for _, h := range hours {
			for _, m := range mins {
				out = append(out, clock(h, m, s))
			}
		}
		return out
	}
	fewTimes := mOK && hOK && len(mins)*len(hours) <= maxTimes
	if sOK && len(secs) == 1 && fewTimes {
		return "at " + joinList(times(secs[0]))
	}
	secPhrase := ""
	if seconds && !(sOK && len(secs) == 1 && secs[0] == 0) {
		secPhrase = unitPhrase(sec, "second", strconv.Itoa)
	}
	if secPhrase != "" && fewTimes {
		return secPhrase + ", during " + joinList(times(0))
	}
	minPhrase := ""
	switch {
	case secPhrase != "" && isStar(minute):
	case mOK && len(mins) == 1 && mins[0] == 0 && isStar(hour):
		return "every hour"
	case mOK && len(mins) == 1 && mins[0] == 0 && len(hour) == 1 && hour[0].star:
		return "every " + plural(hour[0].step, "hour")
	default:
		minPhrase = unitPhrase(minute, "minute", strconv.Itoa)
	}
	out := secPhrase
	if out != "" && minPhrase != "" {
		out += ", "
	}
	out += minPhrase
	return out + hourQualifier(hour, strings.HasPrefix(minPhrase, "at "))
}

// unitPhrase describes a second or minute field.
func unitPhrase(items []citem, unit string, name func(int) string) string {
	if vals, ok := singles(items); ok {
		names := make([]string, len(vals))
		for i, v := range vals {
			names[i] = name(v)
		}
		if len(vals) == 1 {
			return "at " + unit + " " + names[0]
		}
		return "at " + unit + "s " + joinList(names)
	}
	if len(items) == 1 {
		it := items[0]
		switch {
		case it.star && it.step == 1:
			return "every " + unit
		case it.star:
			return "every " + plural(it.step, unit)
		case it.step == 1:
			return "every " + unit + " from " + name(it.lo) + " through " + name(it.hi)
		default:
			return "every " + plural(it.step, unit) + " from " + unit + " " + name(it.lo) + " through " + name(it.hi)
		}
	}
	return "at " + unit + "s " + joinList(itemTexts(items, name))
}

// itemTexts writes each item of a mixed list.
func itemTexts(items []citem, name func(int) string) []string {
	out := make([]string, len(items))
	for i, it := range items {
		switch {
		case it.special != "":
			out[i] = it.special
		case it.star && it.step == 1:
			out[i] = "every value"
		case it.star:
			out[i] = "every " + ordinal(it.step)
		case it.single():
			out[i] = name(it.lo)
		case it.step == 1:
			out[i] = name(it.lo) + " through " + name(it.hi)
		default:
			out[i] = "every " + ordinal(it.step) + " from " + name(it.lo) + " through " + name(it.hi)
		}
	}
	return out
}

func hourQualifier(items []citem, minuteAt bool) string {
	hh := func(h int) string { return fmt.Sprintf("%02d", h) }
	if isStar(items) {
		if minuteAt {
			return " past every hour"
		}
		return ""
	}
	lead := ", "
	if minuteAt {
		lead = " past every hour, "
	}
	if len(items) == 1 {
		it := items[0]
		switch {
		case it.star && minuteAt:
			return ", every " + plural(it.step, "hour")
		case it.star:
			return ", during every " + ordinal(it.step) + " hour"
		case it.step == 1:
			return lead + fmt.Sprintf("between %02d:00 and %02d:59", it.lo, it.hi)
		default:
			return fmt.Sprintf(", every %s between %02d:00 and %02d:59", plural(it.step, "hour"), it.lo, it.hi)
		}
	}
	if minuteAt {
		return " past hours " + joinList(itemTexts(items, hh))
	}
	return ", during hours " + joinList(itemTexts(items, hh))
}

//ds:def id=cron-describedays-mn5tyxmv owner=@khanakia stability=stable
func describeDays(domText, dowText string) string {
	domStar := strings.HasPrefix(domText, "*") && !strings.ContainsAny(strings.ToUpper(domText), "LW")
	dowStar := strings.HasPrefix(dowText, "*") && !strings.ContainsAny(dowText, "#Ll")
	dom, dow := "", ""
	if !(domStar && domText == "*") {
		dom = describeDOM(domText)
	}
	if !(dowStar && dowText == "*") {
		dow = describeDOW(dowText)
	}
	switch {
	case dom != "" && dow != "" && !domStar && !dowStar:
		return dom + " or " + dow
	case dom != "" && dow != "":
		return dom + ", " + onlyOn(dow)
	}
	return dom + onlyOn(dow)
}

// onlyOn reads "on Monday" as "only on Monday" where no OR follows.
func onlyOn(dow string) string {
	if strings.HasPrefix(dow, "on ") && !strings.HasPrefix(dow, "on the ") {
		return "only " + dow
	}
	return dow
}

func describeDOM(text string) string {
	plain, special, _ := splitDOM(text)
	var parts, vals []string
	if plain != "" {
		items := readItems(plain, fields[2])
		if len(items) == 1 && items[0].star {
			parts = append(parts, "every "+ordinal(items[0].step)+" day of the month")
		} else {
			vals = itemTexts(items, strconv.Itoa)
		}
	}
	if len(vals) == 1 && !strings.Contains(vals[0], " ") {
		parts = append(parts, "on day "+vals[0]+" of the month")
	} else if len(vals) > 0 {
		parts = append(parts, "on days "+joinList(vals)+" of the month")
	}
	for _, it := range special {
		switch it.kind {
		case domLast:
			if it.n == 0 {
				parts = append(parts, "on the last day of the month")
			} else {
				parts = append(parts, plural(it.n, "day")+" before the last day of the month")
			}
		case domNearest:
			parts = append(parts, fmt.Sprintf("on the weekday nearest day %d of the month", it.n))
		case domLastWeekday:
			parts = append(parts, "on the last weekday of the month")
		}
	}
	return joinList(parts)
}

func describeDOW(text string) string {
	plain, special, _ := splitDOW(text)
	day := func(v int) string { return time.Weekday(v % 7).String() }
	var parts []string
	if plain != "" {
		items := readItems(plain, fields[4])
		switch {
		case len(items) == 1 && items[0].star:
			parts = append(parts, "every "+ordinal(items[0].step)+" day of the week")
		case len(items) == 1 && items[0].ranged && items[0].step == 1:
			parts = append(parts, day(items[0].lo)+" through "+day(items[0].hi))
		default:
			parts = append(parts, "on "+joinList(itemTexts(items, day)))
		}
	}
	for _, it := range special {
		if it.nth == 0 {
			parts = append(parts, "on the last "+it.day.String()+" of the month")
		} else {
			parts = append(parts, "on the "+ordinalWord(it.nth)+" "+it.day.String()+" of the month")
		}
	}
	return joinList(parts)
}

func describeMonths(text string) string {
	if text == "*" {
		return ""
	}
	items := readItems(text, fields[3])
	month := func(v int) string { return time.Month(v).String() }
	switch {
	case len(items) == 1 && items[0].star:
		return "every " + plural(items[0].step, "month")
	case len(items) == 1 && items[0].ranged && items[0].step == 1:
		return month(items[0].lo) + " through " + month(items[0].hi)
	}
	return "only in " + joinList(itemTexts(items, month))
}

// joinList writes "a", "a and b", "a, b and c".
func joinList(xs []string) string {
	switch len(xs) {
	case 0:
		return ""
	case 1:
		return xs[0]
	}
	return strings.Join(xs[:len(xs)-1], ", ") + " and " + xs[len(xs)-1]
}

func plural(n int, unit string) string {
	if n == 1 {
		return "1 " + unit
	}
	return strconv.Itoa(n) + " " + unit + "s"
}

// ordinal writes 2 as "2nd", 3 as "3rd", 11 as "11th".
func ordinal(n int) string {
	suffix := "th"
	if n%100 < 11 || n%100 > 13 {
		switch n % 10 {
		case 1:
			suffix = "st"
		case 2:
			suffix = "nd"
		case 3:
			suffix = "rd"
		}
	}
	return strconv.Itoa(n) + suffix
}

// ordinalWord writes 1-5 as words, for "the third Monday".
func ordinalWord(n int) string {
	return [...]string{"", "first", "second", "third", "fourth", "fifth"}[n]
}
