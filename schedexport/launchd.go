package schedexport

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

// LaunchdOptions are the parts of a launchd job a Schedule does not carry.
type LaunchdOptions struct {
	// Zone is the time zone of the Mac the plist is installed on.
	// Required: launchd reads StartCalendarInterval on the machine's own
	// clock and has no zone setting, so a schedule is exported only when
	// its Location is this zone (the NextFire form converts its one
	// instant into it instead).
	Zone *time.Location
}

// maxIntervals bounds the StartCalendarInterval entries one plist gets:
// an expression like "* 9-17 * * 1-5" with minutes listed one by one
// would need thousands.
const maxIntervals = 1000

var launchdLabel = regexp.MustCompile(`^[A-Za-z0-9._\-]{1,200}$`)

// Launchd writes s as a launchd property list with one
// StartCalendarInterval entry per combination of the restricted fields
// (an unrestricted field is left out, which launchd reads as "every").
// The file is named <name>.plist and its Label is the name, usually
// reverse DNS ("com.example.report"). The mapping:
//
//   - Location must be LaunchdOptions.Zone (see there).
//   - A seconds field other than 0 and a year field are refused: launchd
//     has neither.
//   - "Either day matches" becomes separate Day and Weekday entries;
//     "both must match" with both day fields restricted is refused,
//     since one entry with both keys is not documented to mean AND.
//   - Overlap must be OverlapSkip: launchd never starts a second copy of
//     a running job.
//   - Misfire must be the zero policy: launchd starts one run on wake
//     for the ticks a sleeping Mac missed, and none for a Mac that was
//     off, which no policy describes.
//   - Active and Windows are refused, and so is a schedule needing more
//     than 1000 entries.
func Launchd(s workflow.Schedule, t Target, o LaunchdOptions) (File, error) {
	const f = FormatLaunchd
	j, err := launchdPrepare(s, t, o)
	if err != nil {
		return File{}, err
	}
	if !sameZone(s.Location, o.Zone) {
		return File{}, refuse(f, "Location", "zone %s: launchd fires on the Mac's own zone, %s", s.Location, o.Zone)
	}
	if err := j.noWindows(f); err != nil {
		return File{}, err
	}
	if !s.Active.IsZero() {
		return File{}, refuse(f, "Active", "a job has no start or end; load and unload it at those times")
	}
	if s.Overlap != workflow.OverlapSkip {
		return File{}, refuse(f, "Overlap", "a running job is never started twice, so a tick during a run is skipped; set Overlap to OverlapSkip")
	}
	fl, err := j.fields(f)
	if err != nil {
		return File{}, err
	}
	entries, err := calendarIntervals(fl)
	if err != nil {
		return File{}, err
	}
	return j.plist(entries), nil
}

// LaunchdNextFire writes a property list for the schedule's next fire
// after clk's time (inside Active, Windows and the year field) as one
// StartCalendarInterval entry on LaunchdOptions.Zone's wall clock. launchd
// has no one-shot date: the entry fires again on the same date a year
// later, so the app replaces the plist each time it runs, which is what a
// handoff does anyway. A fire at a second other than 0 is refused
// (launchd starts jobs on the minute). Misfire must be the zero policy,
// as for Launchd. It returns ErrNoNextFire when the schedule never fires
// again.
func LaunchdNextFire(s workflow.Schedule, t Target, o LaunchdOptions, clk clock.Clock) (File, error) {
	j, err := launchdPrepare(s, t, o)
	if err != nil {
		return File{}, err
	}
	next, err := j.nextFire(clk)
	if err != nil {
		return File{}, err
	}
	local := next.In(o.Zone)
	if local.Second() != 0 {
		return File{}, refuse(FormatLaunchd, "second", "the next fire %s is not on a minute; launchd starts jobs on the minute", local.Format(time.RFC3339))
	}
	return j.plist([]interval{{
		{"Month", int(local.Month())}, {"Day", local.Day()}, {"Hour", local.Hour()}, {"Minute", local.Minute()},
	}}), nil
}

func launchdPrepare(s workflow.Schedule, t Target, o LaunchdOptions) (job, error) {
	const f = FormatLaunchd
	j, err := prepare(f, s, t)
	if err != nil {
		return job{}, err
	}
	if !launchdLabel.MatchString(j.name) {
		return job{}, invalid(f, "label %q: use letters, digits, '.', '_' and '-'", j.name)
	}
	if len(t.Command) == 0 || t.Command[0] == "" {
		return job{}, invalid(f, "Target.Command is required")
	}
	if o.Zone == nil {
		return job{}, invalid(f, "LaunchdOptions.Zone is required: name the zone the Mac runs in")
	}
	if _, err := classify(f, s.Misfire); err != nil {
		return job{}, err
	}
	if !s.Misfire.IsZero() {
		return job{}, refuse(f, "Misfire", "launchd starts one run on wake for the ticks a sleeping Mac missed and none for a Mac that was off; leave Misfire zero")
	}
	return j, nil
}

// interval is one StartCalendarInterval entry: keys in the order written.
type interval []intervalKey

// intervalKey is one key of an entry and its value.
type intervalKey struct {
	key string
	val int
}

// calendarIntervals expands fl into StartCalendarInterval entries.
func calendarIntervals(fl cron.Fields) ([]interval, error) {
	const f = FormatLaunchd
	if len(fl.Second) != 1 || fl.Second[0] != 0 {
		return nil, refuse(f, "second", "launchd starts jobs on the minute")
	}
	if fl.Year != nil {
		return nil, refuse(f, "year", "it has no year")
	}
	type dayKey struct {
		key  string
		vals []int
	}
	domFull, dowFull := full(fl.DayOfMonth, 1, 31), full(fl.DayOfWeek, 0, 6)
	var days []dayKey // each: a key and its values; none means every day
	switch {
	case fl.DayOr && !domFull && !dowFull:
		days = []dayKey{{"Day", fl.DayOfMonth}, {"Weekday", fl.DayOfWeek}}
	case fl.DayOr || (domFull && dowFull):
	case domFull:
		days = []dayKey{{"Weekday", fl.DayOfWeek}}
	case dowFull:
		days = []dayKey{{"Day", fl.DayOfMonth}}
	default:
		return nil, refuse(f, "day-of-month", "both day fields are restricted and must both match; an entry with Day and Weekday is not documented to mean that")
	}
	// Each axis is a list of (key, value) choices; an unrestricted field
	// is one empty choice, which writes no key.
	type kv struct {
		key string
		val int
		set bool
	}
	axis := func(key string, vals []int, lo, hi int) []kv {
		if full(vals, lo, hi) {
			return []kv{{}}
		}
		out := make([]kv, len(vals))
		for i, v := range vals {
			out[i] = kv{key, v, true}
		}
		return out
	}
	var dayAxis []kv
	if days == nil {
		dayAxis = []kv{{}}
	}
	for _, d := range days {
		for _, v := range d.vals {
			dayAxis = append(dayAxis, kv{d.key, v, true})
		}
	}
	axes := [][]kv{axis("Month", fl.Month, 1, 12), dayAxis, axis("Hour", fl.Hour, 0, 23), axis("Minute", fl.Minute, 0, 59)}
	n := 1
	for _, a := range axes {
		n *= len(a)
	}
	if n > maxIntervals {
		return nil, refuse(f, "Cron", "it needs %d StartCalendarInterval entries, more than %d", n, maxIntervals)
	}
	out := []interval{{}}
	for _, a := range axes {
		var next []interval
		for _, prefix := range out {
			for _, c := range a {
				e := append(interval(nil), prefix...)
				if c.set {
					e = append(e, intervalKey{c.key, c.val})
				}
				next = append(next, e)
			}
		}
		out = next
	}
	return out, nil
}

// plist writes the property list with the given entries.
func (j job) plist(entries []interval) File {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	b.WriteString("<plist version=\"1.0\">\n<dict>\n")
	b.WriteString("\t<key>Label</key>\n\t<string>" + xmlText(j.name) + "</string>\n")
	b.WriteString("\t<key>ProgramArguments</key>\n\t<array>\n")
	for _, a := range j.target.argv() {
		b.WriteString("\t\t<string>" + xmlText(a) + "</string>\n")
	}
	b.WriteString("\t</array>\n")
	b.WriteString("\t<key>StartCalendarInterval</key>\n\t<array>\n")
	for _, e := range entries {
		b.WriteString("\t\t<dict>\n")
		for _, kv := range e {
			b.WriteString("\t\t\t<key>" + kv.key + "</key>\n\t\t\t<integer>" + strconv.Itoa(kv.val) + "</integer>\n")
		}
		b.WriteString("\t\t</dict>\n")
	}
	b.WriteString("\t</array>\n</dict>\n</plist>\n")
	return File{Name: j.name + ".plist", Data: []byte(b.String())}
}

// xmlText escapes s for XML character data. Quotes stay as they are
// (they need no escape outside an attribute), so a command line reads
// as written; control characters were refused by prepare.
func xmlText(s string) string { return xmlEscaper.Replace(strings.ToValidUTF8(s, "\uFFFD")) }

var xmlEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
