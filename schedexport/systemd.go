package schedexport

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

// unitName is what a systemd unit name may hold before its suffix; "@"
// is left out because it would make a template unit.
var unitName = regexp.MustCompile(`^[A-Za-z0-9:_.\-]{1,240}$`)

// SystemdTimer writes s as a systemd timer and the oneshot service it
// starts: <name>.timer and <name>.service, in that order. The mapping:
//
//   - OnCalendar= is written from the parsed fields with the Location's
//     IANA name (or UTC) at the end, so the timer fires on the schedule's
//     wall clock. Seconds and a year field are expressed. A day rule of
//     "either day matches" becomes two OnCalendar= lines, one per day
//     field, since systemd ANDs a weekday with a date. "@every" is
//     refused.
//   - Persistent=true for MisfireFireOnce without Within (systemd starts
//     the service once at boot for the ticks it missed); false for
//     MisfireSkip and the zero policy (systemd's default). MisfireFireAll
//     and Within are refused: systemd fires at most once and has no age
//     limit.
//   - Overlap must be OverlapSkip: starting a service that is still
//     running does nothing, so systemd always skips a tick during a run
//     and cannot express OverlapAllow (the zero value).
//   - AccuracySec=1s, because systemd's default lets a timer fire up to a
//     minute late to batch wake-ups.
//   - Active and Windows are refused.
//
// The service runs Target.Command and Target.Args, quoted for ExecStart=.
//
//ds:def id=schedexport-systemdtimer-527u7nzp owner=@khanakia stability=stable
func SystemdTimer(s workflow.Schedule, t Target) ([]File, error) {
	const f = FormatSystemd
	j, err := systemdPrepare(s, t)
	if err != nil {
		return nil, err
	}
	if err := j.noWindows(f); err != nil {
		return nil, err
	}
	if !s.Active.IsZero() {
		return nil, refuse(f, "Active", "a timer has no start or end; enable and disable it at those times")
	}
	if s.Overlap != workflow.OverlapSkip {
		return nil, refuse(f, "Overlap", "a running service is never started twice, so a tick during a run is skipped; set Overlap to OverlapSkip")
	}
	fl, err := j.fields(f)
	if err != nil {
		return nil, err
	}
	zone, err := zoneName(f, s.Location)
	if err != nil {
		return nil, err
	}
	if zone == "" {
		zone = "UTC"
	}
	return j.systemdUnits(onCalendar(fl, zone))
}

// SystemdTimerNextFire writes a one-shot timer and its service for the
// schedule's next fire after clk's time (inside Active and Windows, in
// the schedule's year field): OnCalendar= is that instant in UTC. A
// desktop app writes it before it quits so systemd starts it at the next
// tick, and writes a new one each time it runs. Persistent= follows the
// misfire policy as for SystemdTimer; overlap is not checked, since one
// fire has no previous run of its own. It returns ErrNoNextFire when the
// schedule never fires again.
//
//ds:def id=schedexport-systemdtimernextfire-bbkt6p5a owner=@khanakia stability=stable
func SystemdTimerNextFire(s workflow.Schedule, t Target, clk clock.Clock) ([]File, error) {
	j, err := systemdPrepare(s, t)
	if err != nil {
		return nil, err
	}
	next, err := j.nextFire(clk)
	if err != nil {
		return nil, err
	}
	return j.systemdUnits([]string{next.UTC().Format("2006-01-02 15:04:05") + " UTC"})
}

//ds:def id=schedexport-systemdprepare-qtrfbgra owner=@khanakia stability=stable
func systemdPrepare(s workflow.Schedule, t Target) (job, error) {
	const f = FormatSystemd
	j, err := prepare(f, s, t)
	if err != nil {
		return job{}, err
	}
	if !unitName.MatchString(j.name) {
		return job{}, invalid(f, "unit name %q: use letters, digits, ':', '_', '.' and '-'", j.name)
	}
	if len(t.Command) == 0 {
		return job{}, invalid(f, "Target.Command is required")
	}
	if strings.IndexAny(t.Command[0], "-@:+!|") == 0 || t.Command[0] == "" {
		return job{}, invalid(f, "command %q is empty or starts with an ExecStart= prefix character", t.Command[0])
	}
	if strings.HasSuffix(j.desc, `\`) {
		return job{}, invalid(f, "description ends with a backslash, which continues the line")
	}
	return j, nil
}

// systemdUnits writes the timer with the given OnCalendar= values and the
// service.
//
//ds:def id=schedexport-job-systemdunits-5b3buk4u owner=@khanakia stability=stable
func (j job) systemdUnits(calendars []string) ([]File, error) {
	m, err := classify(FormatSystemd, j.s.Misfire)
	if err != nil {
		return nil, err
	}
	switch m {
	case misfireDeadline:
		return nil, refuse(FormatSystemd, "Misfire", "Within %s: Persistent= catches up any missed tick, however old", j.s.Misfire.Within)
	}
	desc := strings.ReplaceAll(j.desc, "%", "%%")
	var timer strings.Builder
	timer.WriteString("[Unit]\nDescription=" + desc + "\n\n[Timer]\n")
	for _, c := range calendars {
		timer.WriteString("OnCalendar=" + c + "\n")
	}
	timer.WriteString("AccuracySec=1s\n")
	timer.WriteString("Persistent=" + strconv.FormatBool(m == misfireCatchUp) + "\n")
	timer.WriteString("Unit=" + j.name + ".service\n\n[Install]\nWantedBy=timers.target\n")

	args := j.argvQuoted()
	service := "[Unit]\nDescription=" + desc + "\n\n[Service]\nType=oneshot\nExecStart=" + args + "\n"
	return []File{
		{Name: j.name + ".timer", Data: []byte(timer.String())},
		{Name: j.name + ".service", Data: []byte(service)},
	}, nil
}

// argvQuoted is the command line for ExecStart=: each argument with "%"
// and "$" doubled (specifier and variable expansion), and double-quoted
// with backslash escapes when it is empty or holds a space, a quote, a
// backslash or a ";".
//
//ds:def id=schedexport-job-argvquoted-9n89ub9y owner=@khanakia stability=stable
func (j job) argvQuoted() string {
	var out []string
	for _, a := range j.target.argv() {
		esc := strings.NewReplacer("%", "%%", "$", "$$").Replace(a)
		if a == "" || strings.ContainsAny(a, " \t\"'\\;") {
			esc = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(esc) + `"`
		}
		out = append(out, esc)
	}
	return strings.Join(out, " ")
}

// onCalendar writes fl as OnCalendar= values ("Mon..Fri *-*-* 09:00:00
// Europe/Berlin"): one, or two when either day field alone selects a day.
//
//ds:def id=schedexport-oncalendar-39zvsdns owner=@khanakia stability=stable
func onCalendar(fl cron.Fields, zone string) []string {
	date := func(dom []int) string {
		year := "*"
		if fl.Year != nil {
			year = systemdList(fl.Year, cron.MinYear, cron.MaxYear, false, false)
		}
		return year + "-" + systemdList(fl.Month, 1, 12, true, true) + "-" + systemdList(dom, 1, 31, true, true)
	}
	clock := systemdList(fl.Hour, 0, 23, true, true) + ":" + systemdList(fl.Minute, 0, 59, true, true) + ":" + systemdList(fl.Second, 0, 59, true, true)
	line := func(dow []int, dom []int) string {
		out := date(dom) + " " + clock + " " + zone
		if dow != nil && !full(dow, 0, 6) {
			out = weekdays(dow) + " " + out
		}
		return out
	}
	all := allDays()
	domFull, dowFull := full(fl.DayOfMonth, 1, 31), full(fl.DayOfWeek, 0, 6)
	switch {
	case fl.DayOr && !domFull && !dowFull:
		return []string{line(nil, fl.DayOfMonth), line(fl.DayOfWeek, all)}
	case fl.DayOr:
		return []string{line(nil, all)}
	}
	return []string{line(fl.DayOfWeek, fl.DayOfMonth)}
}

func allDays() []int {
	out := make([]int, 31)
	for i := range out {
		out[i] = i + 1
	}
	return out
}

// systemdList writes a calendar component: "*", values, "a..b" ranges and
// "a/n" (or "a..b/n") repetitions; pad writes two digits; open allows
// "a/n" for a run that reaches the field's end.
func systemdList(vals []int, lo, hi int, pad, open bool) string {
	if full(vals, lo, hi) {
		return "*"
	}
	num := strconv.Itoa
	if pad {
		num = itoa2
	}
	var items []string
	for _, sp := range spans(vals) {
		switch {
		case sp.lo == sp.hi:
			items = append(items, num(sp.lo))
		case sp.step == 1:
			items = append(items, num(sp.lo)+".."+num(sp.hi))
		case open && sp.hi+sp.step > hi:
			items = append(items, num(sp.lo)+"/"+strconv.Itoa(sp.step))
		default:
			items = append(items, num(sp.lo)+".."+num(sp.hi)+"/"+strconv.Itoa(sp.step))
		}
	}
	return strings.Join(items, ",")
}

// weekdays writes days (0-6, Sunday 0) as systemd weekday names, with
// runs inside Monday to Saturday as "Mon..Fri" and Sunday last.
func weekdays(days []int) string {
	names := [7]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
	var mon, items []string
	sunday := false
	var week []int
	for _, d := range days {
		if d == 0 {
			sunday = true
			continue
		}
		week = append(week, d)
	}
	for _, sp := range spans(week) {
		switch {
		case sp.lo == sp.hi:
			mon = append(mon, names[sp.lo])
		case sp.step == 1:
			mon = append(mon, names[sp.lo]+".."+names[sp.hi])
		default:
			for d := sp.lo; d <= sp.hi; d += sp.step {
				mon = append(mon, names[d])
			}
		}
	}
	items = append(items, mon...)
	if sunday {
		items = append(items, names[0])
	}
	return strings.Join(items, ",")
}
