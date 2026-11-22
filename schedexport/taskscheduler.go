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

// TaskSchedulerOptions are the parts of a Windows task a Schedule does not
// carry.
//
//ds:def id=schedexport-taskscheduleroptions-dzg46fkr owner=@khanakia stability=stable
type TaskSchedulerOptions struct {
	// Zone is the time zone of the Windows machine the task is
	// registered on. Required by TaskScheduler: a CalendarTrigger runs on
	// the machine's local clock, so a schedule is exported only when its
	// Location is this zone.
	Zone *time.Location
}

// maxTriggers is the most triggers Task Scheduler takes in one task.
//
//ds:def id=schedexport-maxtriggers-chw2bwff owner=@khanakia stability=stable
const maxTriggers = 48

// taskBase is the StartBoundary date of a recurring trigger with no
// Active.From or year field: any date in the past does, and a fixed one
// keeps the file the same on every run.
var taskBase = [3]int{2000, 1, 1}

var taskName = regexp.MustCompile(`^[A-Za-z0-9._\-][A-Za-z0-9 ._\-]{0,199}$`)

// TaskScheduler writes s as a Windows Task Scheduler task (XML, schema
// version 1.2) with CalendarTriggers. The file is named <name>.xml; the
// task's URI is \<name>. The mapping:
//
//   - Location must be TaskSchedulerOptions.Zone (see there).
//   - Each time of day (hour, minute and second fields) is a trigger's
//     StartBoundary; times an equal gap apart, at least a minute, become
//     one trigger with a Repetition. More than 48 triggers are refused.
//   - The day fields become ScheduleByDay, ScheduleByWeek, ScheduleByMonth
//     or ScheduleByMonthDayOfWeek. "Either day matches" with both fields
//     restricted, and "both must match" with both restricted, are
//     refused: two triggers would start the task twice on a day matching
//     both, and no single schedule ANDs a date with a weekday.
//   - Active and a contiguous year field set StartBoundary and
//     EndBoundary; a year list with gaps is refused.
//   - MultipleInstancesPolicy is Parallel for OverlapAllow and IgnoreNew
//     for OverlapSkip.
//   - StartWhenAvailable is true for MisfireFireOnce without Within (the
//     task runs once as soon as it can after a missed start), false for
//     MisfireSkip and the zero policy. MisfireFireAll and Within are
//     refused.
//   - Windows and "@every" are refused.
//
// The action runs Target.Command[0] with the rest of Command and Args as
// its arguments, quoted the way Windows programs split a command line.
//
//ds:def id=schedexport-taskscheduler-xtvrpdcn owner=@khanakia stability=stable
func TaskScheduler(s workflow.Schedule, t Target, o TaskSchedulerOptions) (File, error) {
	const f = FormatTaskScheduler
	j, err := taskPrepare(s, t)
	if err != nil {
		return File{}, err
	}
	if o.Zone == nil {
		return File{}, invalid(f, "TaskSchedulerOptions.Zone is required: name the zone the machine runs in")
	}
	if !sameZone(s.Location, o.Zone) {
		return File{}, refuse(f, "Location", "zone %s: a calendar trigger fires on the machine's own zone, %s", s.Location, o.Zone)
	}
	if err := j.noWindows(f); err != nil {
		return File{}, err
	}
	fl, err := j.fields(f)
	if err != nil {
		return File{}, err
	}
	sched, err := taskDays(fl)
	if err != nil {
		return File{}, err
	}
	times, err := taskTimes(fl)
	if err != nil {
		return File{}, err
	}
	from, until, err := taskBounds(s, fl, o.Zone)
	if err != nil {
		return File{}, err
	}
	var triggers strings.Builder
	for _, tt := range times {
		start := startBoundary(from, tt.at, o.Zone)
		triggers.WriteString("    <CalendarTrigger>\n      <StartBoundary>" + start + "</StartBoundary>\n")
		if !until.IsZero() {
			triggers.WriteString("      <EndBoundary>" + until.Add(-time.Second).In(o.Zone).Format("2006-01-02T15:04:05") + "</EndBoundary>\n")
		}
		if tt.every > 0 {
			triggers.WriteString("      <Repetition>\n        <Interval>" + isoDuration(tt.every) + "</Interval>\n        <Duration>" + isoDuration(tt.span) + "</Duration>\n      </Repetition>\n")
		}
		triggers.WriteString(sched)
		triggers.WriteString("    </CalendarTrigger>\n")
	}
	return j.task(triggers.String())
}

// TaskSchedulerNextFire writes a task with one TimeTrigger at the
// schedule's next fire after clk's time (inside Active, Windows and the
// year field), as a UTC instant, so it needs no zone. A desktop app
// registers it before it quits and writes a new one each time it runs.
// The overlap and misfire policies map as for TaskScheduler. It returns
// ErrNoNextFire when the schedule never fires again.
//
//ds:def id=schedexport-taskschedulernextfire-9kprgebd owner=@khanakia stability=stable
func TaskSchedulerNextFire(s workflow.Schedule, t Target, clk clock.Clock) (File, error) {
	j, err := taskPrepare(s, t)
	if err != nil {
		return File{}, err
	}
	next, err := j.nextFire(clk)
	if err != nil {
		return File{}, err
	}
	return j.task("    <TimeTrigger>\n      <StartBoundary>" + next.UTC().Format("2006-01-02T15:04:05Z") + "</StartBoundary>\n    </TimeTrigger>\n")
}

func taskPrepare(s workflow.Schedule, t Target) (job, error) {
	const f = FormatTaskScheduler
	j, err := prepare(f, s, t)
	if err != nil {
		return job{}, err
	}
	if !taskName.MatchString(j.name) {
		return job{}, invalid(f, "task name %q: use letters, digits, spaces, '.', '_' and '-'", j.name)
	}
	if len(t.Command) == 0 || t.Command[0] == "" {
		return job{}, invalid(f, "Target.Command is required")
	}
	return j, nil
}

// task writes the task XML around the given triggers.
func (j job) task(triggers string) (File, error) {
	const f = FormatTaskScheduler
	m, err := classify(f, j.s.Misfire)
	if err != nil {
		return File{}, err
	}
	if m == misfireDeadline {
		return File{}, refuse(f, "Misfire", "Within %s: StartWhenAvailable runs a missed start however late", j.s.Misfire.Within)
	}
	instances := "Parallel"
	if j.s.Overlap == workflow.OverlapSkip {
		instances = "IgnoreNew"
	}
	argv := j.target.argv()
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">` + "\n")
	b.WriteString("  <RegistrationInfo>\n    <Description>" + xmlText(j.desc) + "</Description>\n    <URI>\\" + xmlText(j.name) + "</URI>\n  </RegistrationInfo>\n")
	b.WriteString("  <Triggers>\n" + triggers + "  </Triggers>\n")
	b.WriteString("  <Settings>\n    <MultipleInstancesPolicy>" + instances + "</MultipleInstancesPolicy>\n")
	b.WriteString("    <StartWhenAvailable>" + strconv.FormatBool(m == misfireCatchUp) + "</StartWhenAvailable>\n")
	b.WriteString("    <Enabled>true</Enabled>\n  </Settings>\n")
	b.WriteString("  <Actions Context=\"Author\">\n    <Exec>\n      <Command>" + xmlText(argv[0]) + "</Command>\n")
	if len(argv) > 1 {
		quoted := make([]string, len(argv)-1)
		for i, a := range argv[1:] {
			quoted[i] = windowsArg(a)
		}
		b.WriteString("      <Arguments>" + xmlText(strings.Join(quoted, " ")) + "</Arguments>\n")
	}
	b.WriteString("    </Exec>\n  </Actions>\n</Task>\n")
	return File{Name: j.name + ".xml", Data: []byte(b.String())}, nil
}

// taskTime is one trigger's time of day in seconds, with an optional
// repetition every `every` seconds for `span` seconds.
type taskTime struct{ at, every, span int }

// taskTimes turns the time fields into trigger times. A Repetition's
// Duration is set half an interval past the last time, so the last time
// fires and the one after it does not whether Task Scheduler counts the
// end inclusively or not.
//
//ds:def id=schedexport-tasktimes-fvefju5j owner=@khanakia stability=stable
func taskTimes(fl cron.Fields) ([]taskTime, error) {
	var secs []int
	for _, h := range fl.Hour {
		for _, m := range fl.Minute {
			for _, s := range fl.Second {
				secs = append(secs, h*3600+m*60+s)
			}
		}
	}
	if len(secs) == 1 {
		return []taskTime{{at: secs[0]}}, nil
	}
	if sp := spans(secs); len(sp) == 1 && sp[0].step >= 60 {
		return []taskTime{{at: sp[0].lo, every: sp[0].step, span: sp[0].hi - sp[0].lo + sp[0].step/2}}, nil
	}
	if len(secs) > maxTriggers {
		return nil, refuse(FormatTaskScheduler, "Cron", "it needs %d triggers, more than %d, and its times are not one repetition of at least a minute", len(secs), maxTriggers)
	}
	out := make([]taskTime, len(secs))
	for i, s := range secs {
		out[i] = taskTime{at: s}
	}
	return out, nil
}

// taskDays writes the ScheduleBy element for the day and month fields.
//
//ds:def id=schedexport-taskdays-6jx7h8xt owner=@khanakia stability=stable
func taskDays(fl cron.Fields) (string, error) {
	const f = FormatTaskScheduler
	domFull, dowFull, monthFull := full(fl.DayOfMonth, 1, 31), full(fl.DayOfWeek, 0, 6), full(fl.Month, 1, 12)
	everyDay := (fl.DayOr && (domFull || dowFull)) || (domFull && dowFull)
	switch {
	case everyDay && monthFull:
		return "      <ScheduleByDay>\n        <DaysInterval>1</DaysInterval>\n      </ScheduleByDay>\n", nil
	case everyDay:
		return byMonth(allDays(), fl.Month), nil
	case fl.DayOr:
		return "", refuse(f, "day-of-month", "either day may match; a trigger per day field would start the task twice on a day matching both")
	case domFull && monthFull:
		return "      <ScheduleByWeek>\n        <WeeksInterval>1</WeeksInterval>\n" + daysOfWeek(fl.DayOfWeek) + "      </ScheduleByWeek>\n", nil
	case domFull:
		var b strings.Builder
		b.WriteString("      <ScheduleByMonthDayOfWeek>\n        <Weeks>\n")
		for _, w := range []string{"1", "2", "3", "4", "Last"} {
			b.WriteString("          <Week>" + w + "</Week>\n")
		}
		b.WriteString("        </Weeks>\n" + daysOfWeek(fl.DayOfWeek) + months(fl.Month) + "      </ScheduleByMonthDayOfWeek>\n")
		return b.String(), nil
	case dowFull:
		return byMonth(fl.DayOfMonth, fl.Month), nil
	}
	return "", refuse(f, "day-of-month", "both day fields are restricted and must both match; no schedule ANDs a date with a weekday")
}

func byMonth(days, ms []int) string {
	var b strings.Builder
	b.WriteString("      <ScheduleByMonth>\n        <DaysOfMonth>\n")
	for _, d := range days {
		b.WriteString("          <Day>" + strconv.Itoa(d) + "</Day>\n")
	}
	b.WriteString("        </DaysOfMonth>\n" + months(ms) + "      </ScheduleByMonth>\n")
	return b.String()
}

func daysOfWeek(days []int) string {
	var b strings.Builder
	b.WriteString("        <DaysOfWeek>\n")
	for _, d := range days {
		b.WriteString("          <" + time.Weekday(d).String() + " />\n")
	}
	b.WriteString("        </DaysOfWeek>\n")
	return b.String()
}

func months(ms []int) string {
	var b strings.Builder
	b.WriteString("        <Months>\n")
	for _, m := range ms {
		b.WriteString("          <" + time.Month(m).String() + " />\n")
	}
	b.WriteString("        </Months>\n")
	return b.String()
}

// taskBounds is the period the triggers are live: Active narrowed by a
// contiguous year field. Zero ends mean no bound.
//
//ds:def id=schedexport-taskbounds-up2zj8uf owner=@khanakia stability=stable
func taskBounds(s workflow.Schedule, fl cron.Fields, zone *time.Location) (from, until time.Time, err error) {
	from, until = s.Active.From, s.Active.Until
	if fl.Year != nil {
		first, last := fl.Year[0], fl.Year[len(fl.Year)-1]
		if last-first+1 != len(fl.Year) {
			return from, until, refuse(FormatTaskScheduler, "year", "years %v have gaps; a task has one start and one end", fl.Year)
		}
		if y := time.Date(first, time.January, 1, 0, 0, 0, 0, zone); from.IsZero() || y.After(from) {
			from = y
		}
		if y := time.Date(last+1, time.January, 1, 0, 0, 0, 0, zone); until.IsZero() || y.Before(until) {
			until = y
		}
	}
	return from, until, nil
}

// startBoundary is the local date and time of a trigger at `at` seconds
// into the day: on taskBase without a start, else on the start's date, or
// the day after when that time of day is before the start.
func startBoundary(from time.Time, at int, zone *time.Location) string {
	h, m, sec := at/3600, at/60%60, at%60
	if from.IsZero() {
		return time.Date(taskBase[0], time.Month(taskBase[1]), taskBase[2], h, m, sec, 0, time.UTC).Format("2006-01-02T15:04:05")
	}
	local := from.In(zone)
	t := time.Date(local.Year(), local.Month(), local.Day(), h, m, sec, 0, zone)
	if t.Before(from) {
		t = time.Date(local.Year(), local.Month(), local.Day()+1, h, m, sec, 0, zone)
	}
	return t.Format("2006-01-02T15:04:05")
}

// isoDuration writes seconds as an XML duration ("PT15M", "P1D").
func isoDuration(sec int) string {
	if sec == 86400 {
		return "P1D"
	}
	out := "PT"
	if h := sec / 3600; h > 0 {
		out += strconv.Itoa(h) + "H"
	}
	if m := sec / 60 % 60; m > 0 {
		out += strconv.Itoa(m) + "M"
	}
	if s := sec % 60; s > 0 || out == "PT" {
		out += strconv.Itoa(s) + "S"
	}
	return out
}

// windowsArg quotes one argument the way CommandLineToArgvW splits it:
// wrapped in double quotes when empty or holding a space, tab or quote,
// with a quote escaped by a backslash and backslashes before a quote (or
// the closing quote) doubled.
func windowsArg(s string) string {
	if s != "" && !strings.ContainsAny(s, " \t\"") {
		return s
	}
	var b strings.Builder
	b.WriteByte('"')
	slashes := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\':
			slashes++
		case '"':
			b.WriteString(strings.Repeat(`\`, slashes+1))
			slashes = 0
		default:
			slashes = 0
		}
		b.WriteByte(c)
	}
	b.WriteString(strings.Repeat(`\`, slashes))
	b.WriteByte('"')
	return b.String()
}
