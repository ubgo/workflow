package schedexport

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

// ErrNotExpressible is matched by every refusal of a schedule a format
// cannot express exactly. The concrete error is a *NotExpressibleError
// naming the format, the field and the reason.
//
//ds:def id=schedexport-errnotexpressible-6gamr9aa owner=@khanakia stability=stable
var ErrNotExpressible = errors.New("schedexport: not expressible in this format")

// ErrNoNextFire is returned by the NextFire forms when the schedule never
// fires again after the clock's time: its Active period or its last year
// has passed, or no tick passes its windows.
//
//ds:def id=schedexport-errnonextfire-2pu4m79k owner=@khanakia stability=stable
var ErrNoNextFire = errors.New("schedexport: the schedule never fires again")

// Format names an output format in a NotExpressibleError.
//
//ds:def id=schedexport-format-n224t5n3 owner=@khanakia stability=stable
type Format string

// The formats.
const (
	//ds:def id=schedexport-formatkubernetes-w8hwdk4e owner=@khanakia stability=stable
	FormatKubernetes Format = "kubernetes"
	//ds:def id=schedexport-formatsystemd-64bz5hh3 owner=@khanakia stability=stable
	FormatSystemd Format = "systemd"
	//ds:def id=schedexport-formatlaunchd-pbkgytm7 owner=@khanakia stability=stable
	FormatLaunchd Format = "launchd"
	//ds:def id=schedexport-formattaskscheduler-dfgxw7y7 owner=@khanakia stability=stable
	FormatTaskScheduler Format = "taskscheduler"
)

// NotExpressibleError reports the part of a schedule a format cannot
// express, so the caller can change that field (or pick another format)
// instead of shipping a file that fires at other times than the driver
// would.
//
//ds:def id=schedexport-notexpressibleerror-nwwc7ap8 owner=@khanakia stability=stable
type NotExpressibleError struct {
	Format Format
	// Field is the Schedule field ("Cron", "Location", "Misfire",
	// "Overlap", "Windows", "Active") or, for a cron field, its name
	// ("second", "day-of-month", "year").
	Field  string
	Reason string
}

// Error implements error.
func (e *NotExpressibleError) Error() string {
	return fmt.Sprintf("schedexport: %s cannot express %s: %s", e.Format, e.Field, e.Reason)
}

// Unwrap makes the error match ErrNotExpressible.
func (e *NotExpressibleError) Unwrap() error { return ErrNotExpressible }

func refuse(f Format, field, reason string, args ...any) error {
	return &NotExpressibleError{Format: f, Field: field, Reason: fmt.Sprintf(reason, args...)}
}

func invalid(f Format, format string, args ...any) error {
	return fmt.Errorf("%w: schedexport: %s: "+format, append([]any{workflow.ErrInvalid, f}, args...)...)
}

// File is one exported file: a suggested file name and its contents.
//
//ds:def id=schedexport-file-utggyyut owner=@khanakia stability=stable
type File struct {
	// Name is the file name the format expects ("report.timer",
	// "com.example.report.plist"); the directory is the app's choice.
	Name string
	// Data is the file's contents.
	Data []byte
}

// Target is what an exported schedule runs, and what its files are
// called.
//
//ds:def id=schedexport-target-htxk8djj owner=@khanakia stability=stable
type Target struct {
	// Name names the job, unit, label or task. Empty means the
	// Schedule's Name. Each format checks it against its own rules.
	Name string

	// Description is a human-readable line where the format has one.
	// Empty means a default made from the name.
	Description string

	// Command is the program and its leading arguments. It is required
	// by the OS formats; for a CronJob it overrides the image's
	// entrypoint and may be empty.
	Command []string

	// Args are further arguments (a CronJob's container args).
	Args []string

	// Parser reads Schedule.Cron when Schedule.Seconds is false, as a
	// driver's WithCronParser does (cron.ParseYear for a year field).
	// Nil means cron.Parse. A schedule from another parser is exported
	// when it offers its fields (cron.FieldsOf); the NextFire forms need
	// only its Next.
	Parser cron.Parser
}

// argv is Command followed by Args.
func (t Target) argv() []string {
	return append(append([]string(nil), t.Command...), t.Args...)
}

// job is a validated, parsed schedule ready for a format.
type job struct {
	s      workflow.Schedule
	target Target
	sched  cron.Schedule
	name   string
	desc   string
}

// prepare validates s and t for format f and parses the expression.
//
//ds:def id=schedexport-prepare-w5mrnvgs owner=@khanakia stability=stable
func prepare(f Format, s workflow.Schedule, t Target) (job, error) {
	if err := workflow.ValidateSchedule(s); err != nil {
		return job{}, fmt.Errorf("schedexport: %s: %w", f, err)
	}
	name := t.Name
	if name == "" {
		name = s.Name
	}
	if name == "" {
		return job{}, invalid(f, "no name: set Target.Name or Schedule.Name")
	}
	for _, v := range append([]string{name, t.Description}, t.argv()...) {
		if i := strings.IndexFunc(v, isControl); i >= 0 {
			return job{}, invalid(f, "control character %q in %q", v[i], v)
		}
	}
	sched, err := workflow.ParseCron(s.Cron, s.Seconds, t.Parser)
	if err != nil {
		return job{}, fmt.Errorf("%w: schedexport: %s: %w", workflow.ErrInvalid, f, err)
	}
	if sched == nil {
		return job{}, invalid(f, "the parser returned no schedule for %q", s.Cron)
	}
	desc := t.Description
	if desc == "" {
		desc = "Schedule " + name
	}
	return job{s: s, target: t, sched: sched, name: name, desc: desc}, nil
}

func isControl(r rune) bool { return r < 0x20 || r == 0x7f }

// fields returns the schedule's cron fields, refusing what a calendar
// format cannot read: an "@every" interval and a parser's schedule that
// offers none. A year field matching every year is dropped: it differs
// from no year field only after cron.MaxYear.
func (j job) fields(f Format) (cron.Fields, error) {
	fl, ok := cron.FieldsOf(j.sched)
	if !ok {
		return cron.Fields{}, refuse(f, "Cron", "%q has no calendar fields (an @every interval counts from the Unix epoch, and a custom parser's schedule offers none); write it as a cron expression", j.s.Cron)
	}
	if len(fl.Year) == cron.MaxYear-cron.MinYear+1 {
		fl.Year = nil
	}
	return fl, nil
}

// noWindows refuses time-of-day windows: no format has them.
//
//ds:def id=schedexport-job-nowindows-bmfak65q owner=@khanakia stability=stable
func (j job) noWindows(f Format) error {
	if !j.s.Windows.IsZero() {
		return refuse(f, "Windows", "it has no time-of-day windows; drop them or check the time in the command")
	}
	return nil
}

// zoneName returns the IANA name of loc for a format that names the
// zone, "" for UTC. A zone that time.LoadLocation cannot find again by
// its name (time.FixedZone) is refused: the other scheduler only reads
// names.
//
//ds:def id=schedexport-zonename-ufzk25yh owner=@khanakia stability=stable
func zoneName(f Format, loc *time.Location) (string, error) {
	if workflow.IsUTC(loc) {
		return "", nil
	}
	name := loc.String()
	if _, err := time.LoadLocation(name); err != nil || name == "Local" {
		return "", refuse(f, "Location", "zone %q is not an IANA zone name", name)
	}
	return name, nil
}

// sameZone reports whether a and b evaluate wall times alike, by name.
func sameZone(a, b *time.Location) bool {
	if workflow.IsUTC(a) || workflow.IsUTC(b) {
		return workflow.IsUTC(a) && workflow.IsUTC(b)
	}
	return a.String() == b.String()
}

// maxNextScan bounds how many ticks nextFire skips while looking for one
// inside the schedule's windows.
const maxNextScan = 100000

// nextFire is the schedule's next fire strictly after the clock's time:
// inside its Active period and its windows, as a driver would fire it.
//
//ds:def id=schedexport-job-nextfire-vhxvmqtq owner=@khanakia stability=stable
func (j job) nextFire(clk clock.Clock) (time.Time, error) {
	if clk == nil {
		clk = clock.Real{}
	}
	after := clk.Now()
	for range maxNextScan {
		t := workflow.NextActive(j.sched, after, j.s.Location, j.s.Active)
		if t.IsZero() {
			return time.Time{}, fmt.Errorf("%w: %q after %s", ErrNoNextFire, j.s.Cron, after.Format(time.RFC3339))
		}
		if j.s.Windows.Allows(t, j.s.Location) {
			return t.In(j.s.Location), nil
		}
		after = t
	}
	return time.Time{}, fmt.Errorf("%w: no tick of %q passes its windows in %d ticks", ErrNoNextFire, j.s.Cron, maxNextScan)
}

// span is the values lo, lo+step, ..., hi of a field.
type span struct{ lo, hi, step int }

// spans compresses an ascending list into runs: consecutive values become
// one span, and three or more values with an equal gap become a stepped
// span; the rest stay single.
func spans(vals []int) []span {
	var out []span
	for i := 0; i < len(vals); {
		if i+1 < len(vals) {
			d := vals[i+1] - vals[i]
			j := i + 1
			for j+1 < len(vals) && vals[j+1]-vals[j] == d {
				j++
			}
			if d == 1 || j-i >= 2 {
				out = append(out, span{vals[i], vals[j], d})
				i = j + 1
				continue
			}
		}
		out = append(out, span{vals[i], vals[i], 1})
		i++
	}
	return out
}

// full reports whether vals is every value of lo-hi.
func full(vals []int, lo, hi int) bool { return len(vals) == hi-lo+1 }

// itoa2 writes v with at least two digits.
func itoa2(v int) string {
	if v < 10 && v >= 0 {
		return "0" + strconv.Itoa(v)
	}
	return strconv.Itoa(v)
}

// misfire is a schedule's misfire policy in the terms the formats share.
type misfire int

const (
	misfireDefault  misfire = iota // zero policy: the target's own default
	misfireSkip                    // MisfireSkip
	misfireCatchUp                 // MisfireFireOnce without Within
	misfireDeadline                // MisfireFireOnce with Within
)

// classify maps m, refusing MisfireFireAll: no format fires every missed
// tick.
//
//ds:def id=schedexport-classify-a9sc88gr owner=@khanakia stability=stable
func classify(f Format, m workflow.Misfire) (misfire, error) {
	switch m.Policy {
	case "":
		return misfireDefault, nil
	case workflow.MisfireSkip:
		return misfireSkip, nil
	case workflow.MisfireFireOnce:
		if m.Within > 0 {
			return misfireDeadline, nil
		}
		return misfireCatchUp, nil
	}
	return 0, refuse(f, "Misfire", "policy %q: it starts at most one run for the ticks it missed", m.Policy)
}
