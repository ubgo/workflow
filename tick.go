package workflow

import (
	"context"
	"fmt"
	"time"
)

// LabelSchedule is the label drivers with CapTickEvents set on every run
// a schedule fires, to the schedule's ScheduleID, so a run-finished
// observer can tell which schedule a run came from (AttachScheduleHooks
// relies on it). It is under the reserved LabelPrefixInternal, so an app
// cannot set it by accident.
const LabelSchedule = LabelPrefixInternal + "schedule"

// LabelScheduleName is set beside LabelSchedule to the schedule's Name
// (for a declared cron, its expression), so a run-finished observer on
// any node can match a run to a named schedule without a lookup.
const LabelScheduleName = LabelPrefixInternal + "schedule-name"

// FiredLabels returns the labels of a run a schedule fires: a copy of the
// schedule's labels plus LabelSchedule and LabelScheduleName. Drivers with
// CapTickEvents build every fired job's labels with it.
func FiredLabels(labels map[string]string, id ScheduleID, name string) map[string]string {
	out := make(map[string]string, len(labels)+2)
	for k, v := range labels {
		out[k] = v
	}
	out[LabelSchedule] = string(id)
	out[LabelScheduleName] = name
	return out
}

// Window is a time-of-day range on the wall clock of a schedule's zone,
// "HH:MM" each (24-hour). From is inclusive and To exclusive; a To before
// From wraps midnight ("22:00" to "06:00" is the night). From equal to To
// is refused, because it would mean either nothing or the whole day.
type Window struct {
	From, To string
}

// Windows limits a schedule's ticks by time of day: a tick fires only inside at least one Between window
// (no Between windows: at any time) and outside every Unless window.
type Windows struct {
	Between []Window
	Unless  []Window
}

// IsZero reports whether w limits nothing.
func (w Windows) IsZero() bool { return len(w.Between) == 0 && len(w.Unless) == 0 }

// Allows reports whether t, read on loc's wall clock (UTC when loc is
// nil), passes w. Windows that fail validation never match. It is a pure
// function, so every driver applies windows the same way.
func (w Windows) Allows(t time.Time, loc *time.Location) bool {
	if loc == nil {
		loc = time.UTC
	}
	local := t.In(loc)
	m := local.Hour()*60 + local.Minute()
	if len(w.Between) > 0 {
		in := false
		for _, win := range w.Between {
			if win.contains(m) {
				in = true
				break
			}
		}
		if !in {
			return false
		}
	}
	for _, win := range w.Unless {
		if win.contains(m) {
			return false
		}
	}
	return true
}

// contains reports whether minute-of-day m falls in the window.
func (w Window) contains(m int) bool {
	from, err1 := parseHHMM(w.From)
	to, err2 := parseHHMM(w.To)
	if err1 != nil || err2 != nil || from == to {
		return false
	}
	if from < to {
		return m >= from && m < to
	}
	return m >= from || m < to // wraps midnight
}

// parseHHMM reads "HH:MM" into minutes after midnight.
func parseHHMM(s string) (int, error) {
	t, err := time.Parse("15:04", s)
	if err != nil || len(s) != len("15:04") {
		return 0, fmt.Errorf("%q is not a time of day in HH:MM", s)
	}
	return t.Hour()*60 + t.Minute(), nil
}

// validateWindows checks every window; the caller names where they sit.
func validateWindows(w Windows) error {
	for _, list := range [][]Window{w.Between, w.Unless} {
		for _, win := range list {
			from, err := parseHHMM(win.From)
			if err != nil {
				return fmt.Errorf("window From: %w", err)
			}
			to, err := parseHHMM(win.To)
			if err != nil {
				return fmt.Errorf("window To: %w", err)
			}
			if from == to {
				return fmt.Errorf("window %s-%s is empty: From equals To", win.From, win.To)
			}
		}
	}
	return nil
}

// TickOutcome is what happened to one schedule tick.
type TickOutcome string

// The tick outcomes.
const (
	// TickFired: the tick enqueued a run (Tick.Run), or tried to and the
	// enqueue failed (Tick.Err).
	TickFired TickOutcome = "fired"

	// TickSkipped: the tick was due and on time but did not fire;
	// Tick.Reason says why.
	TickSkipped TickOutcome = "skipped"

	// TickMisfired: the scheduler woke late and its misfire policy
	// dropped Tick.Missed ticks, which a scheduler that looks only at the
	// current minute cannot even see.
	TickMisfired TickOutcome = "misfired"
)

// The reasons of a TickSkipped.
const (
	TickReasonWhen   = "when"   // Workflow.ScheduleWhen returned false
	TickReasonWindow = "window" // the tick fell outside the schedule's Windows
	TickReasonPaused = "paused" // the schedule was paused (SchedulePauser)
	// TickReasonOverlap (overlap.go): OverlapSkip held the tick back.
)

// Tick reports one tick of a schedule, or, for TickMisfired, one batch of
// dropped ticks.
type Tick struct {
	// ScheduleID is the schedule. For a declared Workflow.Cron entry it
	// is the driver's stable id for that declaration, the same one
	// ScheduleLister reports.
	ScheduleID ScheduleID

	// Name is the Schedule.Name; for a declared cron, the expression.
	Name string

	// Workflow is the workflow the schedule runs.
	Workflow string

	// Declared is true for a Workflow.Cron entry.
	Declared bool

	// At is the tick's time; for TickMisfired, the newest dropped tick.
	At time.Time

	// Location is the zone the schedule is evaluated in.
	Location *time.Location

	// Outcome is what happened.
	Outcome TickOutcome

	// Reason is set for TickSkipped: TickReasonWhen, TickReasonWindow or
	// TickReasonPaused.
	Reason string

	// Missed is how many ticks a TickMisfired dropped.
	Missed int

	// Run is the run a TickFired enqueued.
	Run RunRef

	// Err is why a TickFired could not enqueue.
	Err error

	// Labels are the schedule's labels. Observers must not modify them.
	Labels map[string]string

	// Manual is true for a TickFired that ScheduleTriggerer started by
	// hand; At is then the driver's clock at the trigger.
	Manual bool
}

// TickObserver observes ticks. It must be idempotent, because delivery is
// at least once.
type TickObserver func(ctx context.Context, t Tick)

// TickNotifier is an optional interface a client implements to report
// every tick outcome: fired, skipped and misfired. A client implements it when it
// declares CapTickEvents. The contract is RunNotifier's: register before
// Start (ErrAlreadyStarted after), observers run on a driver goroutine in
// registration order, panics are recovered, delivery is at least once and
// may be lost on a crash. Ticks are reported only by the node that fired
// or skipped them.
type TickNotifier interface {
	OnTick(fn TickObserver) error
}

// Plan is Due plus how many ticks the policy dropped, so a driver can
// report TickMisfired. The fire half is exactly Due's answer. The count
// covers ticks in (last, now] the policy did not fire, scanning in chunks
// so a long outage of a frequent cron costs bounded memory; it is capped
// at maxCountedTicks. The zero policy returns nil and 0, as Due does.
func (m Misfire) Plan(src TickSource, last, now time.Time, loc *time.Location) (fire []time.Time, dropped int) {
	fire = m.Due(src, last, now, loc)
	if m.IsZero() || !now.After(last) {
		return fire, 0
	}
	total := countTicks(src, last, now, loc)
	if total > len(fire) {
		dropped = total - len(fire)
	}
	return fire, dropped
}

// maxCountedTicks caps how many missed ticks Plan counts, so a
// once-a-second cron down for a year does not scan for a minute.
const maxCountedTicks = 1 << 20

// countTicks counts ticks of src in (from, to], at most maxCountedTicks.
func countTicks(src TickSource, from, to time.Time, loc *time.Location) int {
	n := 0
	for n < maxCountedTicks {
		chunk := src.Between(from, to, loc, tickScanChunk)
		n += len(chunk)
		if len(chunk) < tickScanChunk || !chunk[len(chunk)-1].After(from) {
			break
		}
		from = chunk[len(chunk)-1]
	}
	return min(n, maxCountedTicks)
}

// LastTick returns the newest tick of src in (from, to], or zero when
// there is none. Drivers use it to fill Tick.At of a TickMisfired.
func LastTick(src TickSource, from, to time.Time, loc *time.Location) time.Time {
	t := newestTicks(src, from, to, loc, 1)
	if len(t) == 0 {
		return time.Time{}
	}
	return t[0]
}

// ScheduleInfo is one live schedule as ScheduleLister reports it.
type ScheduleInfo struct {
	// ID is the schedule. A declared cron has the driver's stable id for
	// that declaration.
	ID ScheduleID

	// Name is the Schedule.Name; for a declared cron, the expression.
	Name string

	// Workflow is the workflow the schedule runs.
	Workflow string

	// Cron is the expression.
	Cron string

	// Location is the zone it is evaluated in.
	Location *time.Location

	// Declared is true for a Workflow.Cron entry.
	Declared bool

	// Paused is true while SchedulePauser holds it.
	Paused bool

	// Next is the next fire time in Location; zero when the expression
	// never fires again. It ignores windows and ScheduleWhen, which are
	// decided at tick time.
	Next time.Time

	// Misfire is the schedule's misfire policy.
	Misfire Misfire

	// Windows are the schedule's time-of-day windows.
	Windows Windows

	// Labels are the schedule's labels. Callers must not modify them.
	Labels map[string]string

	// Overlap is the schedule's overlap policy (for a declared cron,
	// Workflow.CronOverlap); empty means OverlapAllow.
	Overlap OverlapPolicy

	// Last is the schedule's most recent fire, filled by drivers with
	// CapScheduleHistory; zero otherwise.
	Last ScheduleLast

	// Seconds is Schedule.Seconds (Workflow.CronSeconds for a declared
	// cron).
	Seconds bool

	// Active is the schedule's period (Workflow.CronActive for a declared
	// cron). Next already respects it.
	Active Active

	// KeepRuns is Schedule.KeepRuns (Workflow.CronKeepRuns).
	KeepRuns int

	// Tenant is Schedule.Tenant; empty for a declared cron.
	Tenant string
}

// ScheduleLister lists the live schedules with their next fire time,
// declared and made at run time, sorted by Workflow, then Name, then ID.
// Unscheduled ones are not listed. A client implements it when it
// declares CapScheduleList.
type ScheduleLister interface {
	Schedules(ctx context.Context) ([]ScheduleInfo, error)
}

// ScheduleTarget names a schedule for SchedulePauser, or all of them.
// Exactly one field is set.
type ScheduleTarget struct {
	ID  ScheduleID
	All bool
}

// ValidateScheduleTarget refuses a target with neither or both fields
// set. Errors wrap ErrInvalid.
func ValidateScheduleTarget(t ScheduleTarget) error {
	if t.ID.IsZero() == !t.All {
		return invalidf("schedule target must set exactly one of ID and All")
	}
	return nil
}

// SchedulePauser stops schedules firing without removing them, and
// resumes them. A client implements it when it
// declares CapSchedulePause.
//
// A paused schedule's ticks do not fire and are reported as TickSkipped
// with TickReasonPaused. A tick that fell due during the pause never
// fires, not even on resume and whatever the misfire policy (a pause
// means "not now", not "later"): the schedule resumes at its next
// tick, so each tick is reported once. The misfire policy still governs
// ticks missed while nothing was running. Pausing All pauses every
// schedule, including ones made after
// the call; ResumeSchedule with All clears every pause. Pause and resume
// are idempotent; an unknown ID returns ErrNotFound. On a durable driver
// the pause is stored.
type SchedulePauser interface {
	PauseSchedule(ctx context.Context, t ScheduleTarget) error
	ResumeSchedule(ctx context.Context, t ScheduleTarget) error
}
