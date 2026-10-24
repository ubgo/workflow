package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// OverlapPolicy says what a schedule does with a tick that falls due
// while the run its previous fire started is still unfinished.
type OverlapPolicy string

// The overlap policies.
const (
	// OverlapAllow fires every tick whatever the previous run is doing.
	// It is the default (the zero value means it too).
	OverlapAllow OverlapPolicy = "allow"

	// OverlapSkip skips a tick while the schedule's previous run is not
	// terminal, and reports it as TickSkipped with TickReasonOverlap. The
	// decision reads the previous run's status from the driver's run
	// state, not a lock held by one node, so it holds across a fleet and
	// across a restart on a durable driver. A previous run the driver no
	// longer knows (deleted, pruned) does not block. It needs
	// CapOverlapSkip.
	OverlapSkip OverlapPolicy = "skip"
)

// TickReasonOverlap is the reason of a TickSkipped that OverlapSkip held
// back because the schedule's previous run was unfinished.
const TickReasonOverlap = "overlap"

// ErrOverlap is returned by ScheduleTriggerer.TriggerSchedule when the
// schedule's OverlapPolicy is OverlapSkip and its previous run is still
// unfinished: a manual trigger obeys the same rule as a tick.
var ErrOverlap = errors.New("workflow: the schedule's previous run is unfinished")

// validOverlap reports whether p is a known policy (the zero value
// included).
func validOverlap(p OverlapPolicy) bool {
	return p == "" || p == OverlapAllow || p == OverlapSkip
}

// overlapCaps is the capability p needs.
func overlapCaps(p OverlapPolicy) Caps {
	if p == OverlapSkip {
		return CapOverlapSkip
	}
	return 0
}

// Blocks reports whether p holds back a fire when the schedule's previous
// run has status prev. prev is empty when the schedule never fired or the
// driver no longer knows its previous run; that never blocks. Every
// driver decides overlap with this one function, so the rule cannot
// drift between backends.
func (p OverlapPolicy) Blocks(prev Status) bool {
	return p == OverlapSkip && prev != "" && !prev.IsTerminal()
}

// ScheduleTriggerer runs a live schedule now, outside its cron, with the
// schedule's Input and labels (FiredLabels plus LabelTrigger), as if a
// tick had fired at the driver's current time. A client implements it
// when it declares CapScheduleTrigger.
//
// A trigger obeys the schedule's OverlapPolicy: with OverlapSkip and the
// previous run unfinished it returns ErrOverlap and enqueues nothing. It
// does not consult Windows or ScheduleWhen (an operator asking for a run
// now has already decided), and it fires a paused schedule too (a pause
// stops the cron, not the operator). The triggered run becomes the
// schedule's last run (ScheduleInfo.Last: Fire and Run), so a later tick's
// overlap check sees it, and drivers with CapTickEvents report it as a
// TickFired with Tick.Manual set. The schedule's next cron fire is
// unchanged. An unknown or unscheduled ID returns ErrNotFound.
type ScheduleTriggerer interface {
	TriggerSchedule(ctx context.Context, id ScheduleID) (RunRef, error)
}

// LabelTrigger marks a run a ScheduleTriggerer started by hand, with the
// value LabelTriggerManual, so run listings and observers can tell it
// from a cron fire.
const LabelTrigger = LabelPrefixInternal + "trigger"

// LabelTriggerManual is the value of LabelTrigger.
const LabelTriggerManual = "manual"

// TriggeredLabels is FiredLabels plus LabelTrigger, the labels of a run
// TriggerSchedule starts. Drivers build a triggered job's labels with it.
func TriggeredLabels(labels map[string]string, id ScheduleID, name string) map[string]string {
	out := FiredLabels(labels, id, name)
	out[LabelTrigger] = LabelTriggerManual
	return out
}

// TriggeredLabelsAt is TriggeredLabels plus LabelTickAt set to at, the
// trigger time. Drivers with CapTickTime build a triggered job's labels
// with it.
func TriggeredLabelsAt(labels map[string]string, id ScheduleID, name string, at time.Time) map[string]string {
	out := FiredLabelsAt(labels, id, name, at)
	out[LabelTrigger] = LabelTriggerManual
	return out
}

// ScheduleLast is the bookkeeping of a schedule's most recent fire, cron
// tick or manual trigger alike. Drivers with CapScheduleHistory store it
// with the schedule (durably on a durable driver) and report it in
// ScheduleInfo.
type ScheduleLast struct {
	// Fire is when the schedule last enqueued a run: the tick's time for
	// a cron fire, the driver's clock for a trigger. Zero when it never
	// fired.
	Fire time.Time

	// Run is the run that fire enqueued. Zero when it never fired.
	Run RunRef

	// Status is that run's status when the schedule was listed; empty
	// when it never fired or the driver no longer knows the run (deleted,
	// pruned). It is the "last outcome" once the run is terminal.
	Status Status
}

// overlapError wraps ErrOverlap with the schedule and its previous run.
type overlapError struct {
	id   ScheduleID
	prev RunRef
	st   Status
}

func (e *overlapError) Error() string {
	return fmt.Sprintf("workflow: schedule %s: previous run %s is %s: %v", e.id, e.prev.RunID, e.st, ErrOverlap)
}

func (e *overlapError) Unwrap() error { return ErrOverlap }

// OverlapRefusal returns the error TriggerSchedule gives when the
// overlap policy holds a trigger back. It matches ErrOverlap and names
// the schedule and the unfinished run.
func OverlapRefusal(id ScheduleID, prev RunRef, st Status) error {
	return &overlapError{id: id, prev: prev, st: st}
}
