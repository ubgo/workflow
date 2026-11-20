package workflow

import (
	"context"
	"fmt"
)

// OverlapMode is what GuardOverlap does when a schedule fires while runs
// it fired earlier are unfinished.
type OverlapMode string

const (
	// OverlapReplace cancels the schedule's unfinished earlier runs once
	// the new one is enqueued (CR-05): the newest tick replaces the
	// running one. Cancellation is the driver's Cancel, so a running step
	// sees its ctx cancelled and stops when it honours it.
	OverlapReplace OverlapMode = "replace"

	// OverlapBufferOne keeps at most one run waiting behind the running
	// one (CR-04): a new run is cancelled at once when two earlier runs
	// of the schedule are unfinished (one running, one waiting). Pair it
	// with Concurrency{MaxRuns: 1} on the workflow (keyed by the schedule
	// when the workflow has several), so the buffered run waits instead
	// of starting beside the running one.
	OverlapBufferOne OverlapMode = "buffer_one"
)

// OverlapRule picks the schedules a mode applies to: by schedule name, by
// workflow (every schedule and declared cron of it), or both (both must
// match). At least one is required.
type OverlapRule struct {
	Name     string
	Workflow string
	Mode     OverlapMode
}

// overlapScan bounds how many of a schedule's newest runs GuardOverlap
// reads per fire.
const overlapScan = 100

// GuardOverlap applies replace and buffer-one overlap to the schedules
// rules pick. Schedule.Overlap keeps OverlapAllow (or OverlapSkip, which
// the driver applies first; a skipped tick fires nothing to guard). It
// observes every fired tick (TickNotifier), lists the schedule's runs by
// LabelSchedule (RunQuerier) and cancels by the rule; a tick fired by
// hand (TriggerSchedule) is treated like any other.
//
// It needs CapTickEvents, CapRunQuery and CapCancel (memory, sqlstore,
// Redis; Hatchet reports no ticks) and refuses with an UnsupportedError
// otherwise. Call it before Start, as every tick observer. The guard acts
// just after the fire, so for a moment the old and the new run both
// exist; a process that dies in between leaves both, and the next fire
// applies the rule again. Errors (a failed list or cancel) go to onErr;
// nil drops them.
func GuardOverlap(c Client, onErr func(error), rules ...OverlapRule) error {
	if len(rules) == 0 {
		return invalidf("GuardOverlap needs at least one rule")
	}
	for _, r := range rules {
		if r.Name == "" && r.Workflow == "" {
			return invalidf("overlap rule needs a Name or a Workflow")
		}
		if r.Mode != OverlapReplace && r.Mode != OverlapBufferOne {
			return invalidf("overlap rule has unknown mode %q", r.Mode)
		}
	}
	for _, need := range []Caps{CapTickEvents, CapRunQuery, CapCancel} {
		if !c.Capabilities().Has(need) {
			return &UnsupportedError{Field: "GuardOverlap", Missing: need, Reason: "it watches fired ticks, lists the schedule's runs and cancels them"}
		}
	}
	tn, ok := c.(TickNotifier)
	if !ok {
		return &UnsupportedError{Field: "GuardOverlap", Missing: CapTickEvents, Reason: "the client does not implement workflow.TickNotifier"}
	}
	rq, ok := c.(RunQuerier)
	if !ok {
		return &UnsupportedError{Field: "GuardOverlap", Missing: CapRunQuery, Reason: "the client does not implement workflow.RunQuerier"}
	}
	report := func(err error) {
		if onErr != nil && err != nil {
			onErr(err)
		}
	}
	return tn.OnTick(func(ctx context.Context, t Tick) {
		if t.Outcome != TickFired || t.Run.IsZero() {
			return
		}
		mode, ok := overlapModeFor(rules, t)
		if !ok {
			return
		}
		page, err := rq.Runs(ctx, RunQuery{Workflow: t.Workflow, Labels: map[string]string{LabelSchedule: string(t.ScheduleID)}, Limit: overlapScan})
		if err != nil {
			report(fmt.Errorf("workflow: overlap guard: list runs of schedule %s: %w", t.ScheduleID, err))
			return
		}
		// Only runs enqueued before the tick's own run count: observers
		// run after the fire, possibly after later fires, and a later run
		// must never be cancelled for an earlier tick. Runs lists newest
		// first, so the runs after the tick's own are the earlier ones.
		var earlier []RunRef
		seen := false
		for _, info := range page.Runs {
			switch {
			case sameRun(info.Ref, t.Run):
				seen = true
			case seen && !info.Status.IsTerminal():
				earlier = append(earlier, info.Ref)
			}
		}
		if !seen {
			return // the run is gone already (finished and pruned); nothing to guard
		}
		switch mode {
		case OverlapReplace:
			for _, ref := range earlier {
				report(cancelIgnoringEnd(ctx, c, ref, t.ScheduleID))
			}
		case OverlapBufferOne:
			if len(earlier) >= 2 {
				report(cancelIgnoringEnd(ctx, c, t.Run, t.ScheduleID))
			}
		}
	})
}

// overlapModeFor returns the mode of the first rule matching t.
func overlapModeFor(rules []OverlapRule, t Tick) (OverlapMode, bool) {
	for _, r := range rules {
		if (r.Name == "" || r.Name == t.Name) && (r.Workflow == "" || r.Workflow == t.Workflow) {
			return r.Mode, true
		}
	}
	return "", false
}

// sameRun reports whether a and b name one run, by whichever IDs both
// carry.
func sameRun(a, b RunRef) bool {
	if !a.RunID.IsZero() && !b.RunID.IsZero() {
		return a.RunID == b.RunID
	}
	return !a.ScheduledID.IsZero() && a.ScheduledID == b.ScheduledID
}

// cancelIgnoringEnd cancels ref; a run that ended meanwhile is fine.
func cancelIgnoringEnd(ctx context.Context, c Client, ref RunRef, id ScheduleID) error {
	if err := c.Cancel(ctx, ref); err != nil {
		return fmt.Errorf("workflow: overlap guard: cancel run %v of schedule %s: %w", ref, id, err)
	}
	return nil
}
