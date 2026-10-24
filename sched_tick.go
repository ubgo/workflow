package workflow

import "time"

// LabelTickAt is the label a run fired by a schedule carries with the
// tick it represents (its logical time), RFC 3339 with nanoseconds in
// UTC. A job reads it through StepInfo.TickAt or TickTime: a run that
// starts late, or a misfire catch-up that fires three missed days in a
// row, still knows which day it is for, where the wall time it started
// would be wrong. For a manual trigger (ScheduleTriggerer) it is the
// driver's clock at the trigger. It is under the reserved
// LabelPrefixInternal, so an app cannot set it by accident. Drivers with
// CapTickTime set it on every fired run.
const LabelTickAt = LabelPrefixInternal + "tick-at"

// FiredLabelsAt is FiredLabels plus LabelTickAt set to at. Drivers with
// CapTickTime build every fired job's labels with it.
func FiredLabelsAt(labels map[string]string, id ScheduleID, name string, at time.Time) map[string]string {
	out := FiredLabels(labels, id, name)
	out[LabelTickAt] = at.UTC().Format(time.RFC3339Nano)
	return out
}

// TickTime returns the tick a run's labels name (LabelTickAt), in UTC;
// false when the run was not fired by a schedule, or by a driver without
// CapTickTime, or the label does not parse.
func TickTime(labels map[string]string) (time.Time, bool) {
	v, ok := labels[LabelTickAt]
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// TickAt returns the tick this run was fired for (see LabelTickAt); false
// when no schedule fired it.
func (i StepInfo) TickAt() (time.Time, bool) { return TickTime(i.Labels) }
