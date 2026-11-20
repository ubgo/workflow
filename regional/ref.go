package regional

import (
	"maps"
	"strings"

	"github.com/ubgo/workflow"
)

// sep ends the region in a prefixed ID. Region names never hold it
// (validRegion), so the first sep in an ID always ends the region and the
// driver's own ID after it may hold any character.
const sep = ":"

// prefix is what a region puts in front of a driver's IDs.
func prefix(region string) string { return region + sep }

// splitID returns the region named by id's prefix and the driver's own
// ID. ok is false for an id with no prefix.
func splitID(id string) (region, inner string, ok bool) {
	region, inner, ok = strings.Cut(id, sep)
	if !ok || region == "" {
		return "", id, false
	}
	return region, inner, true
}

// encodeRef prefixes every set ID of a driver's ref with region.
func encodeRef(region string, ref workflow.RunRef) workflow.RunRef {
	if !ref.RunID.IsZero() {
		ref.RunID = workflow.RunID(prefix(region) + string(ref.RunID))
	}
	if !ref.ScheduledID.IsZero() {
		ref.ScheduledID = workflow.ScheduledID(prefix(region) + string(ref.ScheduledID))
	}
	return ref
}

// decodeRef returns the region ref names and the driver's own ref. ok is
// false for the zero ref, a ref whose set IDs carry no prefix, and a ref
// whose two IDs name different regions: none of them was made by this
// client.
func decodeRef(ref workflow.RunRef) (region string, inner workflow.RunRef, ok bool) {
	rr, rid, rok := splitID(string(ref.RunID))
	sr, sid, sok := splitID(string(ref.ScheduledID))
	inner = workflow.RunRef{RunID: workflow.RunID(rid), ScheduledID: workflow.ScheduledID(sid)}
	switch {
	case ref.RunID.IsZero() && ref.ScheduledID.IsZero():
		return "", ref, false
	case ref.RunID.IsZero():
		return sr, inner, sok
	case ref.ScheduledID.IsZero():
		return rr, inner, rok
	case rok && sok && rr == sr:
		return rr, inner, true
	}
	return "", ref, false
}

// encodeScheduleID prefixes a driver's schedule ID with region.
func encodeScheduleID(region string, id workflow.ScheduleID) workflow.ScheduleID {
	return workflow.ScheduleID(prefix(region) + string(id))
}

// regionLabels returns labels with LabelSchedule, which a fired run
// carries with the driver's own schedule ID, prefixed by region the way
// Schedule, Schedules and Tick.ScheduleID name it, so the label can be
// handed back to PauseSchedule, TriggerSchedule or a run query. labels is
// copied, never changed in place: the driver owns its map.
func regionLabels(labels map[string]string, region string) map[string]string {
	id, ok := labels[workflow.LabelSchedule]
	if !ok || id == "" {
		return labels
	}
	out := maps.Clone(labels)
	out[workflow.LabelSchedule] = string(encodeScheduleID(region, workflow.ScheduleID(id)))
	return out
}

// regionQuery is q as region must see it: a LabelSchedule filter holds
// the ID this client handed out, which is turned back into the driver's
// own ID. It reports false when the filter names a schedule of another
// region (or none), so region is not asked and matches nothing.
func regionQuery(q workflow.RunQuery, region string) (workflow.RunQuery, bool) {
	id, ok := q.Labels[workflow.LabelSchedule]
	if !ok {
		return q, true
	}
	r, inner, ok := splitID(id)
	if !ok || r != region {
		return q, false
	}
	q.Labels = maps.Clone(q.Labels)
	q.Labels[workflow.LabelSchedule] = inner
	return q, true
}
