package failover

import (
	"maps"
	"strconv"
	"strings"

	"github.com/ubgo/workflow"
)

// side is which backend holds a run.
type side uint8

const (
	unknownSide side = iota // a ref without a prefix
	primarySide
	secondarySide
)

// The prefixes a side puts in front of a driver's IDs. The driver's own ID
// follows unchanged, so stripping the prefix gives back exactly the ref the
// driver handed out.
const (
	primaryPrefix   = "primary:"
	secondaryPrefix = "secondary:"
)

func (s side) prefix() string {
	if s == secondarySide {
		return secondaryPrefix
	}
	return primaryPrefix
}

func (s side) String() string {
	if s == secondarySide {
		return "secondary"
	}
	return "primary"
}

// splitID returns the side named by id's prefix and the driver's own ID.
// An unprefixed id is unknownSide and comes back whole.
func splitID(id string) (side, string) {
	switch {
	case strings.HasPrefix(id, primaryPrefix):
		return primarySide, id[len(primaryPrefix):]
	case strings.HasPrefix(id, secondaryPrefix):
		return secondarySide, id[len(secondaryPrefix):]
	}
	return unknownSide, id
}

// encodeRef prefixes every set ID of a driver's ref with s.
func encodeRef(s side, ref workflow.RunRef) workflow.RunRef {
	if !ref.RunID.IsZero() {
		ref.RunID = workflow.RunID(s.prefix() + string(ref.RunID))
	}
	if !ref.ScheduledID.IsZero() {
		ref.ScheduledID = workflow.ScheduledID(s.prefix() + string(ref.ScheduledID))
	}
	return ref
}

// decodeRef returns the side ref names and the driver's own ref. When the
// two IDs name different sides the ref was not made by this client, and
// the result is unknownSide with ok false.
func decodeRef(ref workflow.RunRef) (s side, inner workflow.RunRef, ok bool) {
	rs, rid := splitID(string(ref.RunID))
	ss, sid := splitID(string(ref.ScheduledID))
	inner = workflow.RunRef{RunID: workflow.RunID(rid), ScheduledID: workflow.ScheduledID(sid)}
	switch {
	case ref.RunID.IsZero():
		return ss, inner, true
	case ref.ScheduledID.IsZero():
		return rs, inner, true
	case rs == ss:
		return rs, inner, true
	}
	return unknownSide, ref, false
}

// encodeScheduleID names where a schedule lives. A schedule on both
// backends holds both IDs: "both:" then the primary ID's length, a colon,
// and the two IDs back to back, so neither ID's content can confuse the
// split.
func encodeScheduleID(s side, id workflow.ScheduleID) workflow.ScheduleID {
	return workflow.ScheduleID(s.prefix() + string(id))
}

const bothPrefix = "both:"

func encodeBothScheduleID(p, s workflow.ScheduleID) workflow.ScheduleID {
	return workflow.ScheduleID(bothPrefix + strconv.Itoa(len(p)) + ":" + string(p) + string(s))
}

// decodeScheduleID returns the primary and secondary IDs id stands for;
// either is empty when the schedule does not live there. An unprefixed id
// is taken as the primary's, where schedules live by default.
func decodeScheduleID(id workflow.ScheduleID) (p, s workflow.ScheduleID) {
	str := string(id)
	if rest, ok := strings.CutPrefix(str, bothPrefix); ok {
		if n, tail, ok := cutLen(rest); ok {
			return workflow.ScheduleID(tail[:n]), workflow.ScheduleID(tail[n:])
		}
		return id, ""
	}
	side, inner := splitID(str)
	if side == secondarySide {
		return "", workflow.ScheduleID(inner)
	}
	return workflow.ScheduleID(inner), ""
}

// cutLen parses "<n>:<tail>" and checks tail holds at least n bytes.
func cutLen(s string) (int, string, bool) {
	num, tail, ok := strings.Cut(s, ":")
	if !ok {
		return 0, "", false
	}
	n, err := strconv.Atoi(num)
	if err != nil || n < 0 || n > len(tail) {
		return 0, "", false
	}
	return n, tail, true
}

// sideLabels returns labels with LabelSchedule, which a fired run carries
// with the backend's own schedule ID, prefixed by s the way Schedules,
// Tick.ScheduleID and Schedule name it, so the label can be handed back
// to PauseSchedule, TriggerSchedule or a run query. labels is copied,
// never changed in place: the driver owns its map. Labels without the
// key come back as they are.
func sideLabels(labels map[string]string, s side) map[string]string {
	id, ok := labels[workflow.LabelSchedule]
	if !ok || id == "" {
		return labels
	}
	out := maps.Clone(labels)
	out[workflow.LabelSchedule] = string(encodeScheduleID(s, workflow.ScheduleID(id)))
	return out
}

// sideQuery is q as backend s must see it: a LabelSchedule filter holds
// the ID this client handed out, which is turned back into s's own ID.
// It reports false when the filter names a schedule s does not hold, so
// s is not asked and matches nothing.
func sideQuery(q workflow.RunQuery, s side) (workflow.RunQuery, bool) {
	id, ok := q.Labels[workflow.LabelSchedule]
	if !ok {
		return q, true
	}
	p, sec := decodeScheduleID(workflow.ScheduleID(id))
	inner := p
	if s == secondarySide {
		inner = sec
	}
	if inner.IsZero() {
		return q, false
	}
	q.Labels = maps.Clone(q.Labels)
	q.Labels[workflow.LabelSchedule] = string(inner)
	return q, true
}
