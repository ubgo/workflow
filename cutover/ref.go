package cutover

import (
	"maps"
	"strings"

	"github.com/ubgo/workflow"
)

// Side names one of the two backends of a cutover.
//
//ds:def id=cutover-side-a33cayts owner=@khanakia stability=stable
type Side string

// The two backends.
const (
	// SideOld is the backend the app is leaving: it keeps running the runs
	// it holds and takes no new work.
	SideOld Side = "old"
	// SideNew is the backend the app is moving to: every Enqueue goes there.
	SideNew Side = "new"
)

func (s Side) valid() bool { return s == SideOld || s == SideNew }

// prefix is what s puts in front of a driver's IDs. The driver's own ID
// follows unchanged, so stripping the prefix gives back exactly the ID the
// driver handed out.
func (s Side) prefix() string { return string(s) + ":" }

// other is the backend that is not s.
func (s Side) other() Side {
	if s == SideOld {
		return SideNew
	}
	return SideOld
}

// splitID returns the side id's prefix names and the driver's own ID. An
// unprefixed id comes back whole with an empty side.
func splitID(id string) (Side, string) {
	for _, s := range []Side{SideOld, SideNew} {
		if rest, ok := strings.CutPrefix(id, s.prefix()); ok {
			return s, rest
		}
	}
	return "", id
}

// encodeRef prefixes every set ID of a driver's ref with s.
//
//ds:def id=cutover-encoderef-4er3m4bu owner=@khanakia stability=stable
func encodeRef(s Side, ref workflow.RunRef) workflow.RunRef {
	if !ref.RunID.IsZero() {
		ref.RunID = workflow.RunID(s.prefix() + string(ref.RunID))
	}
	if !ref.ScheduledID.IsZero() {
		ref.ScheduledID = workflow.ScheduledID(s.prefix() + string(ref.ScheduledID))
	}
	return ref
}

// decodeRef returns the side ref names (empty for an unprefixed ref) and
// the driver's own ref. ok is false when the two IDs name different
// sides: the ref was not made by this client.
func decodeRef(ref workflow.RunRef) (s Side, inner workflow.RunRef, ok bool) {
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
	return "", ref, false
}

func encodeScheduleID(s Side, id workflow.ScheduleID) workflow.ScheduleID {
	if id.IsZero() {
		return id
	}
	return workflow.ScheduleID(s.prefix() + string(id))
}

// sideLabels returns labels with LabelSchedule, which a fired run carries
// with the backend's own schedule ID, prefixed by s, so the label can be
// handed back to Unschedule or a run query. labels is copied, never
// changed in place: the driver owns its map.
func sideLabels(labels map[string]string, s Side) map[string]string {
	id, ok := labels[workflow.LabelSchedule]
	if !ok || id == "" {
		return labels
	}
	out := maps.Clone(labels)
	out[workflow.LabelSchedule] = string(encodeScheduleID(s, workflow.ScheduleID(id)))
	return out
}

// sideQuery is q as backend s must see it: a LabelSchedule filter holds an
// ID this client handed out, which is turned back into s's own ID. It
// reports false when the filter names a schedule of the other backend, so
// s is not asked. An unprefixed filter is passed to both as it is.
func sideQuery(q workflow.RunQuery, s Side) (workflow.RunQuery, bool) {
	id, ok := q.Labels[workflow.LabelSchedule]
	if !ok {
		return q, true
	}
	sd, inner := splitID(id)
	if sd == "" {
		return q, true
	}
	if sd != s {
		return q, false
	}
	q.Labels = maps.Clone(q.Labels)
	q.Labels[workflow.LabelSchedule] = inner
	return q, true
}
