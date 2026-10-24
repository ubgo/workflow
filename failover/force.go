package failover

import (
	"context"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.ScheduleTriggerer = (*Client)(nil)
	_ workflow.RunForcer         = (*Client)(nil)
)

// TriggerSchedule runs a schedule now on the backend holding it and
// returns the run's ref prefixed by that backend. A schedule made under
// WithSchedulesOnBoth is triggered on the primary's copy only, so one
// trigger makes one run. It never fails over: a run triggered on the
// secondary would not be the schedule's last run there for the overlap
// check. Needs CapScheduleTrigger.
func (f *Client) TriggerSchedule(ctx context.Context, id workflow.ScheduleID) (workflow.RunRef, error) {
	if err := f.need("TriggerSchedule", workflow.CapScheduleTrigger); err != nil {
		return workflow.RunRef{}, err
	}
	pid, sid := decodeScheduleID(id)
	sd, inner := primarySide, pid
	if pid.IsZero() {
		sd, inner = secondarySide, sid
	}
	t, ok := f.backend(sd).(workflow.ScheduleTriggerer)
	if !ok {
		return workflow.RunRef{}, &workflow.UnsupportedError{Field: "TriggerSchedule", Missing: workflow.CapScheduleTrigger, Reason: "the " + sd.String() + " backend does not implement it"}
	}
	ref, err := t.TriggerSchedule(ctx, inner)
	if err != nil {
		return workflow.RunRef{}, err
	}
	return encodeRef(sd, ref), nil
}

// ForceComplete force-completes the run on the backend its ref names.
// Needs CapForceRun on both.
func (f *Client) ForceComplete(ctx context.Context, ref workflow.RunRef) error {
	return f.force("ForceComplete", ref, func(r workflow.RunForcer, inner workflow.RunRef) error {
		return r.ForceComplete(ctx, inner)
	})
}

// ForceFail force-fails the run on the backend its ref names. Needs
// CapForceRun on both.
func (f *Client) ForceFail(ctx context.Context, ref workflow.RunRef, reason string) error {
	return f.force("ForceFail", ref, func(r workflow.RunForcer, inner workflow.RunRef) error {
		return r.ForceFail(ctx, inner, reason)
	})
}

func (f *Client) force(field string, ref workflow.RunRef, fn func(workflow.RunForcer, workflow.RunRef) error) error {
	if err := f.need(field, workflow.CapForceRun); err != nil {
		return err
	}
	return f.onSide(ref, func(sd side, c workflow.Client, inner workflow.RunRef) error {
		r, ok := c.(workflow.RunForcer)
		if !ok {
			return &workflow.UnsupportedError{Field: field, Missing: workflow.CapForceRun, Reason: "the " + sd.String() + " backend does not implement it"}
		}
		return fn(r, inner)
	})
}
