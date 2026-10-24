package workflowtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.ScheduleTriggerer = (*Fake)(nil)
	_ workflow.RunForcer         = (*Fake)(nil)
)

// FakeForce is one recorded ForceComplete (Complete true) or ForceFail.
type FakeForce struct {
	Ref      workflow.RunRef
	Complete bool
	Reason   string
}

// TriggerSchedule records a trigger of a recorded schedule and enqueues
// its job as a driver would: the schedule's Input and
// workflow.TriggeredLabels, through Enqueue, so AssertEnqueued sees it.
// It needs CapScheduleTrigger and obeys OverlapSkip against the run the
// previous trigger made (use Finish to end it). An unscheduled or unknown
// id goes to the pass-through client, or is ErrNotFound without one.
func (f *Fake) TriggerSchedule(ctx context.Context, id workflow.ScheduleID) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if !f.caps.Has(workflow.CapScheduleTrigger) {
		return workflow.RunRef{}, &workflow.UnsupportedError{Field: "TriggerSchedule", Missing: workflow.CapScheduleTrigger}
	}
	f.mu.Lock()
	i := f.scheduleIndexLocked(id)
	if i < 0 {
		f.mu.Unlock()
		if t, ok := f.inner.(workflow.ScheduleTriggerer); ok {
			return t.TriggerSchedule(ctx, id)
		}
		return workflow.RunRef{}, fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	}
	fs := f.scheds[i]
	if r, ok := f.byRef[fs.LastRun]; ok && fs.Schedule.Overlap.Blocks(r.status) {
		f.mu.Unlock()
		return workflow.RunRef{}, workflow.OverlapRefusal(id, fs.LastRun, r.status)
	}
	f.mu.Unlock()
	ref, err := f.Enqueue(workflow.AllowReservedLabels(ctx, workflow.LabelSchedule, workflow.LabelScheduleName, workflow.LabelTrigger), workflow.Job{Workflow: fs.Schedule.Workflow, Payload: fs.Schedule.Input,
		Labels: workflow.TriggeredLabels(fs.Schedule.Labels, id, fs.Schedule.Name), Tenant: fs.Schedule.Tenant})
	if err != nil {
		return workflow.RunRef{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if i := f.scheduleIndexLocked(id); i >= 0 {
		f.scheds[i].LastRun = ref
	}
	f.triggers = append(f.triggers, id)
	return ref, nil
}

// scheduleIndexLocked is the index of the live recorded schedule id, or
// -1.
func (f *Fake) scheduleIndexLocked(id workflow.ScheduleID) int {
	for i := range f.scheds {
		if f.scheds[i].ID == id && !f.scheds[i].Unscheduled {
			return i
		}
	}
	return -1
}

// ForceComplete records the call and marks a recorded run succeeded, as
// workflow.RunForcer does: it accepts an unfinished or failed run
// (ErrWrongState otherwise) and needs CapForceRun. The Fake runs no
// chain, so a chained job is not enqueued. A ref the Fake did not issue
// goes to the pass-through client, or is ErrNotFound without one.
func (f *Fake) ForceComplete(ctx context.Context, ref workflow.RunRef) error {
	return f.force(ctx, ref, FakeForce{Ref: ref, Complete: true})
}

// ForceFail records the call and marks an unfinished recorded run failed,
// as workflow.RunForcer does (ErrWrongState for a finished run). It needs
// CapForceRun.
func (f *Fake) ForceFail(ctx context.Context, ref workflow.RunRef, reason string) error {
	return f.force(ctx, ref, FakeForce{Ref: ref, Reason: reason})
}

func (f *Fake) force(ctx context.Context, ref workflow.RunRef, rec FakeForce) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	field := "ForceFail"
	if rec.Complete {
		field = "ForceComplete"
	}
	if !f.caps.Has(workflow.CapForceRun) {
		return &workflow.UnsupportedError{Field: field, Missing: workflow.CapForceRun}
	}
	f.mu.Lock()
	r, ok := f.byRef[ref]
	if !ok {
		f.mu.Unlock()
		if fr, ok := f.inner.(workflow.RunForcer); ok {
			if rec.Complete {
				return fr.ForceComplete(ctx, ref)
			}
			return fr.ForceFail(ctx, ref, rec.Reason)
		}
		return fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	defer f.mu.Unlock()
	switch {
	case rec.Complete && r.status == workflow.StatusFailed:
		r.status = workflow.StatusSucceeded
	case rec.Complete && workflow.CanForceComplete(r.status):
		f.finishLocked(r, workflow.StatusSucceeded)
	case !rec.Complete && workflow.CanForceFail(r.status):
		f.finishLocked(r, workflow.StatusFailed)
	default:
		return fmt.Errorf("%w: run %+v is %s", workflow.ErrWrongState, ref, r.status)
	}
	f.forced = append(f.forced, rec)
	return nil
}

// Triggers returns the schedules TriggerSchedule triggered, in order.
func (f *Fake) Triggers() []workflow.ScheduleID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]workflow.ScheduleID(nil), f.triggers...)
}

// Forced returns the recorded ForceComplete and ForceFail calls, in
// order.
func (f *Fake) Forced() []FakeForce {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakeForce(nil), f.forced...)
}

// AssertTriggered fails t unless schedule id was triggered.
func (f *Fake) AssertTriggered(t testing.TB, id workflow.ScheduleID) {
	t.Helper()
	for _, got := range f.Triggers() {
		if got == id {
			return
		}
	}
	t.Errorf("schedule %q was not triggered; triggered: %v", id, f.Triggers())
}

// AssertForced fails t unless ref was force-completed (complete true) or
// force-failed (complete false).
func (f *Fake) AssertForced(t testing.TB, ref workflow.RunRef, complete bool) {
	t.Helper()
	for _, got := range f.Forced() {
		if got.Ref == ref && got.Complete == complete {
			return
		}
	}
	t.Errorf("run %+v was not forced (complete=%v); forced: %+v", ref, complete, f.Forced())
}
