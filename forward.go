package workflow

import (
	"context"
	"time"
)

// Forward implements every optional client interface of the run query,
// pause, schedule, drain, scaling and closure families (RunQuerier,
// RunRetrier, RunDeleter, BulkCanceller, Pauser, SchedulePauser,
// ScheduleLister, ScheduleTriggerer, TickNotifier, Drainer, StopCauser,
// WorkerScaler, ClosureEnqueuer, RunForcer, RunRescheduler, StepCanceller,
// ResultReader, NodeLister, HealthChecker, LifecycleNotifier and
// PayloadLimiter) by forwarding to Inner, and refuses with an UnsupportedError
// naming the capability when Inner does not implement one. A client
// wrapper embeds it beside the Client it wraps, so stacking a wrapper
// never hides what the inner client can do:
//
//	type wrapper struct {
//		workflow.Client
//		workflow.Forward
//	}
//	w := &wrapper{Client: inner, Forward: workflow.Forward{Inner: inner}}
//
// A wrapper that rewrites RunRefs or queries (failover's prefixed refs)
// implements those methods itself instead. RunNotifier is not here: the
// client Use returns forwards it on its own.
type Forward struct {
	Inner Client
}

var (
	_ RunQuerier       = Forward{}
	_ RunRetrier       = Forward{}
	_ RunDeleter       = Forward{}
	_ BulkCanceller    = Forward{}
	_ Pauser           = Forward{}
	_ SchedulePauser   = Forward{}
	_ ScheduleLister   = Forward{}
	_ TickNotifier     = Forward{}
	_ Drainer          = Forward{}
	_ ReportingDrainer = Forward{}
	_ StopCauser       = Forward{}
	_ WorkerScaler     = Forward{}
	_ ClosureEnqueuer  = Forward{}

	_ ScheduleTriggerer = Forward{}
	_ RunForcer         = Forward{}
	_ RunRescheduler    = Forward{}
	_ StepCanceller     = Forward{}
	_ ResultReader      = Forward{}
	_ NodeLister        = Forward{}
	_ HealthChecker     = Forward{}
	_ LifecycleNotifier = Forward{}
	_ PayloadLimiter    = Forward{}
)

// lacks is the refusal for an inner client without an interface.
func lacks(field string, c Caps) error {
	return &UnsupportedError{Field: field, Missing: c, Reason: "the wrapped client does not implement it"}
}

// Runs forwards to Inner's RunQuerier.
func (f Forward) Runs(ctx context.Context, q RunQuery) (RunPage, error) {
	if i, ok := f.Inner.(RunQuerier); ok {
		return i.Runs(ctx, q)
	}
	return RunPage{}, lacks("Runs", CapRunQuery)
}

// Retry forwards to Inner's RunRetrier.
func (f Forward) Retry(ctx context.Context, ref RunRef) (RunRef, error) {
	if i, ok := f.Inner.(RunRetrier); ok {
		return i.Retry(ctx, ref)
	}
	return RunRef{}, lacks("Retry", CapRetryRun)
}

// Delete forwards to Inner's RunDeleter.
func (f Forward) Delete(ctx context.Context, ref RunRef) error {
	if i, ok := f.Inner.(RunDeleter); ok {
		return i.Delete(ctx, ref)
	}
	return lacks("Delete", CapDeleteRun)
}

// CancelAll forwards to Inner's BulkCanceller.
func (f Forward) CancelAll(ctx context.Context, q RunQuery) (int, error) {
	if i, ok := f.Inner.(BulkCanceller); ok {
		return i.CancelAll(ctx, q)
	}
	return 0, lacks("CancelAll", CapCancelAll)
}

// Pause forwards to Inner's Pauser.
func (f Forward) Pause(ctx context.Context, t PauseTarget) error {
	if i, ok := f.Inner.(Pauser); ok {
		return i.Pause(ctx, t)
	}
	return lacks("Pause", RequiresPause(t))
}

// Resume forwards to Inner's Pauser.
func (f Forward) Resume(ctx context.Context, t PauseTarget) error {
	if i, ok := f.Inner.(Pauser); ok {
		return i.Resume(ctx, t)
	}
	return lacks("Resume", RequiresPause(t))
}

// Paused forwards to Inner's Pauser.
func (f Forward) Paused(ctx context.Context) ([]PauseTarget, error) {
	if i, ok := f.Inner.(Pauser); ok {
		return i.Paused(ctx)
	}
	return nil, lacks("Paused", CapPauseWorkflow)
}

// PauseSchedule forwards to Inner's SchedulePauser.
func (f Forward) PauseSchedule(ctx context.Context, t ScheduleTarget) error {
	if i, ok := f.Inner.(SchedulePauser); ok {
		return i.PauseSchedule(ctx, t)
	}
	return lacks("PauseSchedule", CapSchedulePause)
}

// ResumeSchedule forwards to Inner's SchedulePauser.
func (f Forward) ResumeSchedule(ctx context.Context, t ScheduleTarget) error {
	if i, ok := f.Inner.(SchedulePauser); ok {
		return i.ResumeSchedule(ctx, t)
	}
	return lacks("ResumeSchedule", CapSchedulePause)
}

// Schedules forwards to Inner's ScheduleLister.
func (f Forward) Schedules(ctx context.Context) ([]ScheduleInfo, error) {
	if i, ok := f.Inner.(ScheduleLister); ok {
		return i.Schedules(ctx)
	}
	return nil, lacks("Schedules", CapScheduleList)
}

// OnTick forwards to Inner's TickNotifier.
func (f Forward) OnTick(fn TickObserver) error {
	if i, ok := f.Inner.(TickNotifier); ok {
		return i.OnTick(fn)
	}
	return lacks("OnTick", CapTickEvents)
}

// Drain forwards to Inner's Drainer.
func (f Forward) Drain(ctx context.Context) error {
	if i, ok := f.Inner.(Drainer); ok {
		return i.Drain(ctx)
	}
	return lacks("Drain", CapDrain)
}

// DrainWithReport forwards to Inner's ReportingDrainer; an inner client
// without one returns an UnsupportedError naming CapDrainReport.
func (f Forward) DrainWithReport(ctx context.Context) (DrainReport, error) {
	if i, ok := f.Inner.(ReportingDrainer); ok {
		return i.DrainWithReport(ctx)
	}
	return DrainReport{}, lacks("DrainWithReport", CapDrainReport)
}

// Done forwards to Inner's Drainer. An inner client without one has no
// way to say it stopped, so Done returns a nil channel, which never
// closes; check CapDrain before waiting on it.
func (f Forward) Done() <-chan struct{} {
	if i, ok := f.Inner.(Drainer); ok {
		return i.Done()
	}
	return nil
}

// StopCause forwards to Inner's StopCauser; an inner client without one
// never stops itself, so it returns nil.
func (f Forward) StopCause() error {
	if i, ok := f.Inner.(StopCauser); ok {
		return i.StopCause()
	}
	return nil
}

// QueueLoads forwards to Inner's WorkerScaler.
func (f Forward) QueueLoads(ctx context.Context) ([]QueueLoad, error) {
	if i, ok := f.Inner.(WorkerScaler); ok {
		return i.QueueLoads(ctx)
	}
	return nil, lacks("QueueLoads", CapWorkerScale)
}

// SetWorkers forwards to Inner's WorkerScaler.
func (f Forward) SetWorkers(ctx context.Context, queue string, n int) error {
	if i, ok := f.Inner.(WorkerScaler); ok {
		return i.SetWorkers(ctx, queue, n)
	}
	return lacks("SetWorkers", CapWorkerScale)
}

// EnqueueClosure forwards to Inner's ClosureEnqueuer. Enqueue middleware
// of the wrapper does not run for a closure: it has no Job to rewrite.
func (f Forward) EnqueueClosure(ctx context.Context, c ClosureJob) (RunRef, error) {
	if i, ok := f.Inner.(ClosureEnqueuer); ok {
		return i.EnqueueClosure(ctx, c)
	}
	return RunRef{}, lacks("EnqueueClosure", CapClosure)
}

// TriggerSchedule forwards to Inner's ScheduleTriggerer.
func (f Forward) TriggerSchedule(ctx context.Context, id ScheduleID) (RunRef, error) {
	if i, ok := f.Inner.(ScheduleTriggerer); ok {
		return i.TriggerSchedule(ctx, id)
	}
	return RunRef{}, lacks("TriggerSchedule", CapScheduleTrigger)
}

// ForceComplete forwards to Inner's RunForcer.
func (f Forward) ForceComplete(ctx context.Context, ref RunRef) error {
	if i, ok := f.Inner.(RunForcer); ok {
		return i.ForceComplete(ctx, ref)
	}
	return lacks("ForceComplete", CapForceRun)
}

// ForceFail forwards to Inner's RunForcer.
func (f Forward) ForceFail(ctx context.Context, ref RunRef, reason string) error {
	if i, ok := f.Inner.(RunForcer); ok {
		return i.ForceFail(ctx, ref, reason)
	}
	return lacks("ForceFail", CapForceRun)
}

// Reschedule forwards to Inner's RunRescheduler.
func (f Forward) Reschedule(ctx context.Context, ref RunRef, at time.Time) (RunRef, error) {
	if i, ok := f.Inner.(RunRescheduler); ok {
		return i.Reschedule(ctx, ref, at)
	}
	return RunRef{}, lacks("Reschedule", 0)
}

// CancelStep forwards to Inner's StepCanceller.
func (f Forward) CancelStep(ctx context.Context, ref RunRef, step string) error {
	if i, ok := f.Inner.(StepCanceller); ok {
		return i.CancelStep(ctx, ref, step)
	}
	return lacks("CancelStep", 0)
}

// Result forwards to Inner's ResultReader.
func (f Forward) Result(ctx context.Context, ref RunRef) ([]byte, error) {
	if i, ok := f.Inner.(ResultReader); ok {
		return i.Result(ctx, ref)
	}
	return nil, lacks("Result", CapResult)
}

// Nodes forwards to Inner's NodeLister.
func (f Forward) Nodes(ctx context.Context) ([]NodeInfo, error) {
	if i, ok := f.Inner.(NodeLister); ok {
		return i.Nodes(ctx)
	}
	return nil, lacks("Nodes", 0)
}

// Health forwards to Inner (CheckHealth, so an inner client without a
// HealthChecker reads as healthy with a check saying it cannot tell).
func (f Forward) Health(ctx context.Context) HealthReport { return CheckHealth(ctx, f.Inner) }

// OnLifecycle forwards to Inner's LifecycleNotifier.
func (f Forward) OnLifecycle(fn LifecycleFunc) error {
	if i, ok := f.Inner.(LifecycleNotifier); ok {
		return i.OnLifecycle(fn)
	}
	return lacks("OnLifecycle", 0)
}

// PayloadLimit forwards to Inner's PayloadLimiter; zero (no cap) when
// Inner has none.
func (f Forward) PayloadLimit() int {
	if i, ok := f.Inner.(PayloadLimiter); ok {
		return i.PayloadLimit()
	}
	return 0
}
