package workflow

import (
	"context"
	"time"
)

// RunEvent reports that a run reached a terminal status. It is what lets
// app-side code react to a run's end without polling Status: an enqueue
// middleware releasing a uniqueness key, a batch counting finished
// members, an audit log.
type RunEvent struct {
	// Ref is the run. ScheduledID is set too when the run was enqueued
	// with RunAt, so a caller that stored the RunRef Enqueue returned
	// (which held only the ScheduledID) can match the event.
	Ref RunRef

	// Workflow is the workflow name.
	Workflow string

	// Status is terminal: StatusSucceeded, StatusFailed or
	// StatusCancelled, or StatusDiscarded on a driver with CapRunStates.
	Status Status

	// Err is why the run failed; nil when it succeeded. In-process
	// drivers pass the original error; an engine rebuilds it from the
	// message it stored, so errors.Is against an app's own sentinel is
	// reliable only on in-process drivers. A cancelled run's Err matches
	// ErrCancelled on every driver.
	Err error

	// Labels are the run's Job.Labels. Observers must not modify them.
	Labels map[string]string

	// FinishedAt is when the run reached its terminal status, from the
	// driver's clock: the engine's timestamp for an engine driver when the
	// engine records one, else the driver's own clock at the moment it
	// learned of the end (a delayed run cancelled before it fired).
	FinishedAt time.Time

	// Step is the step whose failure ended the run (RunInfo.FailedStep);
	// empty for other ends and for drivers that do not know it.
	Step string

	// Tenant is the run's Job.Tenant (CapTenant).
	Tenant string
}

// RunFinishedFunc observes run ends. It must be idempotent, because
// delivery is at least once.
type RunFinishedFunc func(ctx context.Context, ev RunEvent)

// RunNotifier is an optional interface a driver's client implements to
// report run ends. Callers discover it with a type assertion on the
// Client, so a driver that cannot report run ends is never forced to
// pretend it can, and app-side code falls back (for example to a TTL)
// when the assertion fails.
//
// The delivery contract every implementation keeps:
//
//   - Register before Start: OnRunFinished after Start returns
//     ErrAlreadyStarted. Several observers may be registered; each sees
//     every event, in registration order.
//   - An observer runs on a driver goroutine, never on the goroutine that
//     enqueued or cancelled the run, and may run concurrently for
//     different runs. It should return quickly; long work belongs in a
//     new Enqueue.
//   - It is called after Status reports the terminal status, so an
//     observer that calls Status sees the same status as ev.Status.
//   - Its ctx is cancelled when the client shuts down (the Start ctx).
//   - A panic in an observer is recovered by the driver and never
//     changes the run's status.
//   - Delivery is at least once: an observer may see the same run's
//     event more than once (an engine retrying a delivery) and must
//     tolerate it.
//   - With CapRunFinished, every run that reaches a terminal status is
//     delivered, including cancelled runs and runs that never started;
//     a durable driver also delivers, after a restart, the ends that
//     happened while no client was running. Without CapRunFinished,
//     delivery is best-effort: an event may be lost (for example on a
//     crash), so a consumer must keep a fallback.
type RunNotifier interface {
	// OnRunFinished registers fn. It returns ErrAlreadyStarted after
	// Start.
	OnRunFinished(fn RunFinishedFunc) error
}
