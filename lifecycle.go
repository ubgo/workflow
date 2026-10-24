package workflow

import (
	"context"
	"time"
)

// LifecycleKind is what happened to a run in a LifecycleEvent.
//
// Code that switches on it must keep a default branch: a later version
// may add kinds.
type LifecycleKind string

// The lifecycle kinds.
const (
	// LifecycleInserted: Enqueue accepted a new run (not a duplicate
	// returned for an idempotency key).
	LifecycleInserted LifecycleKind = "inserted"
	// LifecycleStarted: a step attempt started (Step and Attempt set).
	LifecycleStarted LifecycleKind = "started"
	// LifecycleRetrying: a step attempt failed and the step will run
	// again after its backoff (Err set, Attempt is the failed one).
	LifecycleRetrying LifecycleKind = "retrying"
	// LifecycleSnoozed: a step snoozed (Snooze) and runs again later.
	LifecycleSnoozed LifecycleKind = "snoozed"
	// LifecycleProgress: a step reported progress (Progress set).
	LifecycleProgress LifecycleKind = "progress"
	// LifecycleFinished: the run reached a terminal status (Status set;
	// Err for a failure). The same moment RunNotifier reports.
	LifecycleFinished LifecycleKind = "finished"
)

// LifecycleEvent is one moment in a run's life, for a live view (a
// dashboard, a progress bar, an audit trail) that wants more than the
// end RunNotifier reports.
type LifecycleEvent struct {
	Kind     LifecycleKind
	Ref      RunRef
	Workflow string
	Queue    string
	Tenant   string

	// Step and Attempt name the attempt for Started, Retrying, Snoozed
	// and Progress. For Finished, Step is the step whose failure ended the
	// run (RunEvent.Step) and Attempt is zero; for Inserted both are
	// empty.
	Step    string
	Attempt int

	// Status is the run's status after the event.
	Status Status

	// Err is the attempt's error (Retrying, Snoozed) or the run's
	// (Finished, failed); nil otherwise.
	Err error

	// Progress is set for LifecycleProgress.
	Progress *Progress

	// Node is the node the event happened on (CapNodeIdentity); empty
	// otherwise.
	Node string

	// Labels are the run's labels. Observers must not modify them.
	Labels map[string]string

	// At is when it happened, on the driver's clock.
	At time.Time
}

// LifecycleFunc observes lifecycle events.
type LifecycleFunc func(ctx context.Context, ev LifecycleEvent)

// LifecycleNotifier is an optional interface a client implements to
// stream its runs' lifecycle. The contract:
//
//   - Register before Start: OnLifecycle after Start returns
//     ErrAlreadyStarted. Each observer sees every event, in
//     registration order.
//   - Delivery is best-effort and local: an observer sees the events
//     that happen in this client's process (an Enqueue it accepted, an
//     attempt its workers ran), so on a cluster each node streams its
//     own share. It is a live feed, not a log; RunNotifier with
//     CapRunFinished is the guaranteed channel for run ends.
//   - An observer runs on a driver goroutine, must return quickly, and a
//     panic in it is recovered and never changes a run.
//   - For one run, the events of one process arrive in the order they
//     happened.
type LifecycleNotifier interface {
	OnLifecycle(fn LifecycleFunc) error
}
