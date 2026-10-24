package admin

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
)

// Nodes lists the nodes that are online, sorted by ID; see
// workflow.NodeLister for when a node drops out.
func (a *Admin) Nodes(ctx context.Context) ([]workflow.NodeInfo, error) {
	if err := a.check(OpNodes); err != nil {
		return nil, err
	}
	ns, err := a.c.(workflow.NodeLister).Nodes(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(ns, func(x, y workflow.NodeInfo) int { return strings.Compare(x.ID, y.ID) })
	return ns, nil
}

// Health is the client's health report (workflow.CheckHealth). It never
// fails: a client without HealthChecker is reported live and ready with a
// check saying it cannot tell. An admin screen shows it; a liveness or
// readiness probe should call workflow.CheckHealth on the app's own
// unauthenticated probe route instead of going through the admin API.
func (a *Admin) Health(ctx context.Context) workflow.HealthReport {
	return workflow.CheckHealth(ctx, a.c)
}

// Reschedule moves a delayed run that has not started to at; see
// workflow.RunRescheduler. A zero at is refused here (the interface
// reads it as now, which an operator should say on purpose with the
// current time).
func (a *Admin) Reschedule(ctx context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	if err := a.check(OpReschedule); err != nil {
		return workflow.RunRef{}, err
	}
	if ref.IsZero() {
		return workflow.RunRef{}, fmt.Errorf("%w: admin: run ref is empty", workflow.ErrInvalid)
	}
	if at.IsZero() {
		return workflow.RunRef{}, fmt.Errorf("%w: admin: reschedule needs a time", workflow.ErrInvalid)
	}
	return a.c.(workflow.RunRescheduler).Reschedule(ctx, ref, at)
}

// CancelStep withdraws one step of a run; see workflow.StepCanceller for
// what the run does next.
func (a *Admin) CancelStep(ctx context.Context, ref workflow.RunRef, step string) error {
	if err := a.check(OpCancelStep); err != nil {
		return err
	}
	if ref.IsZero() || step == "" {
		return fmt.Errorf("%w: admin: cancel step needs a run and a step name", workflow.ErrInvalid)
	}
	return a.c.(workflow.StepCanceller).CancelStep(ctx, ref, step)
}
