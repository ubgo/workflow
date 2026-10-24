package workflowtest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// limited is the memory driver claiming no capabilities and refusing,
// through the core helpers, everything that needs one. The memory driver
// has every capability, so without this the suite's refusal subtests
// would only ever be skipped; here they run, and every other subtest
// must skip naming what is missing.
type limited struct {
	*memory.Client
	caps workflow.Caps // what it claims; zero refuses everything
	wfs  []workflow.Workflow
}

func (l *limited) Capabilities() workflow.Caps { return l.caps }

func (l *limited) Register(wf workflow.Workflow) error {
	l.wfs = append(l.wfs, wf)
	return l.Client.Register(wf)
}

func (l *limited) Start(ctx context.Context) error {
	for _, wf := range l.wfs {
		if err := workflow.CheckCaps(l.caps, wf); err != nil {
			return err
		}
	}
	return l.Client.Start(ctx)
}

func (l *limited) Enqueue(ctx context.Context, j workflow.Job) (workflow.RunRef, error) {
	if err := workflow.ValidateJob(j); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJob(l.caps, j); err != nil {
		return workflow.RunRef{}, err
	}
	return l.Client.Enqueue(ctx, j)
}

func (l *limited) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if err := workflow.ValidateSchedule(s); err != nil {
		return "", err
	}
	if err := workflow.CheckScheduleCaps(l.caps, s); err != nil {
		return "", err
	}
	return l.Client.Schedule(ctx, s)
}

func (l *limited) Cancel(context.Context, workflow.RunRef) error {
	return &workflow.UnsupportedError{Field: "Cancel", Missing: workflow.CapCancel}
}

// Status reports the states a driver without CapRunStates has: a run in
// retry backoff reads Queued and a discarded run Cancelled or Failed
// (workflow.EndStatus), since the suite fails a driver that reports a
// state it does not declare.
func (l *limited) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	st, err := l.Client.Status(ctx, ref)
	if err != nil || l.caps.Has(workflow.CapRunStates) {
		return st, err
	}
	switch st {
	case workflow.StatusRetrying:
		return workflow.StatusQueued, nil
	case workflow.StatusDiscarded:
		return workflow.EndStatus(l.caps, workflow.Discard(errDiscarded)), nil
	}
	return st, nil
}

// errDiscarded stands in for the step error behind a discarded run.
var errDiscarded = errors.New("discarded")

// noChainEditor hides the memory driver's chain editor from steps, as a
// driver without CapChain has none.
func noChainEditor(next workflow.Handler) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		return next(workflow.WithChainEditor(ctx, nil), in)
	}
}

func TestRunAgainstADriverWithNoCapabilities(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(time.Date(2026, 1, 1, 10, 0, 30, 0, time.UTC))
		return workflowtest.Instance{Client: &limited{Client: memory.New(memory.WithClock(clk), memory.WithMiddleware(noChainEditor))}, Clock: clk}
	}})
}

// TestRunAgainstACronOnlyDriver runs the suite against a driver that has
// plain UTC cron and nothing else, so the refusals of a zone and of every
// misfire policy run instead of being skipped.
func TestRunAgainstACronOnlyDriver(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(time.Date(2026, 1, 1, 10, 0, 30, 0, time.UTC))
		return workflowtest.Instance{Client: &limited{Client: memory.New(memory.WithClock(clk), memory.WithMiddleware(noChainEditor)), caps: workflow.CapCron}, Clock: clk}
	}})
}
