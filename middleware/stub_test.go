package middleware_test

import (
	"context"
	"strconv"
	"sync"

	"github.com/ubgo/workflow"
)

// stubClient records what reaches the driver. It implements only what the
// middleware calls; the rest return zero values.
type stubClient struct {
	caps       workflow.Caps
	enqueueErr error
	cancelErr  error

	mu        sync.Mutex
	jobs      []workflow.Job
	cancelled []workflow.RunRef
	events    []workflow.Event
	schedules []workflow.Schedule
}

func (s *stubClient) Enqueue(_ context.Context, job workflow.Job) (workflow.RunRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.enqueueErr != nil {
		return workflow.RunRef{}, s.enqueueErr
	}
	s.jobs = append(s.jobs, job)
	id := strconv.Itoa(len(s.jobs))
	if !job.RunAt.IsZero() {
		return workflow.RunRef{ScheduledID: workflow.ScheduledID(id)}, nil
	}
	return workflow.RunRef{RunID: workflow.RunID(id)}, nil
}

func (s *stubClient) Cancel(_ context.Context, ref workflow.RunRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelled = append(s.cancelled, ref)
	return s.cancelErr
}

func (s *stubClient) Register(workflow.Workflow) error { return nil }
func (s *stubClient) Start(context.Context) error      { return nil }
func (s *stubClient) Status(context.Context, workflow.RunRef) (workflow.Status, error) {
	return "", nil
}
func (s *stubClient) Signal(_ context.Context, ev workflow.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}
func (s *stubClient) Schedule(_ context.Context, sc workflow.Schedule) (workflow.ScheduleID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schedules = append(s.schedules, sc)
	return "s1", nil
}
func (s *stubClient) Unschedule(context.Context, workflow.ScheduleID) error { return nil }
func (s *stubClient) Capabilities() workflow.Caps                           { return s.caps }

var _ workflow.Client = (*stubClient)(nil)

// notifierStub adds workflow.RunNotifier to stubClient; the test fires the
// registered observers itself with finish.
type notifierStub struct {
	*stubClient
	observers []workflow.RunFinishedFunc
}

func (n *notifierStub) OnRunFinished(fn workflow.RunFinishedFunc) error {
	n.observers = append(n.observers, fn)
	return nil
}

// finish delivers the end of the i-th enqueued job to every observer.
func (n *notifierStub) finish(i int, st workflow.Status) {
	ev := workflow.RunEvent{Ref: workflow.RunRef{RunID: workflow.RunID(strconv.Itoa(i + 1))}, Workflow: n.jobs[i].Workflow, Status: st, Labels: n.jobs[i].Labels}
	for _, fn := range n.observers {
		fn(context.Background(), ev)
	}
}

var _ workflow.RunNotifier = (*notifierStub)(nil)
