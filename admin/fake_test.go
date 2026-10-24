package admin

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

// allCaps is every capability an Admin operation can need.
const allCaps = workflow.CapRunQuery | workflow.CapRetryRun | workflow.CapDeleteRun | workflow.CapCancel |
	workflow.CapCancelAll | workflow.CapPauseWorkflow | workflow.CapPauseQueue | workflow.CapScheduleList |
	workflow.CapRunStates | workflow.CapSchedulePause | workflow.CapLabelPrefix | workflow.CapScheduleTrigger |
	workflow.CapForceRun | workflow.CapNodeIdentity

// fakeClient is a workflow.Client with every optional interface Admin
// uses. It records each call, so a test can prove a refused request
// reached no operation, and returns err from every call when set.
type fakeClient struct {
	caps workflow.Caps
	err  error

	mu        sync.Mutex
	runs      []workflow.RunInfo
	schedules []workflow.ScheduleInfo
	pauses    []workflow.PauseTarget
	calls     []string
	queries   []workflow.RunQuery
	// refErr is the error Retry and Delete return for one run, ahead of
	// err, so a bulk test can make single runs change under it.
	refErr map[workflow.RunID]error
}

// errFor is refErr for ref, else err.
func (f *fakeClient) errFor(ref workflow.RunRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.refErr[ref.RunID]; ok {
		return e
	}
	return f.err
}

func (f *fakeClient) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
}

func (f *fakeClient) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeClient) Register(workflow.Workflow) error { return nil }
func (f *fakeClient) Start(context.Context) error      { return nil }
func (f *fakeClient) Enqueue(context.Context, workflow.Job) (workflow.RunRef, error) {
	return workflow.RunRef{}, nil
}
func (f *fakeClient) Status(context.Context, workflow.RunRef) (workflow.Status, error) {
	return "", nil
}
func (f *fakeClient) Signal(context.Context, workflow.Event) error { return nil }
func (f *fakeClient) Schedule(context.Context, workflow.Schedule) (workflow.ScheduleID, error) {
	return "", nil
}
func (f *fakeClient) Unschedule(context.Context, workflow.ScheduleID) error { return nil }
func (f *fakeClient) Capabilities() workflow.Caps                           { return f.caps }

func (f *fakeClient) Cancel(_ context.Context, ref workflow.RunRef) error {
	f.record("Cancel " + ref.RunID.String())
	return f.err
}

// Runs filters with RunQuery.Match and pages by offset, newest first as
// the slice is ordered.
func (f *fakeClient) Runs(_ context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	f.record("Runs")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, q)
	if f.err != nil {
		return workflow.RunPage{}, f.err
	}
	var match []workflow.RunInfo
	for _, r := range f.runs {
		if !q.Ref.IsZero() && r.Ref != q.Ref {
			continue
		}
		if q.Match(r) {
			match = append(match, r)
		}
	}
	off := 0
	if q.Cursor != "" {
		off, _ = strconv.Atoi(q.Cursor)
	}
	end := min(off+q.PageLimit(), len(match))
	page := workflow.RunPage{Runs: match[min(off, len(match)):end]}
	if end < len(match) {
		page.Next = strconv.Itoa(end)
	}
	return page, nil
}

func (f *fakeClient) Retry(_ context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	f.record("Retry " + ref.RunID.String())
	return ref, f.errFor(ref)
}

func (f *fakeClient) Delete(_ context.Context, ref workflow.RunRef) error {
	f.record("Delete " + ref.RunID.String())
	return f.errFor(ref)
}

func (f *fakeClient) CancelAll(_ context.Context, q workflow.RunQuery) (int, error) {
	f.record("CancelAll")
	f.mu.Lock()
	f.queries = append(f.queries, q)
	f.mu.Unlock()
	return 3, f.err
}

func (f *fakeClient) Pause(_ context.Context, t workflow.PauseTarget) error {
	f.record("Pause")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pauses = append(f.pauses, t)
	return f.err
}

func (f *fakeClient) Resume(context.Context, workflow.PauseTarget) error {
	f.record("Resume")
	return f.err
}

func (f *fakeClient) Paused(context.Context) ([]workflow.PauseTarget, error) {
	f.record("Paused")
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]workflow.PauseTarget(nil), f.pauses...), f.err
}

func (f *fakeClient) Schedules(context.Context) ([]workflow.ScheduleInfo, error) {
	f.record("Schedules")
	return f.schedules, f.err
}

func (f *fakeClient) PauseSchedule(_ context.Context, t workflow.ScheduleTarget) error {
	f.record("PauseSchedule " + t.ID.String())
	return f.err
}

func (f *fakeClient) ResumeSchedule(_ context.Context, t workflow.ScheduleTarget) error {
	f.record("ResumeSchedule " + t.ID.String())
	return f.err
}

func (f *fakeClient) TriggerSchedule(_ context.Context, id workflow.ScheduleID) (workflow.RunRef, error) {
	f.record("TriggerSchedule " + id.String())
	return workflow.RunRef{RunID: "t-" + workflow.RunID(id)}, f.err
}

func (f *fakeClient) ForceComplete(_ context.Context, ref workflow.RunRef) error {
	f.record("ForceComplete " + ref.RunID.String())
	return f.err
}

func (f *fakeClient) ForceFail(_ context.Context, ref workflow.RunRef, reason string) error {
	f.record("ForceFail " + ref.RunID.String() + " " + reason)
	return f.err
}

func (f *fakeClient) Nodes(context.Context) ([]workflow.NodeInfo, error) {
	f.record("Nodes")
	return []workflow.NodeInfo{{ID: "web-2:7", Mode: workflow.NodeWorker, Workers: 4}, {ID: "web-1:9", Mode: workflow.NodeFull, Queues: []string{"emails"}}}, f.err
}

func (f *fakeClient) Reschedule(_ context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	f.record("Reschedule " + ref.RunID.String() + " " + at.UTC().Format(time.RFC3339))
	return ref, f.errFor(ref)
}

func (f *fakeClient) CancelStep(_ context.Context, ref workflow.RunRef, step string) error {
	f.record("CancelStep " + ref.RunID.String() + " " + step)
	return f.errFor(ref)
}

// bareClient is a Client with no optional interface at all, to prove a
// declared capability without its interface is still refused.
type bareClient struct {
	workflow.Client
	caps workflow.Caps
}

func (b bareClient) Capabilities() workflow.Caps { return b.caps }
