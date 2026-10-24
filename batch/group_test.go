package batch_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// groupStub is a retryStub that also runs groups: it records every RunGroup
// call and returns refs, an error, or both, as the test sets.
type groupStub struct {
	*retryStub
	groups [][]workflow.Job
	refs   func(jobs []workflow.Job) []workflow.RunRef
	err    error
	ids    []string
}

var _ batch.GroupRunner = (*groupStub)(nil)

func (g *groupStub) RunGroup(_ context.Context, group string, jobs []workflow.Job) ([]workflow.RunRef, error) {
	g.groups = append(g.groups, jobs)
	g.ids = append(g.ids, group)
	var refs []workflow.RunRef
	if g.refs != nil {
		refs = g.refs(jobs)
	}
	return refs, g.err
}

func allRefs(jobs []workflow.Job) []workflow.RunRef {
	out := make([]workflow.RunRef, len(jobs))
	for i := range jobs {
		out[i] = workflow.RunRef{RunID: workflow.RunID(fmt.Sprintf("child-%d", i))}
	}
	return out
}

func TestGroupRunner_EligibleBatchRunsAsOneGroup(t *testing.T) {
	ctx := context.Background()
	g := &groupStub{retryStub: newRetryStub(), refs: allRefs}
	store := batch.NewMemoryStore()
	m := batch.New(g, store)
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "a"}, {Workflow: "b", Labels: map[string]string{"app": "x"}}}, Then: "then"})
	if err != nil {
		t.Fatal(err)
	}
	if len(g.groups) != 1 || len(g.enqueued("a"))+len(g.enqueued("b")) != 0 {
		t.Fatalf("groups=%d, per-job enqueues=%d; want one group and none", len(g.groups), len(g.enqueued("a"))+len(g.enqueued("b")))
	}
	if g.ids[0] != string(id) {
		t.Fatalf("group = %q, want the batch id %q", g.ids[0], id)
	}
	for i, j := range g.groups[0] {
		if j.Labels[batch.LabelBatch] != string(id) || j.Labels[batch.LabelIndex] != fmt.Sprint(i) {
			t.Fatalf("job %d labels = %v", i, j.Labels)
		}
	}
	if g.groups[0][1].Labels["app"] != "x" {
		t.Fatal("app labels lost on the way to the group")
	}
	st, _ := store.Get(ctx, id)
	for i, j := range st.Jobs {
		if j.Ref.RunID != workflow.RunID(fmt.Sprintf("child-%d", i)) {
			t.Fatalf("job %d ref = %v", i, j.Ref)
		}
	}
	for i := range 2 {
		if err := finish(t, m, id, i, workflow.StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
	if len(g.enqueued("then")) != 1 {
		t.Fatal("Then did not run after the group's children reported")
	}
}

// TestGroupRunner_FallsBackToEnqueue: a job with an idempotency key or a
// delayed start, or a runner that declines with ErrUnsupported, sends the
// batch through Enqueue one job at a time.
func TestGroupRunner_FallsBackToEnqueue(t *testing.T) {
	cases := []struct {
		name      string
		job       workflow.Job
		err       error
		wantGroup int
	}{
		{"idempotency key", workflow.Job{Workflow: "a", IdempotencyKey: "k"}, nil, 0},
		{"delayed start", workflow.Job{Workflow: "a", RunAt: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}, nil, 0},
		{"runner declines", workflow.Job{Workflow: "a"}, &workflow.UnsupportedError{Field: "RunGroup", Reason: "mixed workflows"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := &groupStub{retryStub: newRetryStub(), err: tc.err}
			m := batch.New(g, batch.NewMemoryStore())
			if _, err := m.Dispatch(context.Background(), batch.Batch{Jobs: []workflow.Job{tc.job, {Workflow: "a"}}}); err != nil {
				t.Fatal(err)
			}
			if len(g.groups) != tc.wantGroup || len(g.enqueued("a")) != 2 {
				t.Fatalf("groups=%d (want %d), enqueues=%d (want 2)", len(g.groups), tc.wantGroup, len(g.enqueued("a")))
			}
		})
	}
}

// TestGroupRunner_PartialStartCancelsBatch: a group that started only some
// children returns the error, cancels the started ones, records the rest
// as cancelled, and still runs Finally once the started ones report.
func TestGroupRunner_PartialStartCancelsBatch(t *testing.T) {
	ctx := context.Background()
	g := &groupStub{retryStub: newRetryStub(), err: errEngineDown, refs: func(jobs []workflow.Job) []workflow.RunRef {
		refs := allRefs(jobs)
		refs[1] = workflow.RunRef{}
		return refs
	}}
	store := batch.NewMemoryStore()
	m := batch.New(g, store)
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: "a"}, {Workflow: "a"}}, Then: "then", Finally: "fin"})
	if !errors.Is(err, errEngineDown) || id == "" {
		t.Fatalf("Dispatch = %q, %v; want the id and the group error", id, err)
	}
	if len(g.cancelled) != 1 || g.cancelled[0].RunID != "child-0" {
		t.Fatalf("cancelled = %v, want the started child", g.cancelled)
	}
	p, _ := m.Progress(ctx, id)
	if !p.Cancelled || p.CancelledJobs != 1 || p.Pending != 1 {
		t.Fatalf("progress = %+v", p)
	}
	if err := finish(t, m, id, 0, workflow.StatusCancelled); err != nil {
		t.Fatal(err)
	}
	if len(g.enqueued("fin")) != 1 || len(g.enqueued("then")) != 0 {
		t.Fatalf("finally=%d then=%d; want 1 and 0", len(g.enqueued("fin")), len(g.enqueued("then")))
	}
}

// TestGroupRunner_WrongRefCountIsAnError: refs that cannot be matched to
// jobs are refused rather than placed on the wrong job.
func TestGroupRunner_WrongRefCountIsAnError(t *testing.T) {
	g := &groupStub{retryStub: newRetryStub(), refs: func([]workflow.Job) []workflow.RunRef { return allRefs(make([]workflow.Job, 1)) }}
	m := batch.New(g, batch.NewMemoryStore())
	id, err := m.Dispatch(context.Background(), batch.Batch{Jobs: []workflow.Job{{Workflow: "a"}, {Workflow: "a"}}})
	if err == nil {
		t.Fatal("Dispatch accepted 1 ref for 2 jobs")
	}
	if p, _ := m.Progress(context.Background(), id); !p.Cancelled {
		t.Fatalf("batch not cancelled: %+v", p)
	}
}

// memGroup runs a group on the memory driver by enqueuing each job, so
// the end-to-end test proves children report through the run-finished hook
// like any enqueued job.
type memGroup struct{ *counting }

func (g memGroup) RunGroup(ctx context.Context, _ string, jobs []workflow.Job) ([]workflow.RunRef, error) {
	refs := make([]workflow.RunRef, len(jobs))
	for i, j := range jobs {
		ref, err := g.Client.Enqueue(ctx, j)
		if err != nil {
			return refs, err
		}
		refs[i] = ref
	}
	return refs, nil
}

func TestGroupRunner_EndToEndOnMemoryHook(t *testing.T) {
	e := newEnvWith(t, func(c *counting) workflow.Client { return memGroup{c} })
	e.dispatch(false, wfOK, wfOK)
	if p := e.waitFinally(); p.Succeeded != 2 || !p.Finished {
		t.Fatalf("Finally payload = %+v", p)
	}
	if n := e.client.count(wfOK); n != 0 {
		t.Fatalf("jobs went through Enqueue %d times; want the group path", n)
	}
	e.wantCounts(1, 0, 1)
}
