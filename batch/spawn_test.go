package batch_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// spawnStub is a groupStub that is also a GroupSpawner: it records every
// SpawnGroup and CancelGroup call.
type spawnStub struct {
	*groupStub
	on        bool
	spawnErr  error
	spawns    [][]workflow.Job
	cancels   []workflow.RunRef
	cancelIDs []string
}

var _ batch.GroupSpawner = (*spawnStub)(nil)

func newSpawnStub() *spawnStub {
	return &spawnStub{groupStub: &groupStub{retryStub: newRetryStub(), refs: allRefs}, on: true}
}

func (s *spawnStub) SpawnsGroups() bool { return s.on }

func (s *spawnStub) SpawnGroup(_ context.Context, group string, jobs []workflow.Job) (workflow.RunRef, error) {
	s.spawns = append(s.spawns, jobs)
	if s.spawnErr != nil {
		return workflow.RunRef{}, s.spawnErr
	}
	return workflow.RunRef{RunID: workflow.RunID("parent-" + group)}, nil
}

func (s *spawnStub) CancelGroup(_ context.Context, group string, parent workflow.RunRef, _ time.Time) error {
	s.cancels = append(s.cancels, parent)
	s.cancelIDs = append(s.cancelIDs, group)
	return nil
}

// labelSrc is a LabelSource that does nothing: tests report job ends
// through RunFinished themselves.
type labelSrc struct{}

func (labelSrc) Attach(context.Context, *batch.Manager) error { return nil }
func (labelSrc) FindsJobsByLabel()                            {}

func spawnManager(t *testing.T, s *spawnStub, opts ...batch.Option) (*batch.Manager, *batch.MemoryStore) {
	t.Helper()
	store := batch.NewMemoryStore()
	m := batch.New(s, store, append([]batch.Option{batch.WithCompletionSource(labelSrc{})}, opts...)...)
	if err := m.Attach(context.Background()); err != nil {
		t.Fatal(err)
	}
	return m, store
}

func twoJobs() []workflow.Job { return []workflow.Job{{Workflow: "a"}, {Workflow: "a"}} }

func TestSpawn_RefusedWithoutALabelSource(t *testing.T) {
	s := newSpawnStub()
	m := batch.New(s, batch.NewMemoryStore())
	if err := m.Attach(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Dispatch(context.Background(), batch.Batch{Jobs: twoJobs()}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Dispatch = %v, want ErrInvalid without a LabelSource", err)
	}
	if len(s.spawns)+len(s.groups) != 0 {
		t.Fatal("a refused dispatch started something")
	}
}

func TestSpawn_StartsOneCoordinatorAndCompletesByLabel(t *testing.T) {
	ctx := context.Background()
	s := newSpawnStub()
	m, store := spawnManager(t, s)
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: twoJobs(), Then: "then", Finally: "finally"})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.spawns) != 1 || len(s.groups) != 0 || len(s.enqueued("a")) != 0 {
		t.Fatalf("spawns=%d groups=%d enqueues=%d, want one coordinator only", len(s.spawns), len(s.groups), len(s.enqueued("a")))
	}
	for i, j := range s.spawns[0] {
		if j.Labels[batch.LabelBatch] != string(id) || j.Labels[batch.LabelIndex] != strconv.Itoa(i) {
			t.Fatalf("spawned job %d labels %v", i, j.Labels)
		}
	}
	st, _ := statuses(t, store, id)
	if !st.Coordinated || !st.Dispatched || st.Parent.RunID != workflow.RunID("parent-"+string(id)) || st.Spec != nil {
		t.Fatalf("state %+v, want coordinated, dispatched, the parent saved and specs dropped", st)
	}
	for i := range 2 {
		if err := m.RunFinished(ctx, map[string]string{batch.LabelBatch: string(id), batch.LabelIndex: strconv.Itoa(i)}, workflow.StatusSucceeded); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.enqueued("then")) != 1 || len(s.enqueued("finally")) != 1 {
		t.Fatalf("then=%d finally=%d, want each once", len(s.enqueued("then")), len(s.enqueued("finally")))
	}
}

func TestSpawn_SwitchedOffOrUnsupportedFallsBackToGroups(t *testing.T) {
	ctx := context.Background()
	for name, s := range map[string]*spawnStub{
		"off":         func() *spawnStub { s := newSpawnStub(); s.on = false; return s }(),
		"unsupported": func() *spawnStub { s := newSpawnStub(); s.spawnErr = workflow.Unsupported("x", "too big"); return s }(),
	} {
		t.Run(name, func(t *testing.T) {
			m, store := spawnManager(t, s)
			id, err := m.Dispatch(ctx, batch.Batch{Jobs: twoJobs()})
			if err != nil {
				t.Fatal(err)
			}
			if len(s.groups) != 1 {
				t.Fatalf("groups = %d, want the RunGroup fallback", len(s.groups))
			}
			st, _ := statuses(t, store, id)
			if st.Coordinated || !st.Parent.IsZero() || st.Jobs[0].Ref.IsZero() {
				t.Fatalf("state %+v, want a plain group dispatch", st)
			}
		})
	}
}

func TestSpawn_FailedSpawnCancelsTheBatchAndTheGroupByLabel(t *testing.T) {
	ctx := context.Background()
	s := newSpawnStub()
	s.spawnErr = errEngineDown
	m, store := spawnManager(t, s)
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: twoJobs(), Finally: "finally"})
	if !errors.Is(err, errEngineDown) || id == "" {
		t.Fatalf("Dispatch = %q, %v; want the id and the spawn error", id, err)
	}
	st, s2 := statuses(t, store, id)
	if !st.Cancelled || s2[0] != workflow.StatusCancelled || s2[1] != workflow.StatusCancelled || !st.Dispatched {
		t.Fatalf("state %+v, want the batch cancelled and every job cancelled", st)
	}
	if len(s.cancels) != 1 || !s.cancels[0].IsZero() || s.cancelIDs[0] != string(id) {
		t.Fatalf("CancelGroup calls %v %v, want one by label with no parent", s.cancels, s.cancelIDs)
	}
	if len(s.enqueued("finally")) != 1 {
		t.Fatal("Finally did not run after the failed spawn")
	}
}

func TestSpawn_CancelReachesTheGroup(t *testing.T) {
	ctx := context.Background()
	s := newSpawnStub()
	m, _ := spawnManager(t, s)
	id, err := m.Dispatch(ctx, batch.Batch{Jobs: twoJobs()})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if len(s.cancels) != 1 || s.cancels[0].RunID != workflow.RunID("parent-"+string(id)) {
		t.Fatalf("CancelGroup calls %v, want one with the parent", s.cancels)
	}
	if len(s.cancelled) != 0 {
		t.Fatalf("per-run cancels %v, want none (children have no refs)", s.cancelled)
	}
}

func TestSpawn_ResumeCallsSpawnGroupAgain(t *testing.T) {
	ctx := context.Background()
	clk := newFake()
	s := newSpawnStub()
	store := batch.NewMemoryStore()
	m := batch.New(s, store, batch.WithClock(clk))
	id := deadDispatch(t, clk, store, 2, false, true)
	if _, err := store.Update(ctx, id, func(st *batch.State) error {
		st.Coordinated = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.ResumeDispatches(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.spawns) != 1 || len(s.groups) != 0 || len(s.enqueued("a")) != 0 {
		t.Fatalf("spawns=%d groups=%d enqueues=%d, want only SpawnGroup again", len(s.spawns), len(s.groups), len(s.enqueued("a")))
	}
	st, sts := statuses(t, store, id)
	if !st.Dispatched || st.Parent.RunID != workflow.RunID(fmt.Sprintf("parent-%s", id)) {
		t.Fatalf("state %+v, want dispatched with the coordinator saved", st)
	}
	for i, x := range sts {
		if x != "" {
			t.Fatalf("job %d status %s, want none recorded", i, x)
		}
	}
}
