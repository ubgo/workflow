package hatchet_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/hatchet"
)

// coordTimeout is the coordinator attempt timeout of the live tests: the
// crash test waits about this long for the engine to retry the killed
// worker's coordinator.
const coordTimeout = 20 * time.Second

func newCoordEnv(t *testing.T) *groupEnv {
	t.Helper()
	return newGroupEnvWith(t, hatchet.Config{WorkerName: "wfcoord", BatchCoordinator: true, BatchCoordinatorTimeout: coordTimeout})
}

// coordState checks a coordinated dispatch: a parent saved, no job refs.
func (e *groupEnv) coordState(id batch.ID) batch.State {
	e.t.Helper()
	st, err := e.store.Get(e.ctx, id)
	if err != nil {
		e.t.Fatal(err)
	}
	if !st.Coordinated || st.Parent.IsZero() || !st.Dispatched {
		e.t.Fatalf("state %+v, want a dispatched coordinated batch with its parent", st)
	}
	for i, j := range st.Jobs {
		if !j.Ref.IsZero() {
			e.t.Fatalf("job %d has ref %v; children of a coordinator have none", i, j.Ref)
		}
	}
	return st
}

// childStatuses lists the batch's child runs by label (the coordinator
// itself has no job index) and returns their statuses by job index,
// failing on a job with more than one run.
func childStatuses(t *testing.T, c *hatchet.Client, id batch.ID) map[int]rest.V1TaskStatus {
	t.Helper()
	limit, f := int64(5000), false
	rows, err := c.Native().Runs().List(context.Background(), rest.V1WorkflowRunListParams{
		Since:              time.Now().Add(-time.Hour),
		AdditionalMetadata: &[]string{batch.LabelBatch + ":" + id.String()},
		Limit:              &limit,
		IncludePayloads:    &f,
	})
	if err != nil {
		t.Fatal(err)
	}
	out := map[int]rest.V1TaskStatus{}
	for _, r := range rows.Rows {
		if r.AdditionalMetadata == nil {
			continue
		}
		v, ok := (*r.AdditionalMetadata)[batch.LabelIndex].(string)
		if !ok {
			continue
		}
		i, err := strconv.Atoi(v)
		if err != nil {
			t.Fatal(err)
		}
		if _, dup := out[i]; dup {
			t.Fatalf("job %d has more than one engine run", i)
		}
		out[i] = r.Status
	}
	return out
}

// TestBatchCoordinatorLive runs batches through the coordinator
// (Config.BatchCoordinator) with the Hatchet BatchSource as the only
// completion source.
func TestBatchCoordinatorLive(t *testing.T) {
	t.Run("all succeed", func(t *testing.T) {
		e := newCoordEnv(t)
		id := e.dispatch(batch.Batch{Name: "ok", Jobs: e.jobs("ok", "ok", "ok")})
		e.coordState(id)
		p := wantCalls(t, e.waitFinally(id, liveWait), 1, 0)
		if p.Succeeded != 3 || p.Cancelled {
			t.Fatalf("progress %+v, want 3 succeeded", p)
		}
		if n := len(childStatuses(t, e.c, id)); n != 3 {
			t.Fatalf("engine lists %d children, want 3", n)
		}
	})

	t.Run("failing child cancels the rest", func(t *testing.T) {
		e := newCoordEnv(t)
		id := e.dispatch(batch.Batch{Name: "fail", Jobs: e.jobs("block", "bad", "block")})
		e.coordState(id)
		calls := e.waitFinally(id, liveWait)
		p := wantCalls(t, calls, 0, 1)
		if p.Failed != 1 || p.CancelledJobs != 2 || !p.Cancelled {
			t.Fatalf("Finally got %+v, want 1 failed and the 2 blocked jobs cancelled", p)
		}
		for i, s := range childStatuses(t, e.c, id) {
			if i != 1 && s != rest.V1TaskStatusCANCELLED {
				t.Fatalf("child %d engine status %s, want CANCELLED", i, s)
			}
		}
	})

	t.Run("batch cancel", func(t *testing.T) {
		e := newCoordEnv(t)
		id := e.dispatch(batch.Batch{Name: "cancel-all", Jobs: e.jobs("block", "block", "block")})
		e.coordState(id)
		waitUntil(t, liveWait, func() bool {
			running := 0
			for _, s := range childStatuses(t, e.c, id) {
				if s == rest.V1TaskStatusRUNNING {
					running++
				}
			}
			return running == 3
		}, func() string { return "the three children never all ran" })
		if err := e.m.Cancel(e.ctx, id); err != nil {
			t.Fatal(err)
		}
		p := wantCalls(t, e.waitFinally(id, liveWait), 0, 0)
		if p.CancelledJobs != 3 || !p.Cancelled {
			t.Fatalf("Finally got %+v, want every job cancelled", p)
		}
		for i, s := range childStatuses(t, e.c, id) {
			if s != rest.V1TaskStatusCANCELLED {
				t.Fatalf("child %d engine status %s, want CANCELLED", i, s)
			}
		}
	})
}

// Coordinator crash test: the helper's worker runs the coordinator of a
// batch of coordCrashJobs jobs (two bulk calls: 1000 + 1) and kills itself
// right after the first.
const (
	envCoordCrash  = "WFBATCH_COORD_CRASH"
	coordCrashJobs = 1001
)

// coordCrashWorkflows: a job marks done-<index>-<run> and returns; in the
// helper it blocks, as the helper dies before any could finish anyway.
func coordCrashWorkflows(dir, prefix string, helper bool) []workflow.Workflow {
	mark := func(name string) { _ = os.WriteFile(filepath.Join(dir, name), nil, 0o600) }
	job := func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		if helper {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		mark(fmt.Sprintf("done-%s-%s", info.Labels[batch.LabelIndex], info.RunID))
		return nil, nil
	}
	finally := func(context.Context, []byte) ([]byte, error) {
		mark("finally-" + uuid.NewString()[:8])
		return nil, nil
	}
	return []workflow.Workflow{
		{Name: prefix + "_job", Retry: workflow.RetryPolicy{Max: 3}, Timeout: crashStepTimeout, Steps: []workflow.Step{{Name: "a", Run: job}}},
		{Name: prefix + "_finally", Steps: []workflow.Step{{Name: "a", Run: finally}}},
	}
}

// TestBatchCoordinatorCrashHelper is the process
// TestBatchCoordinatorCrashLive kills. It does nothing unless started by
// that test.
func TestBatchCoordinatorCrashHelper(t *testing.T) {
	dir, prefix := os.Getenv(envCrashDir), os.Getenv(envCrashPrefix)
	if dir == "" || os.Getenv(envCoordCrash) == "" {
		t.Skip("only runs as TestBatchCoordinatorCrashLive's helper process")
	}
	ctx := context.Background()
	c := hatchet.New(hatchet.Config{WorkerName: "wfccrash-helper", BatchCoordinator: true, BatchCoordinatorTimeout: coordTimeout})
	store := newFileStore(filepath.Join(dir, "store.json"))
	m := batch.New(c, store, batch.WithCompletionSource(c.BatchSource(store, liveSourcePoll, nil)))
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	for _, wf := range coordCrashWorkflows(dir, prefix, true) {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	hatchet.SetAfterChunk(c, func() {
		_ = os.WriteFile(filepath.Join(dir, "chunk-1"), nil, 0o600)
		p, _ := os.FindProcess(os.Getpid())
		_ = p.Kill()
		select {}
	})
	jobs := make([]workflow.Job, coordCrashJobs)
	for i := range jobs {
		jobs[i] = workflow.Job{Workflow: prefix + "_job"}
	}
	if _, err := m.Dispatch(ctx, batch.Batch{Name: "coord-crash", Jobs: jobs, Finally: prefix + "_finally"}); err != nil {
		t.Fatal(err)
	}
	select {} // the coordinator runs on this worker and kills the process
}

// TestBatchCoordinatorCrashLive kills the worker running a batch's
// coordinator between its two bulk calls. A fresh process sharing the
// batch store runs the engine's retry of the coordinator, which gets the
// first 1000 children back by their keys and starts only the last one;
// every job then runs exactly once, each has one engine run, and Finally
// runs once.
func TestBatchCoordinatorCrashLive(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live crash test needs a Hatchet engine (task test:live)", envToken)
	}
	dir := t.TempDir()
	prefix := "wfccrash_" + uuid.NewString()[:8]
	helper := exec.Command(os.Args[0], "-test.run=^TestBatchCoordinatorCrashHelper$")
	helper.Env = append(os.Environ(), envCrashDir+"="+dir, envCrashPrefix+"="+prefix, envCoordCrash+"=1")
	out, _ := helper.CombinedOutput()
	if _, err := os.Stat(filepath.Join(dir, "chunk-1")); err != nil {
		t.Fatalf("the helper was not killed after the coordinator's first bulk call: %v\n%s", err, out)
	}
	all, err := newFileStore(filepath.Join(dir, "store.json")).Unfinished(context.Background())
	if err != nil || len(all) != 1 {
		t.Fatalf("store holds %d open batches (%v), want the helper's", len(all), err)
	}
	id := all[0].ID
	if !all[0].Coordinated || all[0].Parent.IsZero() {
		t.Fatalf("helper left %+v, want a coordinated batch with its parent", all[0])
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := hatchet.New(hatchet.Config{WorkerName: "wfccrash", BatchCoordinator: true, BatchCoordinatorTimeout: coordTimeout})
	store := newFileStore(filepath.Join(dir, "store.json"))
	m := batch.New(c, store, batch.WithCompletionSource(c.BatchSource(store, liveSourcePoll, nil)))
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	for _, wf := range coordCrashWorkflows(dir, prefix, false) {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	finals := func() int {
		n, _ := filepath.Glob(filepath.Join(dir, "finally-*"))
		return len(n)
	}
	waitUntil(t, 8*time.Minute, func() bool { return finals() > 0 }, func() string {
		p, _ := m.Progress(ctx, id)
		return fmt.Sprintf("Finally did not run after the coordinator crash; progress %+v", p)
	})
	time.Sleep(batchQuiet)
	if n := finals(); n != 1 {
		t.Fatalf("Finally ran %d times, want once", n)
	}
	p, err := m.Progress(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if p.Succeeded != coordCrashJobs || !p.Finished {
		t.Fatalf("progress %+v, want all %d jobs succeeded", p, coordCrashJobs)
	}
	for i := range coordCrashJobs {
		done, _ := filepath.Glob(filepath.Join(dir, "done-"+strconv.Itoa(i)+"-*"))
		if len(done) != 1 {
			t.Fatalf("job %d completed %d times, want exactly once", i, len(done))
		}
	}
	if n := len(childStatuses(t, c, id)); n != coordCrashJobs {
		t.Fatalf("engine lists %d children, want %d", n, coordCrashJobs)
	}
}
