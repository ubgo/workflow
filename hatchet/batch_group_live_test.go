package hatchet_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/hatchet"
)

// liveSourcePoll is the BatchSource interval of the live batch tests.
const liveSourcePoll = 500 * time.Millisecond

// groupEnv is one live batch setup: a client, its Manager on a MemoryStore
// fed by the Hatchet BatchSource alone (no hook, so the source is what is
// proven), and the callbacks each batch fired.
type groupEnv struct {
	t      *testing.T
	prefix string
	c      *hatchet.Client
	m      *batch.Manager
	store  *batch.MemoryStore
	ctx    context.Context

	mu    sync.Mutex
	calls map[string][]batch.Progress
}

// newGroupEnv registers ok, bad, block and the three callbacks, plus
// extra, and starts the client.
func newGroupEnv(t *testing.T, extra ...workflow.Workflow) *groupEnv {
	t.Helper()
	return newGroupEnvWith(t, hatchet.Config{WorkerName: "wfgrp"}, extra...)
}

// newGroupEnvWith is newGroupEnv with the driver built from cfg.
func newGroupEnvWith(t *testing.T, cfg hatchet.Config, extra ...workflow.Workflow) *groupEnv {
	t.Helper()
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live batch test needs a Hatchet engine (task test:live)", envToken)
	}
	e := &groupEnv{t: t, prefix: "wfgrp_" + uuid.NewString()[:8], calls: map[string][]batch.Progress{}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	e.ctx = ctx
	e.c = hatchet.New(cfg)
	e.store = batch.NewMemoryStore()
	e.m = batch.New(e.c, e.store, batch.WithCompletionSource(e.c.BatchSource(e.store, liveSourcePoll, nil)))
	if err := e.m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	callback := func(kind string) workflow.Handler {
		return func(_ context.Context, in []byte) ([]byte, error) {
			var p batch.Progress
			if err := json.Unmarshal(in, &p); err != nil {
				return nil, workflow.Permanent(err)
			}
			e.mu.Lock()
			e.calls[kind] = append(e.calls[kind], p)
			e.mu.Unlock()
			return nil, nil
		}
	}
	wfs := append([]workflow.Workflow{
		e.one("ok", func(context.Context, []byte) ([]byte, error) { return []byte(`"ok"`), nil }),
		e.one("bad", func(context.Context, []byte) ([]byte, error) {
			return nil, workflow.Permanent(errors.New("job failed"))
		}),
		e.one("block", func(ctx context.Context, _ []byte) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}),
		e.one("then", callback("then")),
		e.one("catch", callback("catch")),
		e.one("finally", callback("finally")),
	}, extra...)
	for _, wf := range wfs {
		if err := e.c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *groupEnv) name(n string) string { return e.prefix + "_" + n }

func (e *groupEnv) one(name string, h workflow.Handler) workflow.Workflow {
	return workflow.Workflow{Name: e.name(name), Steps: []workflow.Step{{Name: "a", Run: h}}}
}

func (e *groupEnv) jobs(names ...string) []workflow.Job {
	out := make([]workflow.Job, len(names))
	for i, n := range names {
		out[i] = workflow.Job{Workflow: e.name(n)}
	}
	return out
}

func (e *groupEnv) dispatch(b batch.Batch) batch.ID {
	e.t.Helper()
	b.Then, b.Catch, b.Finally = e.name("then"), e.name("catch"), e.name("finally")
	id, err := e.m.Dispatch(e.ctx, b)
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

// refs returns the run refs Dispatch saved, after checking that every job
// got a distinct engine run (RunGroup started each exactly once).
func (e *groupEnv) refs(id batch.ID) []workflow.RunRef {
	e.t.Helper()
	st, err := e.store.Get(e.ctx, id)
	if err != nil {
		e.t.Fatal(err)
	}
	seen := map[workflow.RunID]bool{}
	out := make([]workflow.RunRef, len(st.Jobs))
	for i, j := range st.Jobs {
		if j.Ref.RunID == "" || seen[j.Ref.RunID] {
			e.t.Fatalf("job %d has run %q: want a distinct engine run per job", i, j.Ref.RunID)
		}
		seen[j.Ref.RunID] = true
		out[i] = j.Ref
	}
	return out
}

// waitFinally waits for Finally, then watches batchQuiet longer so a
// duplicate or stray callback shows, and returns the calls.
func (e *groupEnv) waitFinally(id batch.ID, wait time.Duration) map[string][]batch.Progress {
	e.t.Helper()
	waitUntil(e.t, wait, func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		return len(e.calls["finally"]) > 0
	}, func() string {
		p, _ := e.m.Progress(e.ctx, id)
		return fmt.Sprintf("Finally did not run; progress %+v", p)
	})
	time.Sleep(batchQuiet)
	e.mu.Lock()
	defer e.mu.Unlock()
	out := map[string][]batch.Progress{}
	for k, v := range e.calls {
		out[k] = append([]batch.Progress(nil), v...)
	}
	return out
}

func waitUntil(t *testing.T, wait time.Duration, done func() bool, why func() string) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("after %s: %s", wait, why())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func wantCalls(t *testing.T, calls map[string][]batch.Progress, then, catch int) batch.Progress {
	t.Helper()
	if n := len(calls["then"]); n != then {
		t.Fatalf("Then ran %d times, want %d", n, then)
	}
	if n := len(calls["catch"]); n != catch {
		t.Fatalf("Catch ran %d times, want %d", n, catch)
	}
	if n := len(calls["finally"]); n != 1 {
		t.Fatalf("Finally ran %d times, want once", n)
	}
	p := calls["finally"][0]
	if !p.Finished || p.Pending != 0 {
		t.Fatalf("Finally got progress %+v, want every job finished", p)
	}
	return p
}

// TestBatchGroupLive runs batches through the driver's RunGroup (bulk
// triggers) with the Hatchet BatchSource as the only completion source.
func TestBatchGroupLive(t *testing.T) {
	t.Run("all succeed", func(t *testing.T) {
		e := newGroupEnv(t)
		id := e.dispatch(batch.Batch{Name: "ok", Jobs: e.jobs("ok", "ok", "ok")})
		e.refs(id)
		p := wantCalls(t, e.waitFinally(id, liveWait), 1, 0)
		if p.Succeeded != 3 || p.Cancelled {
			t.Fatalf("progress %+v, want 3 succeeded", p)
		}
	})

	t.Run("failing child cancels the rest", func(t *testing.T) {
		e := newGroupEnv(t)
		id := e.dispatch(batch.Batch{Name: "fail", Jobs: e.jobs("block", "bad", "block")})
		e.refs(id)
		calls := e.waitFinally(id, liveWait)
		p := wantCalls(t, calls, 0, 1)
		if c := calls["catch"][0]; c.Failed != 1 || !c.Cancelled {
			t.Fatalf("Catch got %+v, want the failure and the batch cancelled", c)
		}
		if p.Failed != 1 || p.CancelledJobs != 2 || !p.Cancelled {
			t.Fatalf("Finally got %+v, want 1 failed and the 2 blocked jobs cancelled", p)
		}
	})

	t.Run("allow failures", func(t *testing.T) {
		e := newGroupEnv(t)
		id := e.dispatch(batch.Batch{Name: "allow", AllowFailures: true, Jobs: e.jobs("ok", "bad", "ok", "ok")})
		e.refs(id)
		calls := e.waitFinally(id, liveWait)
		p := wantCalls(t, calls, 0, 1)
		if p.Failed != 1 || p.Succeeded != 3 || p.Cancelled || p.CancelledJobs != 0 {
			t.Fatalf("Finally got %+v, want 3 succeeded, 1 failed, nothing cancelled", p)
		}
	})

	t.Run("cancelled child", func(t *testing.T) {
		e := newGroupEnv(t)
		id := e.dispatch(batch.Batch{Name: "cancel-one", Jobs: e.jobs("ok", "block")})
		refs := e.refs(id)
		waitRunning(t, e.c, refs[1])
		if err := e.c.Cancel(e.ctx, refs[1]); err != nil {
			t.Fatal(err)
		}
		p := wantCalls(t, e.waitFinally(id, liveWait), 0, 0)
		if p.Succeeded != 1 || p.CancelledJobs != 1 || p.Failed != 0 || p.Cancelled {
			t.Fatalf("Finally got %+v, want 1 succeeded and 1 cancelled job, no Catch", p)
		}
	})

	t.Run("batch cancel", func(t *testing.T) {
		e := newGroupEnv(t)
		id := e.dispatch(batch.Batch{Name: "cancel-all", Jobs: e.jobs("block", "block", "block")})
		refs := e.refs(id)
		for _, r := range refs {
			waitRunning(t, e.c, r)
		}
		if err := e.m.Cancel(e.ctx, id); err != nil {
			t.Fatal(err)
		}
		p := wantCalls(t, e.waitFinally(id, liveWait), 0, 0)
		if p.CancelledJobs != 3 || !p.Cancelled {
			t.Fatalf("Finally got %+v, want every job cancelled", p)
		}
		for i, r := range refs {
			if s, err := e.c.Status(e.ctx, r); err != nil || s != workflow.StatusCancelled {
				t.Fatalf("job %d engine status %q, %v; want cancelled", i, s, err)
			}
		}
	})

	t.Run("chunked by size", func(t *testing.T) {
		e := newGroupEnv(t)
		// About 800 KiB each once base64-encoded: RunGroup's 2 MiB chunk
		// holds two, so three jobs take two bulk triggers.
		payload, _ := json.Marshal(strings.Repeat("x", 600<<10))
		jobs := e.jobs("ok", "ok", "ok")
		for i := range jobs {
			jobs[i].Payload = payload
		}
		id := e.dispatch(batch.Batch{Name: "big", Jobs: jobs})
		e.refs(id)
		p := wantCalls(t, e.waitFinally(id, liveWait), 1, 0)
		if p.Succeeded != 3 {
			t.Fatalf("progress %+v, want 3 succeeded", p)
		}
	})

	t.Run("chunked by count", func(t *testing.T) {
		e := newGroupEnv(t)
		// 1001 jobs take two bulk triggers (1000 + 1), and the source
		// pages through more than one List page (200 runs) per batch.
		const n = 1001
		names := make([]string, n)
		for i := range names {
			names[i] = "ok"
		}
		id := e.dispatch(batch.Batch{Name: "many", Jobs: e.jobs(names...)})
		e.refs(id)
		p := wantCalls(t, e.waitFinally(id, 5*time.Minute), 1, 0)
		if p.Succeeded != n {
			t.Fatalf("progress %+v, want %d succeeded", p, n)
		}
	})
}

// waitRunning waits until the engine reports ref running, so a cancel hits
// a live step rather than a queued run.
func waitRunning(t *testing.T, c *hatchet.Client, ref workflow.RunRef) {
	t.Helper()
	waitUntil(t, liveWait, func() bool {
		s, err := c.Status(context.Background(), ref)
		return err == nil && s == workflow.StatusRunning
	}, func() string { return "run " + ref.RunID.String() + " never ran" })
}

// Crash test settings: the helper process that dispatches and is killed,
// reusing crashStepTimeout (live_test.go) for the engine to reclaim its
// stuck steps.
const (
	envCrashDir    = "WFBATCH_CRASH_DIR"
	envCrashPrefix = "WFBATCH_CRASH_PREFIX"
	crashJobs      = 4
)

// crashWorkflows are the workflows of the crash test. In the helper
// process every slow attempt blocks until the process is killed; in the
// test process it succeeds. Each attempt leaves a marker file, so the test
// can count starts and completions per job across both processes.
func crashWorkflows(dir, prefix string, helper bool) []workflow.Workflow {
	mark := func(name string) {
		_ = os.WriteFile(filepath.Join(dir, name), nil, 0o600)
	}
	slow := func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		idx := info.Labels[batch.LabelIndex]
		if helper {
			mark(fmt.Sprintf("start-%s-%d", idx, info.Attempt))
			select {}
		}
		mark(fmt.Sprintf("done-%s-%s", idx, info.RunID))
		return in, nil
	}
	finally := func(context.Context, []byte) ([]byte, error) {
		mark("finally-" + uuid.NewString()[:8])
		return nil, nil
	}
	return []workflow.Workflow{
		{Name: prefix + "_slow", Retry: workflow.RetryPolicy{Max: 3}, Timeout: crashStepTimeout, Steps: []workflow.Step{{Name: "a", Run: slow}}},
		{Name: prefix + "_finally", Steps: []workflow.Step{{Name: "a", Run: finally}}},
	}
}

// TestBatchCrashHelper is the process TestBatchCrashLive kills: it
// dispatches the batch and blocks in every step. It does nothing unless
// started by that test.
func TestBatchCrashHelper(t *testing.T) {
	dir, prefix := os.Getenv(envCrashDir), os.Getenv(envCrashPrefix)
	if dir == "" {
		t.Skip("only runs as TestBatchCrashLive's helper process")
	}
	ctx := context.Background()
	c := hatchet.New(hatchet.Config{WorkerName: "wfcrash-helper"})
	store := newFileStore(filepath.Join(dir, "store.json"))
	m := batch.New(c, store, batch.WithCompletionSource(c.BatchSource(store, liveSourcePoll, nil)))
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	for _, wf := range crashWorkflows(dir, prefix, true) {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	jobs := make([]workflow.Job, crashJobs)
	for i := range jobs {
		jobs[i] = workflow.Job{Workflow: prefix + "_slow"}
	}
	id, err := m.Dispatch(ctx, batch.Batch{Name: "crash", Jobs: jobs, Finally: prefix + "_finally"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "batch-id"), []byte(id), 0o600); err != nil {
		t.Fatal(err)
	}
	select {}
}

// TestBatchCrashLive kills the process that dispatched a batch, and whose
// worker is running every job, with SIGKILL mid-step. A fresh process
// sharing the batch store then finishes the batch: every job completes
// exactly once, no job gets a second engine run, and Finally runs once.
func TestBatchCrashLive(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live crash test needs a Hatchet engine (task test:live)", envToken)
	}
	dir := t.TempDir()
	prefix := "wfcrash_" + uuid.NewString()[:8]
	helper := exec.Command(os.Args[0], "-test.run=^TestBatchCrashHelper$")
	helper.Env = append(os.Environ(), envCrashDir+"="+dir, envCrashPrefix+"="+prefix)
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	killed := false
	t.Cleanup(func() {
		if !killed {
			_ = helper.Process.Kill()
			_ = helper.Wait()
		}
	})
	starts := func() int {
		n, _ := filepath.Glob(filepath.Join(dir, "start-*"))
		return len(n)
	}
	waitUntil(t, liveWait, func() bool { return starts() >= crashJobs }, func() string {
		return fmt.Sprintf("only %d of %d jobs started in the helper", starts(), crashJobs)
	})
	if err := helper.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = helper.Wait()
	killed = true
	idBytes, err := os.ReadFile(filepath.Join(dir, "batch-id"))
	if err != nil {
		t.Fatal(err)
	}
	id := batch.ID(idBytes)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := hatchet.New(hatchet.Config{WorkerName: "wfcrash"})
	store := newFileStore(filepath.Join(dir, "store.json"))
	m := batch.New(c, store, batch.WithCompletionSource(c.BatchSource(store, liveSourcePoll, nil)))
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	for _, wf := range crashWorkflows(dir, prefix, false) {
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
	waitUntil(t, 3*time.Minute, func() bool { return finals() > 0 }, func() string {
		p, _ := m.Progress(ctx, id)
		return fmt.Sprintf("Finally did not run after the crash; progress %+v", p)
	})
	time.Sleep(batchQuiet)
	if n := finals(); n != 1 {
		t.Fatalf("Finally ran %d times, want once", n)
	}
	p, err := m.Progress(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if p.Succeeded != crashJobs || !p.Finished {
		t.Fatalf("progress %+v, want all %d jobs succeeded", p, crashJobs)
	}
	for i := range crashJobs {
		done, _ := filepath.Glob(filepath.Join(dir, "done-"+strconv.Itoa(i)+"-*"))
		if len(done) != 1 {
			t.Fatalf("job %d completed %d times, want exactly once", i, len(done))
		}
	}
	// No job got a second engine run: the batch's label matches exactly
	// one run per job.
	rows, err := c.Native().Runs().List(ctx, rest.V1WorkflowRunListParams{
		Since:              time.Now().Add(-time.Hour),
		AdditionalMetadata: &[]string{batch.LabelBatch + ":" + id.String()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) != crashJobs {
		t.Fatalf("engine lists %d runs for the batch, want %d", len(rows.Rows), crashJobs)
	}
}

// fileStore is a batch.Store in one JSON file, so the crash test's two
// processes, which never run at the same time, share batch state.
type fileStore struct {
	mu   sync.Mutex
	path string
}

func newFileStore(path string) *fileStore { return &fileStore{path: path} }

func (f *fileStore) load() (map[batch.ID]batch.State, error) {
	b, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return map[batch.ID]batch.State{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[batch.ID]batch.State{}
	return out, json.Unmarshal(b, &out)
}

func (f *fileStore) save(m map[batch.ID]batch.State) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}

func (f *fileStore) Create(_ context.Context, st batch.State) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.load()
	if err != nil {
		return err
	}
	if _, ok := m[st.ID]; ok {
		return fmt.Errorf("batch %q exists", st.ID)
	}
	m[st.ID] = st
	return f.save(m)
}

func (f *fileStore) Get(_ context.Context, id batch.ID) (batch.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.load()
	if err != nil {
		return batch.State{}, err
	}
	st, ok := m[id]
	if !ok {
		return batch.State{}, fmt.Errorf("batch %q: %w", id, workflow.ErrNotFound)
	}
	return st, nil
}

func (f *fileStore) Update(_ context.Context, id batch.ID, fn func(*batch.State) error) (batch.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.load()
	if err != nil {
		return batch.State{}, err
	}
	st, ok := m[id]
	if !ok {
		return batch.State{}, fmt.Errorf("batch %q: %w", id, workflow.ErrNotFound)
	}
	if err := fn(&st); err != nil {
		return batch.State{}, err
	}
	m[id] = st
	return st, f.save(m)
}

func (f *fileStore) Unfinished(_ context.Context) ([]batch.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, err := f.load()
	if err != nil {
		return nil, err
	}
	var out []batch.State
	for _, st := range m {
		if st.Open() {
			out = append(out, st)
		}
	}
	return out, nil
}

// Dispatch crash test settings: the helper dispatches dispatchCrashJobs
// jobs whose payloads make RunGroup take two bulk triggers of two jobs
// each, and kills itself after the first. Its dispatch lease is short so
// the test does not wait long, but longer than the engine's list lag.
const (
	envDispatchCrash  = "WFBATCH_DISPATCH_CRASH"
	dispatchCrashJobs = 4
	dispatchLease     = 10 * time.Second
)

// dispatchCrashPayload is about 800 KiB once base64-encoded, so RunGroup's
// 2 MiB chunk holds two jobs.
func dispatchCrashPayload() []byte {
	p, _ := json.Marshal(strings.Repeat("x", 600<<10))
	return p
}

// TestBatchDispatchCrashHelper is the process TestBatchDispatchCrashLive
// kills: it dispatches the batch and SIGKILLs itself right after the
// first bulk trigger, before any ref is saved. It does nothing unless
// started by that test.
func TestBatchDispatchCrashHelper(t *testing.T) {
	dir, prefix := os.Getenv(envCrashDir), os.Getenv(envCrashPrefix)
	if dir == "" || os.Getenv(envDispatchCrash) == "" {
		t.Skip("only runs as TestBatchDispatchCrashLive's helper process")
	}
	ctx := context.Background()
	c := hatchet.New(hatchet.Config{WorkerName: "wfdcrash-helper"})
	store := newFileStore(filepath.Join(dir, "store.json"))
	m := batch.New(c, store, batch.WithDispatchLease(dispatchLease), batch.WithCompletionSource(c.BatchSource(store, liveSourcePoll, nil)))
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	for _, wf := range crashWorkflows(dir, prefix, true) {
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
	jobs := make([]workflow.Job, dispatchCrashJobs)
	for i := range jobs {
		jobs[i] = workflow.Job{Workflow: prefix + "_slow", Payload: dispatchCrashPayload()}
	}
	_, err := m.Dispatch(ctx, batch.Batch{Name: "dispatch-crash", Jobs: jobs, Finally: prefix + "_finally"})
	t.Fatalf("Dispatch returned (%v) instead of the process being killed between chunks", err)
}

// TestBatchDispatchCrashLive kills the process that was dispatching a
// batch between its two bulk triggers, so two jobs run on the engine with
// no ref saved and two were never started. A fresh process sharing the
// batch store resumes the dispatch once the lease ends: it finds the two
// started runs by label, starts only the other two, and the batch
// finishes with every job done exactly once, one engine run per job and
// Finally once.
func TestBatchDispatchCrashLive(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live crash test needs a Hatchet engine (task test:live)", envToken)
	}
	dir := t.TempDir()
	prefix := "wfdcrash_" + uuid.NewString()[:8]
	helper := exec.Command(os.Args[0], "-test.run=^TestBatchDispatchCrashHelper$")
	helper.Env = append(os.Environ(), envCrashDir+"="+dir, envCrashPrefix+"="+prefix, envDispatchCrash+"=1")
	out, _ := helper.CombinedOutput() // returns once the helper killed itself
	if _, err := os.Stat(filepath.Join(dir, "chunk-1")); err != nil {
		t.Fatalf("the helper was not killed after the first chunk: %v\n%s", err, out)
	}
	var st batch.State
	{
		all, err := newFileStore(filepath.Join(dir, "store.json")).Unfinished(context.Background())
		if err != nil || len(all) != 1 {
			t.Fatalf("store holds %d open batches (%v), want the one the helper left", len(all), err)
		}
		st = all[0]
	}
	if st.Dispatched || !st.Grouped {
		t.Fatalf("helper left %+v, want an unfinished grouped dispatch", st)
	}
	for i, j := range st.Jobs {
		if !j.Ref.IsZero() {
			t.Fatalf("job %d has ref %v; the crash came before any ref was saved", i, j.Ref)
		}
	}
	id := st.ID

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := hatchet.New(hatchet.Config{WorkerName: "wfdcrash"})
	store := newFileStore(filepath.Join(dir, "store.json"))
	m := batch.New(c, store, batch.WithDispatchLease(dispatchLease), batch.WithCompletionSource(c.BatchSource(store, liveSourcePoll, nil)))
	if err := m.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	for _, wf := range crashWorkflows(dir, prefix, false) {
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
	waitUntil(t, 3*time.Minute, func() bool { return finals() > 0 }, func() string {
		p, _ := m.Progress(ctx, id)
		return fmt.Sprintf("Finally did not run after the dispatch crash; progress %+v", p)
	})
	time.Sleep(batchQuiet)
	if n := finals(); n != 1 {
		t.Fatalf("Finally ran %d times, want once", n)
	}
	p, err := m.Progress(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if p.Succeeded != dispatchCrashJobs || !p.Finished {
		t.Fatalf("progress %+v, want all %d jobs succeeded", p, dispatchCrashJobs)
	}
	for i := range dispatchCrashJobs {
		done, _ := filepath.Glob(filepath.Join(dir, "done-"+strconv.Itoa(i)+"-*"))
		if len(done) != 1 {
			t.Fatalf("job %d completed %d times, want exactly once", i, len(done))
		}
	}
	rows, err := c.Native().Runs().List(ctx, rest.V1WorkflowRunListParams{
		Since:              time.Now().Add(-time.Hour),
		AdditionalMetadata: &[]string{batch.LabelBatch + ":" + id.String()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) != dispatchCrashJobs {
		t.Fatalf("engine lists %d runs for the batch, want %d (the resumer started no job twice)", len(rows.Rows), dispatchCrashJobs)
	}
}
