package batch_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/memory"
)

// counting wraps the memory client and counts enqueues per workflow, so a
// test can assert that a callback was enqueued exactly once, not only that
// it ran. Embedding *memory.Client keeps OnRunFinished, so Attach works.
type counting struct {
	*memory.Client
	mu     sync.Mutex
	counts map[string]int
}

func (c *counting) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	c.mu.Lock()
	c.counts[job.Workflow]++
	c.mu.Unlock()
	return c.Client.Enqueue(ctx, job)
}

func (c *counting) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[name]
}

// env is a started memory client with a Manager attached and three
// callback workflows that report their payload on a channel.
type env struct {
	t       *testing.T
	client  *counting
	mgr     *batch.Manager
	store   *batch.MemoryStore
	ctx     context.Context
	results chan callback
}

type callback struct {
	name string
	p    batch.Progress
}

const (
	wfOK      = "ok"
	wfFail    = "fail"
	wfBlock   = "block"
	cbThen    = "on-then"
	cbCatch   = "on-catch"
	cbFinally = "on-finally"
)

// waitTimeout bounds every wait on a real goroutine; the memory driver
// finishes these runs in microseconds, so hitting it means a bug.
const waitTimeout = 5 * time.Second

func newEnv(t *testing.T) *env { return newEnvWith(t, nil) }

// newEnvWith is newEnv with the Manager built on wrap(client) (nil keeps
// the counting client) and with opts, so a test can hide the hook or name
// the completion sources.
func newEnvWith(t *testing.T, wrap func(*counting) workflow.Client, opts ...batch.Option) *env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := &counting{Client: memory.New(memory.WithWorkers(8)), counts: map[string]int{}}
	e := &env{t: t, client: c, ctx: ctx, results: make(chan callback, 16)}
	step := func(h workflow.Handler) []workflow.Step { return []workflow.Step{{Name: "s", Run: h}} }
	reg := func(name string, h workflow.Handler) {
		if err := c.Register(workflow.Workflow{Name: name, Steps: step(h)}); err != nil {
			t.Fatal(err)
		}
	}
	reg(wfOK, func(context.Context, []byte) ([]byte, error) { return nil, nil })
	reg(wfFail, func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errors.New("boom")) })
	reg(wfBlock, func(ctx context.Context, _ []byte) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() })
	for _, name := range []string{cbThen, cbCatch, cbFinally} {
		reg(name, func(_ context.Context, in []byte) ([]byte, error) {
			var p batch.Progress
			if err := json.Unmarshal(in, &p); err != nil {
				return nil, err
			}
			e.results <- callback{name: name, p: p}
			return nil, nil
		})
	}
	var mc workflow.Client = c
	if wrap != nil {
		mc = wrap(c)
	}
	e.store = batch.NewMemoryStore()
	e.mgr = batch.New(mc, e.store, opts...)
	if err := e.mgr.Attach(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) dispatch(allowFailures bool, workflows ...string) batch.ID {
	e.t.Helper()
	b := batch.Batch{Name: "test", Then: cbThen, Catch: cbCatch, Finally: cbFinally, AllowFailures: allowFailures}
	for _, w := range workflows {
		b.Jobs = append(b.Jobs, workflow.Job{Workflow: w})
	}
	id, err := e.mgr.Dispatch(e.ctx, b)
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

// waitFinally waits for the Finally callback to run and returns its
// payload. Finally is the last callback a batch enqueues, so once it has
// run every other callback decision has been made and counted.
func (e *env) waitFinally() batch.Progress {
	e.t.Helper()
	deadline := time.After(waitTimeout)
	for {
		select {
		case cb := <-e.results:
			if cb.name == cbFinally {
				return cb.p
			}
		case <-deadline:
			e.t.Fatal("Finally never ran")
		}
	}
}

func (e *env) wantCounts(then, catch, finally int) {
	e.t.Helper()
	for name, want := range map[string]int{cbThen: then, cbCatch: catch, cbFinally: finally} {
		if got := e.client.count(name); got != want {
			e.t.Errorf("%s enqueued %d times, want %d", name, got, want)
		}
	}
}

func TestAllSucceedRunsThenAndFinally(t *testing.T) {
	e := newEnv(t)
	id := e.dispatch(false, wfOK, wfOK, wfOK)
	p := e.waitFinally()
	if !p.Finished || p.Succeeded != 3 || p.Pending != 0 || p.Cancelled || p.Percent() != 100 {
		t.Errorf("Finally payload = %+v", p)
	}
	e.wantCounts(1, 0, 1)
	got, err := e.mgr.Progress(e.ctx, id)
	if err != nil || got.ID != id || got.Succeeded != 3 {
		t.Errorf("Progress = %+v, %v", got, err)
	}
}

func TestFailureRunsCatchAndCancelsTheRest(t *testing.T) {
	e := newEnv(t)
	e.dispatch(false, wfBlock, wfFail, wfBlock)
	p := e.waitFinally()
	if p.Failed != 1 || p.CancelledJobs != 2 || !p.Cancelled || p.Succeeded != 0 {
		t.Errorf("Finally payload = %+v", p)
	}
	e.wantCounts(0, 1, 1)
}

func TestAllowFailuresKeepsRunningButSkipsThen(t *testing.T) {
	e := newEnv(t)
	e.dispatch(true, wfOK, wfFail, wfFail, wfOK)
	p := e.waitFinally()
	if p.Failed != 2 || p.Succeeded != 2 || p.Cancelled || p.CancelledJobs != 0 {
		t.Errorf("Finally payload = %+v", p)
	}
	// Two failures, one Catch: it fires on the first failure only.
	e.wantCounts(0, 1, 1)
}

func TestCancelBatch(t *testing.T) {
	e := newEnv(t)
	id := e.dispatch(false, wfBlock, wfBlock)
	if err := e.mgr.Cancel(e.ctx, id); err != nil {
		t.Fatal(err)
	}
	p := e.waitFinally()
	if !p.Cancelled || p.CancelledJobs != 2 {
		t.Errorf("Finally payload = %+v", p)
	}
	e.wantCounts(0, 0, 1)
	// Cancelling again, or a finished batch, is a no-op.
	if err := e.mgr.Cancel(e.ctx, id); err != nil {
		t.Errorf("second Cancel: %v", err)
	}
	if err := e.mgr.Cancel(e.ctx, "nope"); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("Cancel unknown = %v, want ErrNotFound", err)
	}
}

// TestRepeatedDeliveryIsIgnored replays every run end a second time, as an
// at-least-once hook may: no job is counted twice, no callback fires twice.
func TestRepeatedDeliveryIsIgnored(t *testing.T) {
	e := newEnv(t)
	id := e.dispatch(false, wfOK, wfOK)
	e.waitFinally()
	for i := range 2 {
		labels := map[string]string{batch.LabelBatch: string(id), batch.LabelIndex: string(rune('0' + i))}
		if err := e.mgr.RunFinished(e.ctx, labels, workflow.StatusFailed); err != nil {
			t.Fatal(err)
		}
	}
	p, _ := e.mgr.Progress(e.ctx, id)
	if p.Succeeded != 2 || p.Failed != 0 {
		t.Errorf("Progress after replay = %+v", p)
	}
	e.wantCounts(1, 0, 1)
}

func TestDispatchValidation(t *testing.T) {
	e := newEnv(t)
	if _, err := e.mgr.Dispatch(e.ctx, batch.Batch{Name: "empty"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty batch = %v, want ErrInvalid", err)
	}
	labels := map[string]string{"tenant": "a"}
	id, err := e.mgr.Dispatch(e.ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: wfOK, Labels: labels}}})
	if err != nil || id == "" {
		t.Fatalf("Dispatch = %q, %v", id, err)
	}
	if len(labels) != 1 {
		t.Errorf("Dispatch modified the caller's labels: %v", labels)
	}
	chained := batch.Batch{Jobs: []workflow.Job{{Workflow: wfOK, Chain: []workflow.Job{{Workflow: wfOK}}}}}
	if _, err := e.mgr.Dispatch(e.ctx, chained); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("a job with a chain = %v, want ErrInvalid", err)
	}
	if err := e.mgr.Add(e.ctx, id, chained.Jobs...); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("adding a job with a chain = %v, want ErrInvalid", err)
	}
	odd := batch.Batch{Jobs: []workflow.Job{{Workflow: wfOK}}, DiscardedAs: workflow.StatusRunning}
	if _, err := e.mgr.Dispatch(e.ctx, odd); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("DiscardedAs running = %v, want ErrInvalid", err)
	}
}
