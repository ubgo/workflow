package workflow_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

func TestParity2CapNames(t *testing.T) {
	got := (workflow.CapRunStates | workflow.CapChain | workflow.CapWorkerScale | workflow.CapClosure).String()
	if got != "RunStates|Chain|WorkerScale|Closure" {
		t.Fatalf("String = %q", got)
	}
	if workflow.CapRunStates <= workflow.CapDrain {
		t.Fatal("a new capability moved an existing bit")
	}
}

func TestNewStatuses(t *testing.T) {
	if !workflow.StatusDiscarded.IsTerminal() || workflow.StatusRetrying.IsTerminal() {
		t.Fatal("Discarded must be terminal and Retrying not")
	}
	for _, s := range []workflow.Status{workflow.StatusRetrying, workflow.StatusDiscarded} {
		if !s.Valid() {
			t.Fatalf("%q not valid", s)
		}
		if err := workflow.ValidateRunQuery(workflow.RunQuery{Statuses: []workflow.Status{s}}); err != nil {
			t.Fatalf("query on %q: %v", s, err)
		}
	}
	if workflow.Status("nope").Valid() {
		t.Fatal("unknown status valid")
	}
	if err := workflow.ValidateCancelQuery(workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusDiscarded}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("cancel of discarded runs: %v, want ErrInvalid", err)
	}
}

// TestEndStatus pins the decision every driver shares: only an explicit
// Discard is ever StatusDiscarded, and only with CapRunStates.
func TestEndStatus(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name string
		have workflow.Caps
		err  error
		want workflow.Status
	}{
		{"plain error", workflow.CapRunStates | workflow.CapDiscard, boom, workflow.StatusFailed},
		{"permanent", workflow.CapRunStates, workflow.Permanent(boom), workflow.StatusFailed},
		{"discard with run states", workflow.CapRunStates | workflow.CapDiscard, workflow.Discard(boom), workflow.StatusDiscarded},
		{"discard with run states only", workflow.CapRunStates, workflow.Discard(boom), workflow.StatusDiscarded},
		{"discard with discard only", workflow.CapDiscard, workflow.Discard(boom), workflow.StatusCancelled},
		{"discard on a driver with neither", 0, workflow.Discard(boom), workflow.StatusFailed},
		{"wrapped discard", workflow.CapRunStates, errors.Join(boom, workflow.Discard(boom)), workflow.StatusDiscarded},
	} {
		if got := workflow.EndStatus(tc.have, tc.err); got != tc.want {
			t.Errorf("%s: EndStatus = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestChainValidationAndCaps(t *testing.T) {
	job := workflow.Job{Workflow: "a", Chain: []workflow.Job{{Workflow: "b", RunAt: time.Unix(1, 0)}, {Workflow: "c"}}}
	if err := workflow.ValidateJob(job); err != nil {
		t.Fatal(err)
	}
	if got := workflow.RequiresJob(job); !got.Has(workflow.CapChain | workflow.CapDelayedStart) {
		t.Fatalf("RequiresJob = %s, want Chain and DelayedStart", got)
	}
	var ue *workflow.UnsupportedError
	if err := workflow.CheckJob(workflow.CapDelayedStart, job); !errors.As(err, &ue) || ue.Field != "Job.Chain" {
		t.Fatalf("CheckJob = %v, want an UnsupportedError on Job.Chain", err)
	}
	bad := workflow.Job{Workflow: "a", Chain: []workflow.Job{{Workflow: ""}}}
	if err := workflow.ValidateJob(bad); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("chained job without workflow: %v", err)
	}
	long := workflow.Job{Workflow: "a", Chain: make([]workflow.Job, workflow.MaxChainJobs+1)}
	for i := range long.Chain {
		long.Chain[i].Workflow = "b"
	}
	if err := workflow.ValidateJob(long); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("chain over MaxChainJobs: %v", err)
	}
}

func TestNextInChainFlattens(t *testing.T) {
	chain := []workflow.Job{
		{Workflow: "b", Chain: []workflow.Job{{Workflow: "b2"}}, Labels: map[string]string{"k": "v"}},
		{Workflow: "c"},
	}
	next, ok := workflow.NextInChain(chain)
	if !ok || next.Workflow != "b" {
		t.Fatalf("NextInChain = %+v, %v", next, ok)
	}
	var names []string
	for _, j := range next.Chain {
		names = append(names, j.Workflow)
	}
	if strings.Join(names, ",") != "b2,c" {
		t.Fatalf("rest = %v, want b2,c", names)
	}
	next.Labels["k"] = "changed"
	if chain[0].Labels["k"] != "v" {
		t.Fatal("NextInChain shares labels with its input")
	}
	if _, ok := workflow.NextInChain(nil); ok {
		t.Fatal("empty chain has a next job")
	}
}

func TestChainEdits(t *testing.T) {
	ctx := context.Background()
	if err := workflow.PrependToChain(ctx, workflow.Job{Workflow: "x"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("PrependToChain without an editor = %v, want ErrUnsupported", err)
	}
	if err := workflow.AppendToChain(ctx, workflow.Job{Workflow: "x"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("AppendToChain without an editor = %v, want ErrUnsupported", err)
	}
	base := []workflow.Job{{Workflow: "c"}}
	ed := workflow.NewChainEdits(workflow.CapChain, base)
	sctx := workflow.WithChainEditor(ctx, ed)
	if ed.Changed() {
		t.Fatal("fresh edits report a change")
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(workflow.AppendToChain(sctx, workflow.Job{Workflow: "d"}))
	must(workflow.PrependToChain(sctx, workflow.Job{Workflow: "a1"}, workflow.Job{Workflow: "a2"}))
	must(workflow.PrependToChain(sctx, workflow.Job{Workflow: "z"}))
	got := ed.Apply(base)
	var names []string
	for _, j := range got {
		names = append(names, j.Workflow)
	}
	if strings.Join(names, ",") != "z,a1,a2,c,d" {
		t.Fatalf("Apply = %v, want z,a1,a2,c,d", names)
	}
	if len(base) != 1 {
		t.Fatal("Apply modified its input")
	}
	if err := workflow.AppendToChain(sctx, workflow.Job{Workflow: "late", RunAt: time.Unix(5, 0)}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("an edit the driver cannot honour = %v, want ErrUnsupported", err)
	}
	if err := workflow.AppendToChain(sctx, workflow.Job{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("an invalid job = %v, want ErrInvalid", err)
	}
	full := workflow.NewChainEdits(workflow.CapChain, make([]workflow.Job, workflow.MaxChainJobs))
	if err := full.Append(workflow.Job{Workflow: "one-too-many"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("an edit past MaxChainJobs = %v, want ErrInvalid", err)
	}
}

func TestCloneJobIsDeep(t *testing.T) {
	j := workflow.Job{Workflow: "a", Payload: []byte("p"), Labels: map[string]string{"k": "v"},
		Retry: &workflow.RetryPolicy{Max: 1}, Chain: []workflow.Job{{Workflow: "b", Payload: []byte("q")}}}
	c := workflow.CloneJob(j)
	if !reflect.DeepEqual(c, j) {
		t.Fatalf("clone differs: %+v", c)
	}
	c.Payload[0], c.Labels["k"], c.Retry.Max, c.Chain[0].Payload[0] = 'x', "x", 9, 'x'
	if string(j.Payload) != "p" || j.Labels["k"] != "v" || j.Retry.Max != 1 || string(j.Chain[0].Payload) != "q" {
		t.Fatal("clone shares state with the original")
	}
}

// recordingEnqueue is an EnqueueFunc that records the jobs it is given.
type recordingEnqueue struct {
	mu   sync.Mutex
	jobs []string
	fail map[string]bool
}

func (r *recordingEnqueue) enqueue(_ context.Context, job workflow.Job) (workflow.RunRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail[job.Workflow] {
		return workflow.RunRef{}, errors.New("refused")
	}
	r.jobs = append(r.jobs, job.Workflow)
	return workflow.RunRef{RunID: workflow.RunID("run-" + job.Workflow)}, nil
}

func (r *recordingEnqueue) sent() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.jobs, ",")
}

func TestDeferredFlushAndDiscard(t *testing.T) {
	ctx := context.Background()
	rec := &recordingEnqueue{fail: map[string]bool{"bad": true}}
	enq := workflow.DeferEnqueue()(rec.enqueue)

	// Without a Deferred on the ctx the job goes at once.
	if ref, err := enq(ctx, workflow.Job{Workflow: "now"}); err != nil || ref.RunID != "run-now" {
		t.Fatalf("direct enqueue = %+v, %v", ref, err)
	}

	d := workflow.NewDeferred()
	dctx := workflow.WithDeferred(ctx, d)
	for _, wf := range []string{"a", "bad", "b"} {
		if ref, err := enq(dctx, workflow.Job{Workflow: wf}); err != nil || !ref.IsZero() {
			t.Fatalf("held enqueue = %+v, %v; want the zero ref and nil", ref, err)
		}
	}
	if d.Len() != 3 || rec.sent() != "now" {
		t.Fatalf("held %d, sent %q before Flush", d.Len(), rec.sent())
	}
	refs, err := d.Flush(ctx)
	if err == nil || !strings.Contains(err.Error(), `"bad"`) {
		t.Fatalf("Flush error = %v, want the failed job named", err)
	}
	if rec.sent() != "now,a,b" || len(refs) != 3 || !refs[1].IsZero() || refs[2].RunID != "run-b" {
		t.Fatalf("sent %q, refs %+v", rec.sent(), refs)
	}
	if _, err := enq(dctx, workflow.Job{Workflow: "late"}); !errors.Is(err, workflow.ErrDeferredClosed) {
		t.Fatalf("enqueue after Flush = %v, want ErrDeferredClosed", err)
	}
	if refs, err := d.Flush(ctx); err != nil || len(refs) != 0 {
		t.Fatal("a second Flush sent something")
	}

	d2 := workflow.NewDeferred()
	if _, err := enq(workflow.WithDeferred(ctx, d2), workflow.Job{Workflow: "dropped"}); err != nil {
		t.Fatal(err)
	}
	if n := d2.Discard(); n != 1 {
		t.Fatalf("Discard dropped %d, want 1", n)
	}
	if refs, _ := d2.Flush(ctx); len(refs) != 0 || strings.Contains(rec.sent(), "dropped") {
		t.Fatal("a discarded job was sent")
	}
}

func TestDeferHTTP(t *testing.T) {
	rec := &recordingEnqueue{}
	enq := workflow.DeferEnqueue()(rec.enqueue)
	handler := func(status int, panics bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := enq(r.Context(), workflow.Job{Workflow: r.URL.Path[1:]}); err != nil {
				t.Errorf("enqueue: %v", err)
			}
			if rec.sent() != "" && !strings.HasPrefix(rec.sent(), "ok") {
				t.Errorf("sent %q during the request", rec.sent())
			}
			if panics {
				panic("handler bug")
			}
			if status != 0 {
				w.WriteHeader(status)
			}
			_, _ = w.Write([]byte("done"))
		})
	}
	serve := func(h http.Handler, path string) {
		t.Helper()
		defer func() { _ = recover() }()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, path, nil))
	}

	serve(workflow.DeferHTTP(handler(0, false), workflow.DeferOptions{}), "/ok-implicit-200")
	serve(workflow.DeferHTTP(handler(http.StatusCreated, false), workflow.DeferOptions{}), "/ok-201")
	serve(workflow.DeferHTTP(handler(http.StatusBadRequest, false), workflow.DeferOptions{}), "/client-error")
	serve(workflow.DeferHTTP(handler(http.StatusInternalServerError, false), workflow.DeferOptions{}), "/server-error")
	serve(workflow.DeferHTTP(handler(0, true), workflow.DeferOptions{}), "/panicked")
	if got := rec.sent(); got != "ok-implicit-200,ok-201" {
		t.Fatalf("sent %q, want only the successful requests' jobs", got)
	}

	// A custom success rule replaces the default.
	only201 := workflow.DeferOptions{Succeeded: func(s int) bool { return s == http.StatusCreated }}
	serve(workflow.DeferHTTP(handler(0, false), only201), "/not-created")
	serve(workflow.DeferHTTP(handler(http.StatusCreated, false), only201), "/ok-created")
	if got := rec.sent(); got != "ok-implicit-200,ok-201,ok-created" {
		t.Fatalf("sent %q under the custom rule", got)
	}

	// A flush failure reaches OnError.
	var flushErr error
	failing := workflow.DeferEnqueue()(func(context.Context, workflow.Job) (workflow.RunRef, error) {
		return workflow.RunRef{}, errors.New("backend down")
	})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = failing(r.Context(), workflow.Job{Workflow: "x"})
	})
	serve(workflow.DeferHTTP(h, workflow.DeferOptions{OnError: func(_ *http.Request, err error) { flushErr = err }}), "/x")
	if flushErr == nil || !strings.Contains(flushErr.Error(), "backend down") {
		t.Fatalf("OnError got %v", flushErr)
	}
}

// fakeScaler is a WorkerScaler over fixed loads that records SetWorkers.
type fakeScaler struct {
	mu    sync.Mutex
	loads []workflow.QueueLoad
	set   map[string]int
	calls int
}

func (f *fakeScaler) QueueLoads(context.Context) ([]workflow.QueueLoad, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]workflow.QueueLoad(nil), f.loads...), nil
}

func (f *fakeScaler) SetWorkers(_ context.Context, q string, n int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.set == nil {
		f.set = map[string]int{}
	}
	f.set[q] = n
	for i := range f.loads {
		if f.loads[i].Queue == q {
			f.loads[i].Workers = n
		}
	}
	return nil
}

func TestByDepthAndByWait(t *testing.T) {
	loads := []workflow.QueueLoad{
		{Queue: "mail", Depth: 90, Workers: 5, OldestWait: time.Second},
		{Queue: "reports", Depth: 10, Workers: 5, OldestWait: 9 * time.Second},
	}
	got := workflow.ByDepth(workflow.BalanceConfig{}).Balance(loads)
	if got["mail"] != 9 || got["reports"] != 1 {
		t.Fatalf("ByDepth = %v, want mail 9, reports 1", got)
	}
	got = workflow.ByWait(workflow.BalanceConfig{}).Balance(loads)
	if got["mail"] != 1 || got["reports"] != 9 {
		t.Fatalf("ByWait = %v, want mail 1, reports 9", got)
	}

	// Minimums first, a maximum caps a queue and its excess goes elsewhere.
	cfg := workflow.BalanceConfig{Total: 12, Bounds: map[string]workflow.QueueBounds{
		"mail":    {Max: 6},
		"reports": {Min: 3},
	}}
	got = workflow.ByDepth(cfg).Balance(loads)
	if got["mail"] != 6 || got["reports"] != 6 {
		t.Fatalf("bounded ByDepth = %v, want mail 6, reports 6", got)
	}

	// No load at all: the budget is shared evenly.
	idle := []workflow.QueueLoad{{Queue: "a", Workers: 2}, {Queue: "b", Workers: 2}}
	got = workflow.ByDepth(workflow.BalanceConfig{}).Balance(idle)
	if got["a"] != 2 || got["b"] != 2 {
		t.Fatalf("idle = %v, want 2 and 2", got)
	}

	// MaxShift limits the move per round.
	got = workflow.ByDepth(workflow.BalanceConfig{MaxShift: 1}).Balance(loads)
	if got["mail"] != 6 || got["reports"] != 4 {
		t.Fatalf("MaxShift 1 = %v, want mail 6, reports 4", got)
	}

	// A total below the minimums is raised to them; never below one.
	got = workflow.ByDepth(workflow.BalanceConfig{Total: 1}).Balance(loads)
	if got["mail"] != 1 || got["reports"] != 1 {
		t.Fatalf("total under the minimums = %v, want 1 and 1", got)
	}

	// Every worker is placed, and none twice.
	got = workflow.Proportional(workflow.BalanceConfig{Total: 10}, func(l workflow.QueueLoad) float64 { return 1 }).Balance(
		[]workflow.QueueLoad{{Queue: "x"}, {Queue: "y"}, {Queue: "z"}})
	if got["x"]+got["y"]+got["z"] != 10 || got["x"] != 4 {
		t.Fatalf("even split of 10 over 3 = %v, want 4,3,3 summing to 10", got)
	}
}

func TestBalanceLoop(t *testing.T) {
	sc := &fakeScaler{loads: []workflow.QueueLoad{
		{Queue: "a", Depth: 30, Workers: 2},
		{Queue: "b", Depth: 10, Workers: 2},
	}}
	l := workflow.BalanceLoop{Scaler: sc, Balancer: workflow.ByDepth(workflow.BalanceConfig{})}
	if err := l.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sc.set["a"] != 3 || sc.set["b"] != 1 {
		t.Fatalf("Once set %v, want a 3, b 1", sc.set)
	}
	calls := sc.calls
	if err := l.Once(context.Background()); err != nil || sc.calls != calls {
		t.Fatalf("an unchanged round called SetWorkers again (%d calls, err %v)", sc.calls-calls, err)
	}
	if err := (workflow.BalanceLoop{}).Once(context.Background()); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a loop without parts = %v", err)
	}

	// Run ticks on the injected clock and stops with ctx.
	fc := clock.NewFake(time.Unix(0, 0))
	sc2 := &fakeScaler{loads: []workflow.QueueLoad{{Queue: "a", Depth: 1, Workers: 1}, {Queue: "b", Workers: 3}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- workflow.BalanceLoop{Scaler: sc2, Balancer: workflow.ByDepth(workflow.BalanceConfig{}), Every: time.Second, Clock: fc}.Run(ctx)
	}()
	if err := fc.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fc.Advance(time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for {
		sc2.mu.Lock()
		n := sc2.calls
		sc2.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run did not balance after one tick")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
}

func TestClosureValidationAndRefusal(t *testing.T) {
	fn := func(context.Context) error { return nil }
	for _, c := range []workflow.ClosureJob{
		{Fn: fn},
		{Name: "x"},
		{Name: "x", Fn: fn, Queue: "bad queue"},
		{Name: "x", Fn: fn, Timeout: -1},
		{Name: "x", Fn: fn, Labels: map[string]string{workflow.LabelClosure: "forged"}},
	} {
		if err := workflow.ValidateClosure(c); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("ValidateClosure(%+v) = %v, want ErrInvalid", c, err)
		}
	}
	if err := workflow.ValidateClosure(workflow.ClosureJob{Name: "x", Fn: fn,
		Retry: &workflow.RetryPolicy{Max: 2, NoRetryOn: func(error) bool { return false }}}); err != nil {
		t.Fatalf("a closure retry with functions: %v", err)
	}
	wf := workflow.Workflow{Name: workflow.ClosureWorkflow, Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
	if err := workflow.Validate(wf); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("registering the closure workflow name = %v", err)
	}
	var plain workflow.Client = noClosureClient{}
	_, err := workflow.EnqueueClosure(context.Background(), plain, workflow.ClosureJob{Name: "x", Fn: fn})
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || ue.Missing != workflow.CapClosure {
		t.Fatalf("EnqueueClosure on a client without closures = %v", err)
	}
}

// noClosureClient is a Client with no capabilities; only Capabilities is
// ever called.
type noClosureClient struct{ workflow.Client }

func (noClosureClient) Capabilities() workflow.Caps { return 0 }

func TestForwardNewInterfaces(t *testing.T) {
	f := workflow.Forward{Inner: noClosureClient{}}
	if _, err := f.QueueLoads(context.Background()); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("QueueLoads = %v", err)
	}
	if err := f.SetWorkers(context.Background(), "q", 1); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("SetWorkers = %v", err)
	}
	if _, err := f.EnqueueClosure(context.Background(), workflow.ClosureJob{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("EnqueueClosure = %v", err)
	}
}
