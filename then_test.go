package workflow_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// thenRec records the runs of the workflows it builds.
type thenRec struct {
	mu   sync.Mutex
	runs []workflow.StepInfo
	ins  []string
}

func (r *thenRec) wf(name string, fail bool) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		r.mu.Lock()
		r.runs = append(r.runs, info)
		r.ins = append(r.ins, string(in))
		r.mu.Unlock()
		if fail {
			return nil, workflow.Permanent(errors.New("boom"))
		}
		return nil, nil
	}}}}
}

func (r *thenRec) byWorkflow(name string) []workflow.StepInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []workflow.StepInfo
	for _, s := range r.runs {
		if s.Workflow == name {
			out = append(out, s)
		}
	}
	return out
}

// waitFor polls cond for up to 4s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// errSink collects ThenErrors.
type errSink struct {
	mu   sync.Mutex
	errs []error
}

func (s *errSink) add(err error) { s.mu.Lock(); s.errs = append(s.errs, err); s.mu.Unlock() }
func (s *errSink) all() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}

// TestThenStartsNextWorkflow: a succeeded import starts index with the
// payload Start built, its LabelThenFrom and the derived key; a failed
// import starts alert (On: failed) and not index; Start returning false
// starts nothing.
func TestThenStartsNextWorkflow(t *testing.T) {
	var rec thenRec
	then, err := workflow.Then([]workflow.ThenRule{
		{When: "import", Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
			if ev.Labels["skip"] == "yes" {
				return workflow.Job{}, false, nil
			}
			return workflow.Job{Workflow: "index", Payload: []byte(`"` + ev.Labels["file"] + `"`)}, true, nil
		}},
		{Name: "alert-on-failure", When: "import", On: []workflow.Status{workflow.StatusFailed}, Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
			return workflow.Job{Workflow: "alert"}, true, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := startThen(t, memory.New(), then, rec.wf("import", false), rec.wf("bad-import", true), rec.wf("index", false), rec.wf("alert", false))
	ctx := context.Background()
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "import", Labels: map[string]string{"file": "a.csv"}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "index run", func() bool { return len(rec.byWorkflow("index")) == 1 })
	idx := rec.byWorkflow("index")[0]
	if idx.Labels[workflow.LabelThenFrom] != ref.RunID.String() {
		t.Fatalf("index labels %v; want %s=%s", idx.Labels, workflow.LabelThenFrom, ref.RunID)
	}
	rec.mu.Lock()
	in := rec.ins[len(rec.ins)-1]
	rec.mu.Unlock()
	if in != `"a.csv"` {
		t.Fatalf("index payload %s", in)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "import", Labels: map[string]string{"skip": "yes"}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := len(rec.byWorkflow("index")); n != 1 {
		t.Fatalf("Start returned false and index ran %d times", n)
	}
	if n := len(rec.byWorkflow("alert")); n != 0 {
		t.Fatalf("alert ran %d times after successes", n)
	}
}

// TestThenOnFailed: a rule with On failed fires for a failed run only.
func TestThenOnFailed(t *testing.T) {
	var rec thenRec
	then, err := workflow.Then([]workflow.ThenRule{
		{When: "job", On: []workflow.Status{workflow.StatusFailed}, Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
			return workflow.Job{Workflow: "alert", Payload: []byte(`"` + ev.Err.Error() + `"`)}, true, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := startThen(t, memory.New(), then, rec.wf("job", true), rec.wf("alert", false))
	if _, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "job"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "alert run", func() bool { return len(rec.byWorkflow("alert")) == 1 })
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if !strings.Contains(rec.ins[len(rec.ins)-1], "boom") {
		t.Fatalf("alert payload %s", rec.ins[len(rec.ins)-1])
	}
}

// manualNotifier keeps the observers instead of a driver calling them,
// so a test can deliver one event twice, as an at-least-once driver may.
type manualNotifier struct {
	workflow.Client
	mu  sync.Mutex
	fns []workflow.RunFinishedFunc
}

func (m *manualNotifier) OnRunFinished(fn workflow.RunFinishedFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fns = append(m.fns, fn)
	return nil
}

func (m *manualNotifier) deliver(ev workflow.RunEvent) {
	m.mu.Lock()
	fns := append([]workflow.RunFinishedFunc(nil), m.fns...)
	m.mu.Unlock()
	for _, fn := range fns {
		fn(context.Background(), ev)
	}
}

// TestThenRepeatedNotificationStartsOnce: the same run-finished event
// delivered twice starts the next job once, under the documented key.
func TestThenRepeatedNotificationStartsOnce(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	inner := memory.New()
	if err := inner.Register(workflow.Workflow{Name: "index", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		<-release
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := inner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var sink errSink
	then, _ := workflow.Then([]workflow.ThenRule{{When: "import", Start: func(workflow.RunEvent) (workflow.Job, bool, error) {
		return workflow.Job{Workflow: "index"}, true, nil
	}}}, workflow.ThenErrors(sink.add))
	n := &manualNotifier{Client: inner}
	if _, err := workflow.Use(n, then); err != nil {
		t.Fatal(err)
	}
	ev := workflow.RunEvent{Ref: workflow.RunRef{RunID: "run-import-1"}, Workflow: "import", Status: workflow.StatusSucceeded}
	n.deliver(ev)
	n.deliver(ev)
	page, err := inner.Runs(ctx, workflow.RunQuery{Workflow: "index", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || len(sink.all()) != 0 {
		t.Fatalf("%d index runs, errors %v; want 1 and none", len(page.Runs), sink.all())
	}
	same, err := inner.Enqueue(ctx, workflow.Job{Workflow: "index", IdempotencyKey: "then/import>index/run-import-1"})
	if err != nil || same != page.Runs[0].Ref {
		t.Fatalf("the documented key gave %v, %v; want %v", same, err, page.Runs[0].Ref)
	}
	// A delayed run cancelled before it started has only a ScheduledID.
	n.deliver(workflow.RunEvent{Ref: workflow.RunRef{ScheduledID: "sch-1"}, Workflow: "import", Status: workflow.StatusSucceeded})
	page, _ = inner.Runs(ctx, workflow.RunQuery{Workflow: "index", Limit: 10, Labels: map[string]string{workflow.LabelThenFrom: "scheduled:sch-1"}})
	if len(page.Runs) != 1 {
		t.Fatalf("no index run from the scheduled ref: %v", sink.all())
	}
}

// TestThenErrors: Start's error, a reserved label and an enqueue error
// reach ThenErrors as *ThenError naming the rule; a plugin whose observer
// was not built by Use reports that instead of enqueuing nowhere.
func TestThenErrors(t *testing.T) {
	var sink errSink
	startErr := errors.New("no file")
	then, err := workflow.Then([]workflow.ThenRule{
		{Name: "fails", When: "a", Start: func(workflow.RunEvent) (workflow.Job, bool, error) { return workflow.Job{}, false, startErr }},
		{Name: "reserved", When: "a", Start: func(workflow.RunEvent) (workflow.Job, bool, error) {
			return workflow.Job{Workflow: "b", Labels: map[string]string{workflow.LabelThenFrom: "forged"}}, true, nil
		}},
		{Name: "unknown", When: "a", Start: func(workflow.RunEvent) (workflow.Job, bool, error) { return workflow.Job{Workflow: "nope"}, true, nil }},
	}, workflow.ThenErrors(sink.add))
	if err != nil {
		t.Fatal(err)
	}
	n := &manualNotifier{Client: memory.New()}
	if _, err := workflow.Use(n, then); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := n.Client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	n.deliver(workflow.RunEvent{Ref: workflow.RunRef{RunID: "r1"}, Workflow: "a", Status: workflow.StatusSucceeded})
	errs := sink.all()
	if len(errs) != 3 {
		t.Fatalf("errors = %v, want 3", errs)
	}
	for i, want := range []string{"fails", "reserved", "unknown"} {
		var te *workflow.ThenError
		if !errors.As(errs[i], &te) || te.Rule != want || te.From.RunID != "r1" {
			t.Fatalf("error %d = %v, want a ThenError for rule %s", i, errs[i], want)
		}
	}
	if !errors.Is(errs[0], startErr) || !errors.Is(errs[1], workflow.ErrInvalid) {
		t.Fatalf("causes: %v / %v", errs[0], errs[1])
	}

	var sink2 errSink
	loose, _ := workflow.Then([]workflow.ThenRule{{When: "a", Start: func(workflow.RunEvent) (workflow.Job, bool, error) {
		return workflow.Job{Workflow: "b"}, true, nil
	}}}, workflow.ThenErrors(sink2.add))
	obs := loose.(workflow.RunFinishedProvider).RunFinishedObservers()
	obs[0](context.Background(), workflow.RunEvent{Ref: workflow.RunRef{RunID: "r"}, Workflow: "a", Status: workflow.StatusSucceeded})
	if e := sink2.all(); len(e) != 1 || !errors.Is(e[0], workflow.ErrInvalid) || !strings.Contains(e[0].Error(), "Use") {
		t.Fatalf("unbound plugin: %v", e)
	}
}

// TestThenRefusals: invalid rules are refused by Then, and a client
// without RunNotifier by Use.
func TestThenRefusals(t *testing.T) {
	start := func(workflow.RunEvent) (workflow.Job, bool, error) { return workflow.Job{}, false, nil }
	cases := map[string][]workflow.ThenRule{
		"no rules":            nil,
		"no When":             {{Start: start}},
		"no Start":            {{When: "a"}},
		"non-terminal status": {{When: "a", Start: start, On: []workflow.Status{workflow.StatusRunning}}},
		"duplicate name":      {{Name: "x", When: "a", Start: start}, {Name: "x", When: "b", Start: start}},
	}
	for name, rules := range cases {
		if _, err := workflow.Then(rules); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	then, _ := workflow.Then([]workflow.ThenRule{{When: "a", Start: start}})
	var ue *workflow.UnsupportedError
	if _, err := workflow.Use(struct{ workflow.Client }{memory.New()}, then); !errors.As(err, &ue) || ue.Missing != workflow.CapRunFinished {
		t.Fatalf("Use without RunNotifier: %v", err)
	}
}

// TestThenTwoClientsAndPluginOrder: one plugin value used with two
// clients enqueues each next job on its own client, and an enqueue
// middleware of a plugin after Then applies to the next job.
func TestThenTwoClientsAndPluginOrder(t *testing.T) {
	then, _ := workflow.Then([]workflow.ThenRule{{When: "a", Start: func(workflow.RunEvent) (workflow.Job, bool, error) {
		return workflow.Job{Workflow: "b"}, true, nil
	}}})
	var recs [2]thenRec
	var clients [2]workflow.Client
	for i := range clients {
		clients[i] = startThen(t, memory.New(), then, recs[i].wf("a", false), recs[i].wf("b", false))
	}
	// Plugin order: Then first, an auditing plugin after it.
	log := newAuditLog(8)
	ordered, err := workflow.Use(memory.New(), then, &auditPlugin{name: "audit", log: log})
	if err != nil {
		t.Fatal(err)
	}
	var orec thenRec
	for _, wf := range []workflow.Workflow{orec.wf("a", false), orec.wf("b", false)} {
		if err := ordered.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ordered.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := clients[1].Enqueue(ctx, workflow.Job{Workflow: "a"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "b on client 1", func() bool { return len(recs[1].byWorkflow("b")) == 1 })
	time.Sleep(20 * time.Millisecond)
	if n := len(recs[0].byWorkflow("b")); n != 0 {
		t.Fatalf("client 0 ran %d b runs for client 1's a", n)
	}

	if _, err := ordered.Enqueue(ctx, workflow.Job{Workflow: "a"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "b on the ordered client", func() bool { return len(orec.byWorkflow("b")) == 1 })
	waitFor(t, "audit of b's enqueue", func() bool {
		for _, l := range log.all() {
			if l == "audit enqueue b" {
				return true
			}
		}
		return false
	})
}

// startThen applies then to c, registers wfs and starts it.
func startThen(t *testing.T, c workflow.Client, then workflow.Plugin, wfs ...workflow.Workflow) workflow.Client {
	t.Helper()
	pc, err := workflow.Use(c, then)
	if err != nil {
		t.Fatal(err)
	}
	for _, wf := range wfs {
		if err := pc.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := pc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return pc
}
