package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// auditLog is one ordered log several plugins can share, so a test reads
// the order of their contributions from one place.
type auditLog struct {
	mu    sync.Mutex
	lines []string
	done  chan struct{}
}

func newAuditLog(runs int) *auditLog { return &auditLog{done: make(chan struct{}, runs)} }

func (l *auditLog) record(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, s)
}

func (l *auditLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// auditPlugin uses every contribution: it records each enqueue, each step
// attempt and each run end into its log.
type auditPlugin struct {
	name string
	log  *auditLog
}

func (a *auditPlugin) Name() string { return a.name }

func (a *auditPlugin) EnqueueMiddleware() []workflow.EnqueueMiddleware {
	return []workflow.EnqueueMiddleware{func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			a.log.record(a.name + " enqueue " + job.Workflow)
			return next(ctx, job)
		}
	}}
}

func (a *auditPlugin) StepMiddleware() []workflow.Middleware {
	return []workflow.Middleware{func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			a.log.record(a.name + " step")
			return next(ctx, in)
		}
	}}
}

func (a *auditPlugin) RunFinishedObservers() []workflow.RunFinishedFunc {
	return []workflow.RunFinishedFunc{func(_ context.Context, ev workflow.RunEvent) {
		a.log.record(a.name + " finished " + ev.Workflow + " " + string(ev.Status))
		a.log.done <- struct{}{}
	}}
}

// ExampleUse adds a plugin that contributes enqueue middleware, step
// middleware and a run-finished observer, with one call.
func ExampleUse() {
	log := newAuditLog(1)
	c, err := workflow.Use(memory.New(), &auditPlugin{name: "audit", log: log})
	if err != nil {
		panic(err)
	}
	err = c.Register(workflow.Workflow{
		Name:  "greet",
		Steps: []workflow.Step{{Name: "say", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	})
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		panic(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "greet"}); err != nil {
		panic(err)
	}
	<-log.done
	for _, line := range log.all() {
		fmt.Println(line)
	}
	// Output:
	// audit enqueue greet
	// audit step
	// audit finished greet succeeded
}

// TestUse_OrderIsTheOrderGiven runs two plugins through a real driver: the
// first plugin's middleware is outermost on both enqueue and step, and its
// observer is registered first.
func TestUse_OrderIsTheOrderGiven(t *testing.T) {
	log := newAuditLog(2)
	c, err := workflow.Use(memory.New(), &auditPlugin{name: "a", log: log}, &auditPlugin{name: "b", log: log})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		log.record("handler")
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"}); err != nil {
		t.Fatal(err)
	}
	<-log.done
	<-log.done
	got := strings.Join(log.all(), ",")
	want := "a enqueue w,b enqueue w,a step,b step,handler,a finished w succeeded,b finished w succeeded"
	if got != want {
		t.Fatalf("order:\n got %s\nwant %s", got, want)
	}
}

// named is a plugin with a name and no contributions.
type named string

func (n named) Name() string { return string(n) }

// noNotifier hides the inner client's RunNotifier: embedding the Client
// interface promotes only its methods.
type noNotifier struct{ workflow.Client }

func TestUse_RefusesObserverWithoutRunNotifier(t *testing.T) {
	_, err := workflow.Use(noNotifier{memory.New()}, named("plain"), &auditPlugin{name: "audit", log: newAuditLog(1)})
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("err = %v, want an UnsupportedError", err)
	}
	if ue.Missing != workflow.CapRunFinished || !strings.Contains(ue.Error(), "plugin audit") {
		t.Fatalf("refusal does not name the capability and plugin: %v", ue)
	}
}

func TestUse_PluginWithoutObserverNeedsNoNotifier(t *testing.T) {
	if _, err := workflow.Use(noNotifier{memory.New()}, named("plain")); err != nil {
		t.Fatal(err)
	}
}

func TestUse_RefusesBadPluginLists(t *testing.T) {
	cases := []struct {
		name    string
		plugins []workflow.Plugin
		want    error
	}{
		{"duplicate name", []workflow.Plugin{named("x"), named("y"), named("x")}, workflow.ErrDuplicatePlugin},
		{"empty name", []workflow.Plugin{named("")}, workflow.ErrInvalid},
		{"nil plugin", []workflow.Plugin{nil}, workflow.ErrInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := workflow.Use(memory.New(), tc.plugins...); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestUse_NoPluginsReturnsClientUnchanged(t *testing.T) {
	inner := memory.New()
	c, err := workflow.Use(inner)
	if err != nil || c != workflow.Client(inner) {
		t.Fatalf("Use() = %v, %v; want the inner client", c, err)
	}
}

func TestUse_KeepsCapabilitiesAndForwardsNotifier(t *testing.T) {
	inner := memory.New()
	c, err := workflow.Use(inner, named("plain"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Capabilities() != inner.Capabilities() {
		t.Fatalf("caps = %v, want %v", c.Capabilities(), inner.Capabilities())
	}
	n, ok := c.(workflow.RunNotifier)
	if !ok {
		t.Fatal("wrapped client hides RunNotifier")
	}
	if err := n.OnRunFinished(func(context.Context, workflow.RunEvent) {}); err != nil {
		t.Fatal(err)
	}

	hidden, err := workflow.Use(noNotifier{memory.New()}, named("plain"))
	if err != nil {
		t.Fatal(err)
	}
	if err := hidden.(workflow.RunNotifier).OnRunFinished(func(context.Context, workflow.RunEvent) {}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("forwarding to a client without RunNotifier: err = %v, want ErrUnsupported", err)
	}
}

func TestUse_AfterStartNamesThePlugin(t *testing.T) {
	inner := memory.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := inner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := workflow.Use(inner, &auditPlugin{name: "audit", log: newAuditLog(1)})
	if !errors.Is(err, workflow.ErrAlreadyStarted) || !strings.Contains(err.Error(), `"audit"`) {
		t.Fatalf("err = %v, want ErrAlreadyStarted naming the plugin", err)
	}
}

// TestUse_RegisterLeavesCallerWorkflowAlone checks that wrapping handlers
// works on a copy: the caller's Steps still hold its own handler, and a
// wait step (no Run) is still a wait step.
func TestUse_RegisterLeavesCallerWorkflowAlone(t *testing.T) {
	log := newAuditLog(1)
	c, err := workflow.Use(memory.New(), &auditPlugin{name: "audit", log: log})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	run := func(context.Context, []byte) ([]byte, error) { calls++; return nil, nil }
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "s", Run: run},
		{Name: "wait", After: []string{"s"}, Wait: &workflow.WaitSpec{Event: "e", Key: "k"}},
	}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	if _, err := wf.Steps[0].Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(log.all()) != 0 {
		t.Fatalf("caller's handler was replaced: calls=%d log=%v", calls, log.all())
	}
	if wf.Steps[1].Kind() != workflow.StepWait {
		t.Fatal("wait step changed kind")
	}
}
