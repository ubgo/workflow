package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

func TestRunKeyIsReserved(t *testing.T) {
	k := workflow.RunKey("abc")
	if k != "ubgo.workflow/run:abc" {
		t.Fatalf("RunKey = %q", k)
	}
	if _, reserved := workflow.ReservedLabel(k); !reserved {
		t.Fatalf("RunKey %q is not under a reserved prefix, so an app key could collide", k)
	}
}

func TestWaitOwnRunValidationAndCaps(t *testing.T) {
	wait := func(w workflow.WaitSpec) workflow.Workflow {
		return workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "wait", Wait: &w}}}
	}
	if got := workflow.Requires(wait(workflow.WaitSpec{Event: "e", OwnRun: true})); !got.Has(workflow.CapWaitOwnRun) || !got.Has(workflow.CapWaitForEvent) {
		t.Fatalf("Requires of an OwnRun wait = %s", got)
	}
	if got := workflow.Requires(wait(workflow.WaitSpec{Event: "e", Key: "k"})); got.Has(workflow.CapWaitOwnRun) {
		t.Fatalf("a keyed wait requires CapWaitOwnRun: %s", got)
	}
	for _, w := range []workflow.WaitSpec{
		{Event: "e", OwnRun: true, Key: "k"},
		{Event: "e", OwnRun: true, KeyFrom: workflow.KeyFunc(func(workflow.Job) string { return "k" })},
	} {
		if err := workflow.Validate(wait(w)); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("OwnRun with a key: %v", err)
		}
	}
	w := workflow.WaitSpec{Event: "e", OwnRun: true}
	if _, err := w.ResolveKey(workflow.Job{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("ResolveKey of an OwnRun wait: %v; want UnsupportedError (no run ID to resolve)", err)
	}
	if k, err := w.ResolveKeyFor(workflow.Job{}, "r1"); err != nil || k != workflow.RunKey("r1") {
		t.Fatalf("ResolveKeyFor = %q %v", k, err)
	}
	if _, err := w.ResolveKeyFor(workflow.Job{}, ""); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("ResolveKeyFor without a run: %v", err)
	}
	keyed := workflow.WaitSpec{Event: "e", Key: "k"}
	if k, err := keyed.ResolveKeyFor(workflow.Job{}, "r1"); err != nil || k != "k" {
		t.Fatalf("ResolveKeyFor of a keyed wait = %q %v", k, err)
	}
}

func TestOnSignalsRefusals(t *testing.T) {
	on := map[string]workflow.SignalHandler{"x": func(context.Context, workflow.RunSignal) error { return nil }}
	_, err := workflow.Use(workflowtest.NewFake(), workflow.OnSignals(workflow.SignalHandlers{On: on}))
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || !strings.Contains(err.Error(), "on-signals") {
		t.Fatalf("OnSignals on a client without RunSignalReader: %v", err)
	}
	if _, err := workflow.Use(memory.New(), workflow.OnSignals(workflow.SignalHandlers{})); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("OnSignals without handlers: %v", err)
	}
	if _, err := workflow.Use(memory.New(), workflow.OnSignals(workflow.SignalHandlers{On: map[string]workflow.SignalHandler{"x": nil}})); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("OnSignals with a nil handler: %v", err)
	}
	if _, err := workflow.Use(memory.New(), workflow.OnSignals(workflow.SignalHandlers{On: on, Batch: -1})); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("OnSignals with a negative batch: %v", err)
	}
	c, err := workflow.Use(memory.New(),
		workflow.OnSignals(workflow.SignalHandlers{Workflow: "a", On: on}),
		workflow.OnSignals(workflow.SignalHandlers{Workflow: "b", On: on}))
	if err != nil {
		t.Fatalf("two OnSignals for two workflows: %v", err)
	}
	if !workflow.Supports(c, workflow.OpRunSignals) {
		t.Fatal("Use hid the driver's RunSignalReader")
	}
}

func TestSignalRunAndParentRefusals(t *testing.T) {
	ctx := context.Background()
	if err := workflow.SignalRun(ctx, memory.New(), workflow.RunRef{}, "x", nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("SignalRun without a run: %v", err)
	}
	if err := workflow.SignalRun(ctx, memory.New(), workflow.RunRef{RunID: "r"}, "", nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("SignalRun without a name: %v", err)
	}
	noWait := workflowtest.NewFake(workflowtest.FakeCaps(workflow.CapsOf(workflow.CapCancel)))
	if err := workflow.SignalRun(ctx, noWait, workflow.RunRef{RunID: "r"}, "x", nil); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("SignalRun on a client without CapWaitForEvent: %v", err)
	}
	if err := workflow.SignalParent(ctx, memory.New(), "x", nil); !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
		t.Fatalf("SignalParent outside a step: %v", err)
	}
	child := workflow.WithStepInfo(ctx, workflow.StepInfo{RunID: "c1", Labels: map[string]string{workflow.LabelParentRun: "p1"}})
	if id, ok := workflow.ParentRun(child); !ok || id != "p1" {
		t.Fatalf("ParentRun = %q %v", id, ok)
	}
	f := workflowtest.NewFake()
	if err := f.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := workflow.SignalParent(child, f, "esc", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if sig := f.Signals(); len(sig) != 1 || sig[0].Key != workflow.RunKey("p1") || sig[0].Name != "esc" {
		t.Fatalf("SignalParent sent %+v", sig)
	}
}

// prefixed is a wrapper whose refs carry a prefix its steps never see,
// like regional or failover: SignalRun must reach the run's own ID.
type prefixed struct {
	workflow.Client
	got workflow.RunRef
}

func (p *prefixed) SignalRun(ctx context.Context, ref workflow.RunRef, ev workflow.Event) error {
	p.got = ref
	ev.Key = workflow.RunKey(workflow.RunID(strings.TrimPrefix(ref.RunID.String(), "eu:")))
	return p.Client.Signal(ctx, ev)
}

func TestSignalRunUsesRunSignaller(t *testing.T) {
	f := workflowtest.NewFake()
	if err := f.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	p := &prefixed{Client: f}
	if err := workflow.SignalRun(context.Background(), p, workflow.RunRef{RunID: "eu:r9"}, "go", nil); err != nil {
		t.Fatal(err)
	}
	if p.got.RunID != "eu:r9" {
		t.Fatalf("RunSignaller got %+v", p.got)
	}
	if sig := f.Signals(); len(sig) != 1 || sig[0].Key != workflow.RunKey("r9") {
		t.Fatalf("signals %+v", sig)
	}
	// Use's client forwards to the wrapper's RunSignaller.
	c, err := workflow.Use(p, workflow.EndRuns())
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.SignalRun(context.Background(), c, workflow.RunRef{RunID: "eu:r8"}, "go", nil); err != nil {
		t.Fatal(err)
	}
	if p.got.RunID != "eu:r8" {
		t.Fatalf("Use's client did not forward SignalRun: %+v", p.got)
	}
}

func TestByRunHelperRefusals(t *testing.T) {
	for _, a := range []workflow.ApprovalSpec{
		{Name: "a"},
		{Name: "a", OwnRun: true, Key: "k"},
		{Name: "a", OwnRun: true, KeyFrom: workflow.KeyFunc(func(workflow.Job) string { return "k" })},
	} {
		if _, err := workflow.ApprovalStep(a); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("ApprovalStep(%+v): %v", a, err)
		}
	}
	s, err := workflow.ApprovalStep(workflow.ApprovalSpec{Name: "a", OwnRun: true})
	if err != nil || !s.Wait.OwnRun || s.Wait.Key != "" {
		t.Fatalf("OwnRun approval step %+v %v", s.Wait, err)
	}
	steps, err := workflow.Race(workflow.RaceSpec{Name: "r", Event: "e", OwnRun: true, Branches: []workflow.Branch{{Name: "x", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
	if err != nil || !steps[0].Wait.OwnRun {
		t.Fatalf("OwnRun race %+v %v", steps, err)
	}
	ctx := context.Background()
	if err := workflow.SignalChoiceRun(ctx, memory.New(), workflow.RunRef{RunID: "r"}, "e", "", nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("SignalChoiceRun without a branch: %v", err)
	}
	if _, err := workflow.UpdateRun(ctx, memory.New(), workflow.NewMemoryPromises(), workflow.RunRef{}, "u", nil, workflow.AwaitOptions{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("UpdateRun without a run: %v", err)
	}
}
