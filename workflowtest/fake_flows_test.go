package workflowtest

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
)

func flowFake(t *testing.T, opts ...FakeOption) *Fake {
	t.Helper()
	f := NewFake(opts...)
	wf := workflow.Workflow{Name: "etl", Steps: []workflow.Step{
		{Name: "extract", Run: noopHandler},
		{Name: "load", After: []string{"extract"}, Run: noopHandler},
	}}
	if err := f.Register(wf); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestFakeResetRun(t *testing.T) {
	ctx := context.Background()
	f := flowFake(t)
	ref, err := f.Enqueue(ctx, workflow.Job{Workflow: "etl"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ResetRun(ctx, ref, "extract"); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("reset of a queued run: %v, want ErrWrongState", err)
	}
	if err := f.Finish(ref, workflow.StatusFailed); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ResetRun(ctx, ref, "nope"); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("reset from an unknown step: %v, want ErrInvalid", err)
	}
	got, err := f.ResetRun(ctx, ref, "extract")
	if err != nil || got != ref {
		t.Fatalf("ResetRun = %+v, %v", got, err)
	}
	if st, _ := f.Status(ctx, ref); st != workflow.StatusQueued {
		t.Fatalf("status after reset = %s, want queued", st)
	}
	f.AssertReset(t, ref, "extract")
	if _, err := f.ResetRun(ctx, workflow.RunRef{RunID: "nope"}, "extract"); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("unknown ref: %v", err)
	}

	if len(spyErrs(func(rt testing.TB) { f.AssertReset(rt, ref, "load") })) == 0 {
		t.Error("AssertReset passed for a step the run was not reset from")
	}
}

func TestFakeSignalWaiting(t *testing.T) {
	ctx := context.Background()
	f := flowFake(t)
	n, err := f.SignalWaiting(ctx, workflow.Event{Name: "refresh", Key: "k"})
	if err != nil || n != 0 {
		t.Fatalf("SignalWaiting = %d, %v; want 0, nil", n, err)
	}
	f.AssertSignalledWaiting(t, "refresh", "k")
	if err := f.Signal(ctx, workflow.Event{Name: "kept", Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if len(spyErrs(func(rt testing.TB) { f.AssertSignalledWaiting(rt, "kept", "k") })) == 0 {
		t.Error("AssertSignalledWaiting counted an event sent with Signal")
	}
	if len(spyErrs(func(rt testing.TB) { f.AssertSignalled(rt, "refresh", "k") })) == 0 {
		t.Error("AssertSignalled counted an event sent with SignalWaiting")
	}
}

// onlyClient has none of the optional interfaces, like a driver without
// ResetRun or SignalWaiting.
type onlyClient struct{ workflow.Client }

func TestFakeLikeRefusesWhatTheClientLacks(t *testing.T) {
	ctx := context.Background()
	like := onlyClient{NewFake()}
	f := flowFake(t, FakeLike(like))
	ref, err := f.Enqueue(ctx, workflow.Job{Workflow: "etl"})
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Finish(ref, workflow.StatusFailed)
	if _, err := f.ResetRun(ctx, ref, "extract"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("ResetRun on a Fake like a client without it: %v, want ErrUnsupported", err)
	}
	if _, err := f.SignalWaiting(ctx, workflow.Event{Name: "x"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("SignalWaiting on a Fake like a client without it: %v, want ErrUnsupported", err)
	}
	if len(f.SignalsWaiting()) != 0 {
		t.Error("a refused SignalWaiting was recorded")
	}
	for _, op := range []workflow.Op{workflow.OpResetRun, workflow.OpSignalWaiting} {
		if workflow.Supports(f, op) {
			t.Errorf("Supports(%s) = true on a Fake like a client without it", op)
		}
		if !workflow.Supports(NewFake(), op) {
			t.Errorf("Supports(%s) = false on a default Fake", op)
		}
	}
	if !workflow.Supports(flowFake(t, FakeLike(NewFake())), workflow.OpResetRun) {
		t.Error("FakeLike of a client with ResetRun refuses it")
	}
}

func TestFakePromises(t *testing.T) {
	ctx := context.Background()
	f := flowFake(t)
	if err := workflow.ResolvePromise(ctx, f, f.Promises(), "p1", []byte(`"paid"`)); err != nil {
		t.Fatal(err)
	}
	if err := workflow.RejectPromise(ctx, f, f.Promises(), "p2", "declined"); err != nil {
		t.Fatal(err)
	}
	f.AssertPromiseResolved(t, "p1", []byte(`"paid"`))
	f.AssertPromiseResolved(t, "p1", nil)
	f.AssertPromiseRejected(t, "p2", "declined")
	f.AssertPromiseRejected(t, "p2", "")
	f.AssertPromisePending(t, "p3")

	for name, check := range map[string]func(testing.TB){
		"resolved with another value": func(rt testing.TB) { f.AssertPromiseResolved(rt, "p1", []byte(`"other"`)) },
		"rejected read as resolved":   func(rt testing.TB) { f.AssertPromiseResolved(rt, "p2", nil) },
		"resolved read as rejected":   func(rt testing.TB) { f.AssertPromiseRejected(rt, "p1", "") },
		"another reason":              func(rt testing.TB) { f.AssertPromiseRejected(rt, "p2", "late") },
		"pending read as resolved":    func(rt testing.TB) { f.AssertPromiseResolved(rt, "p3", nil) },
		"settled read as pending":     func(rt testing.TB) { f.AssertPromisePending(rt, "p1") },
	} {
		if len(spyErrs(check)) == 0 {
			t.Errorf("%s: the assertion passed", name)
		}
	}

	// Settled in the Fake's store but signalled through another client:
	// the waiting run would never hear of it.
	other := flowFake(t)
	if err := workflow.ResolvePromise(ctx, other, f.Promises(), "p4", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	if len(spyErrs(func(rt testing.TB) { f.AssertPromiseResolved(rt, "p4", nil) })) == 0 {
		t.Error("AssertPromiseResolved passed for a settlement signalled through another client")
	}
}

func TestFakeObjectCalls(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	cart, err := workflow.DefineObject(workflow.ObjectSpec[int]{Name: "cart", Store: workflow.NewMemoryJournal(),
		Methods: map[string]workflow.ObjectMethod[int]{"add": func(_ context.Context, s int, _ []byte) (int, []byte, error) { return s + 1, nil, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Register(cart); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := workflow.CallObject(ctx, f, "cart", "alice", "add", []byte(`{"sku":"a1"}`), workflow.Job{}); err != nil {
		t.Fatal(err)
	}
	f.AssertObjectCalled(t, "cart", "alice", "add")
	f.AssertObjectNotCalled(t, "cart", "bob")
	calls := f.ObjectCalls("cart")
	if len(calls) != 1 || calls[0].Key != "alice" || calls[0].Method != "add" || string(calls[0].Args) != `{"sku":"a1"}` {
		t.Fatalf("ObjectCalls = %+v", calls)
	}
	for name, check := range map[string]func(testing.TB){
		"another key":    func(rt testing.TB) { f.AssertObjectCalled(rt, "cart", "bob", "add") },
		"another method": func(rt testing.TB) { f.AssertObjectCalled(rt, "cart", "alice", "remove") },
		"called key":     func(rt testing.TB) { f.AssertObjectNotCalled(rt, "cart", "alice") },
	} {
		if len(spyErrs(check)) == 0 {
			t.Errorf("%s: the assertion passed", name)
		}
	}
}
