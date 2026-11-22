package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/ubgo/workflow"
)

// ObjectHarness builds what the virtual-object cases run on: Start as
// for FlowHarness, and Store, the JournalStore the object keeps its state
// in (the driver's durable one where it has one).
//
//ds:def id=workflowtest-objectharness-hkns6ws6 owner=@khanakia stability=stable
type ObjectHarness struct {
	Start func(t *testing.T, wfs ...workflow.Workflow) workflow.Client
	Store func(t *testing.T) workflow.JournalStore
}

// RunObjectFlows proves DefineObject and CallObject on a driver: calls of
// one key run one at a time and every call is applied exactly once to the
// key's state, keys are independent, and an unknown method fails its call
// without touching the state. The driver needs CapConcurrencyKey.
//
//ds:def id=workflowtest-runobjectflows-86ussnuu owner=@khanakia stability=stable
func RunObjectFlows(t *testing.T, h ObjectHarness) {
	type counter struct {
		N int `json:"n"`
	}
	var mu sync.Mutex
	running := map[string]int{}
	overlap := false
	store := h.Store(t)
	obj, err := workflow.DefineObject(workflow.ObjectSpec[counter]{
		Name:  "obj-counter",
		Store: store,
		Methods: map[string]workflow.ObjectMethod[counter]{
			"add": func(ctx context.Context, s counter, args []byte) (counter, []byte, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				key := info.Labels[workflow.LabelObjectKey]
				mu.Lock()
				running[key]++
				if running[key] > 1 {
					overlap = true
				}
				mu.Unlock()
				defer func() {
					mu.Lock()
					running[key]--
					mu.Unlock()
				}()
				var by int
				if err := json.Unmarshal(args, &by); err != nil {
					return s, nil, workflow.Permanent(err)
				}
				s.N += by
				out, err := json.Marshal(s.N)
				return s, out, err
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := h.Start(t, obj)
	needCaps(t, c, workflow.CapConcurrencyKey)
	ctx := context.Background()
	var refs []workflow.RunRef
	for i := range 25 {
		key := "a"
		if i%5 == 0 {
			key = "b"
		}
		ref, err := workflow.CallObject(ctx, c, "obj-counter", key, "add", []byte("1"), workflow.Job{})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	for _, r := range refs {
		if _, err := flowAwait(t, c, r); err != nil {
			t.Fatal(err)
		}
	}
	for key, want := range map[string]int{"a": 20, "b": 5} {
		got, ok, err := workflow.ObjectState[counter](ctx, store, "obj-counter", key)
		if err != nil || !ok || got.N != want {
			t.Fatalf("key %s: state %+v %v %v, want %d", key, got, ok, err, want)
		}
	}
	mu.Lock()
	if overlap {
		t.Error("two calls of one key ran at once")
	}
	mu.Unlock()

	bad, err := workflow.CallObject(ctx, c, "obj-counter", "a", "nope", nil, workflow.Job{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flowAwait(t, c, bad); err == nil {
		t.Fatal("an unknown method must fail its call")
	}
	if got, _, _ := workflow.ObjectState[counter](ctx, store, "obj-counter", "a"); got.N != 20 {
		t.Fatalf("a failed call changed the state: %+v", got)
	}
	if _, ok, err := workflow.ObjectState[counter](ctx, store, "obj-counter", "never"); ok || err != nil {
		t.Fatalf("a key never called: %v %v", ok, err)
	}
	if _, err := workflow.CallObject(ctx, c, "obj-counter", "", "add", nil, workflow.Job{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("empty key: %v", err)
	}
}
