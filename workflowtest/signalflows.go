package workflowtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// SignalHarness builds what the signal-flow cases run on: Start as for
// FlowHarness, and Promises, the PromiseStore the run's side and the
// caller share (the driver's durable one where it has one).
//
//ds:def id=workflowtest-signalharness-59st97g3 owner=@khanakia stability=stable
type SignalHarness struct {
	Start    func(t *testing.T, wfs ...workflow.Workflow) workflow.Client
	Promises func(t *testing.T, c workflow.Client) workflow.PromiseStore
}

// RunSignalFlows proves the signal helpers of the core on a driver: one
// signal resuming several waiting runs (broadcast), SignalWithStart,
// Update answered by UpdateHandler, a promise resolved from outside the
// run, ContinueAsNew and CancelSteps. A driver without a capability a
// case needs skips that case.
//
//ds:def id=workflowtest-runsignalflows-x6z59d2p owner=@khanakia stability=stable
func RunSignalFlows(t *testing.T, h SignalHarness) {
	waiter := func(name string) workflow.Workflow {
		return workflow.Workflow{Name: name, Steps: []workflow.Step{
			{Name: "wait", Wait: &workflow.WaitSpec{Event: "go", KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["key"] })}},
			{Name: "echo", After: []string{"wait"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
		}}
	}

	t.Run("broadcast", func(t *testing.T) {
		c := h.Start(t, waiter("sig-waiter"))
		needCaps(t, c, workflow.CapWaitForEvent, workflow.CapResult)
		ctx := context.Background()
		var refs []workflow.RunRef
		for range 3 {
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sig-waiter", Labels: map[string]string{"key": "k1"}})
			if err != nil {
				t.Fatal(err)
			}
			refs = append(refs, ref)
		}
		other, err := c.Enqueue(ctx, workflow.Job{Workflow: "sig-waiter", Labels: map[string]string{"key": "k2"}})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range refs {
			flowStatus(t, c, r, workflow.StatusWaiting)
		}
		if err := c.Signal(ctx, workflow.Event{Name: "go", Key: "k1", Payload: []byte(`"all"`)}); err != nil {
			t.Fatal(err)
		}
		for i, r := range refs {
			out, err := flowAwait(t, c, r)
			if err != nil || string(out) != `"all"` {
				t.Fatalf("waiter %d: %q %v", i, out, err)
			}
		}
		if st, err := c.Status(ctx, other); err != nil || st.IsTerminal() {
			t.Fatalf("a run waiting on another key ended: %s %v", st, err)
		}
	})

	t.Run("transient", func(t *testing.T) {
		gate := make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(gate) }) }
		defer release()
		gated := workflow.Workflow{Name: "sig-gated", Steps: []workflow.Step{
			{Name: "gate", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				select {
				case <-gate:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}},
			{Name: "wait", After: []string{"gate"}, Wait: &workflow.WaitSpec{Event: "go", Key: "t1"}},
		}}
		c := h.Start(t, waiter("sig-waiter"), gated)
		ts, ok := c.(workflow.TransientSignaller)
		if !ok {
			t.Skip("driver has no TransientSignaller")
		}
		needCaps(t, c, workflow.CapWaitForEvent, workflow.CapResult)
		ctx := context.Background()
		var refs []workflow.RunRef
		for range 2 {
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sig-waiter", Labels: map[string]string{"key": "t1"}})
			if err != nil {
				t.Fatal(err)
			}
			flowStatus(t, c, ref, workflow.StatusWaiting)
			refs = append(refs, ref)
		}
		// Enqueued before the hint, but not waiting yet: it must not take it.
		late, err := c.Enqueue(ctx, workflow.Job{Workflow: "sig-gated"})
		if err != nil {
			t.Fatal(err)
		}
		flowStatus(t, c, late, workflow.StatusRunning)
		n, err := ts.SignalWaiting(ctx, workflow.Event{Name: "go", Key: "t1", Payload: []byte(`"hint"`)})
		if err != nil || n != 2 {
			t.Fatalf("SignalWaiting = %d, %v; want the 2 waiting runs", n, err)
		}
		for _, r := range refs {
			if out, err := flowAwait(t, c, r); err != nil || string(out) != `"hint"` {
				t.Fatalf("waiting run: %q %v", out, err)
			}
		}
		release()
		flowStatus(t, c, late, workflow.StatusWaiting)
		time.Sleep(50 * time.Millisecond)
		if st, err := c.Status(ctx, late); err != nil || st != workflow.StatusWaiting {
			t.Fatalf("the late run took the transient event: %s %v", st, err)
		}
		if err := c.Signal(ctx, workflow.Event{Name: "go", Key: "t1"}); err != nil {
			t.Fatal(err)
		}
		flowStatus(t, c, late, workflow.StatusSucceeded)
		if n, err := ts.SignalWaiting(ctx, workflow.Event{Name: "go", Key: "nobody"}); err != nil || n != 0 {
			t.Fatalf("SignalWaiting with no waiter = %d, %v", n, err)
		}
	})

	t.Run("signal-with-start", func(t *testing.T) {
		c := h.Start(t, waiter("sig-waiter"))
		needCaps(t, c, workflow.CapWaitForEvent, workflow.CapIdempotencyKey, workflow.CapResult)
		ctx := context.Background()
		job := workflow.Job{Workflow: "sig-waiter", IdempotencyKey: "cart-7", Labels: map[string]string{"key": "cart-7"}}
		first, err := workflow.SignalWithStart(ctx, c, job, workflow.Event{Name: "go", Key: "cart-7", Payload: []byte(`"item"`)})
		if err != nil {
			t.Fatal(err)
		}
		out, err := flowAwait(t, c, first)
		if err != nil || string(out) != `"item"` {
			t.Fatalf("started run: %q %v", out, err)
		}
		if _, err := workflow.SignalWithStart(ctx, c, workflow.Job{Workflow: "sig-waiter"}, workflow.Event{Name: "go"}); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("no idempotency key: %v", err)
		}
	})

	t.Run("update", func(t *testing.T) {
		var c workflow.Client
		var store workflow.PromiseStore
		wf := workflow.Workflow{Name: "sig-counter", Steps: []workflow.Step{
			{Name: "wait", Wait: &workflow.WaitSpec{Event: "add", KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["key"] })}},
			{Name: "apply", After: []string{"wait"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
				return workflow.UpdateHandler(store, func(_ context.Context, args []byte) ([]byte, error) {
					var n int
					if err := json.Unmarshal(args, &n); err != nil {
						return nil, workflow.Permanent(err)
					}
					return json.Marshal(n + 1)
				})(ctx, in)
			}},
		}}
		c = h.Start(t, wf)
		needCaps(t, c, workflow.CapWaitForEvent)
		store = h.Promises(t, c)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "sig-counter", Labels: map[string]string{"key": "c1"}}); err != nil {
			t.Fatal(err)
		}
		out, err := workflow.Update(ctx, c, store, workflow.Event{Name: "add", Key: "c1", Payload: []byte("41")}, workflow.AwaitOptions{Poll: 5 * time.Millisecond})
		if err != nil || string(out) != "42" {
			t.Fatalf("update answered %q, %v; want 42", out, err)
		}
	})

	t.Run("promise", func(t *testing.T) {
		var store workflow.PromiseStore
		wf := workflow.Workflow{Name: "sig-approval", Steps: []workflow.Step{{Name: "await", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var id string
			if err := json.Unmarshal(in, &id); err != nil {
				return nil, workflow.Permanent(err)
			}
			return workflow.AwaitPromise(ctx, store, id, 5*time.Millisecond)
		}}}}
		c := h.Start(t, wf)
		needCaps(t, c, workflow.CapSnooze, workflow.CapResult)
		store = h.Promises(t, c)
		ctx := context.Background()
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sig-approval", Payload: []byte(`"req-1"`)})
		if err != nil {
			t.Fatal(err)
		}
		flowStatus(t, c, ref, workflow.StatusWaiting)
		if err := workflow.ResolvePromise(ctx, c, store, "req-1", []byte(`"approved"`)); err != nil {
			t.Fatal(err)
		}
		if err := workflow.ResolvePromise(ctx, c, store, "req-1", []byte(`"approved"`)); err != nil {
			t.Fatalf("resolving again with the same value: %v", err)
		}
		if err := workflow.RejectPromise(ctx, c, store, "req-1", "too late"); !errors.Is(err, workflow.ErrPromiseSettled) {
			t.Fatalf("a different outcome after settlement: %v", err)
		}
		out, err := flowAwait(t, c, ref)
		if err != nil || string(out) != `"approved"` {
			t.Fatalf("run got %q, %v", out, err)
		}
	})

	t.Run("continue-as-new", func(t *testing.T) {
		var c workflow.Client
		wf := workflow.Workflow{Name: "sig-poller", Steps: []workflow.Step{{Name: "round", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			var n int
			if err := json.Unmarshal(in, &n); err != nil {
				return nil, workflow.Permanent(err)
			}
			if n < 3 {
				if _, err := workflow.ContinueAsNew(ctx, c, fmt.Appendf(nil, "%d", n+1)); err != nil {
					return nil, err
				}
			}
			from, _ := workflow.ContinuedFrom(ctx)
			return json.Marshal(map[string]any{"round": n, "from": from})
		}}}}
		c = h.Start(t, wf)
		needCaps(t, c, workflow.CapIdempotencyKey, workflow.CapRunQuery)
		ctx := context.Background()
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "sig-poller", Payload: []byte("1"), Labels: map[string]string{"tenant-ish": "a"}}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "sig-poller", Statuses: []workflow.Status{workflow.StatusSucceeded}})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Runs) == 3 {
				for _, r := range page.Runs {
					if r.Labels["tenant-ish"] != "a" {
						t.Fatalf("app label not carried: %v", r.Labels)
					}
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d rounds finished, want 3", len(page.Runs))
			}
			time.Sleep(5 * time.Millisecond)
		}
	})

	t.Run("cancel-steps", func(t *testing.T) {
		var mu sync.Mutex
		started := map[string]bool{}
		hold := func(name string) workflow.Handler {
			return func(ctx context.Context, _ []byte) ([]byte, error) {
				mu.Lock()
				started[name] = true
				mu.Unlock()
				<-ctx.Done()
				if errors.Is(context.Cause(ctx), workflow.ErrStepCancelled) {
					return json.Marshal(name + " withdrawn")
				}
				return nil, ctx.Err()
			}
		}
		wf := workflow.Workflow{Name: "sig-region", Steps: []workflow.Step{
			{Name: "a", Run: hold("a")},
			{Name: "b", Run: hold("b")},
			{Name: "join", After: []string{"a", "b"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
		}}
		c := h.Start(t, wf)
		if _, ok := c.(workflow.StepCanceller); !ok {
			t.Skip("driver has no StepCanceller")
		}
		ctx := context.Background()
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sig-region"})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			mu.Lock()
			both := started["a"] && started["b"]
			mu.Unlock()
			if both {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("steps did not start")
			}
			time.Sleep(2 * time.Millisecond)
		}
		if err := workflow.CancelSteps(ctx, c, ref, "a", "b"); err != nil {
			t.Fatal(err)
		}
		out, err := flowAwait(t, c, ref)
		if err != nil || !bytes.Contains(out, []byte("a withdrawn")) || !bytes.Contains(out, []byte("b withdrawn")) {
			t.Fatalf("join got %q, %v", out, err)
		}
	})
}

// RunPromiseStore is the PromiseStore contract: Settle stores once and
// every later or concurrent Settle reads the first value, Get and Delete
// behave, and an id is independent of every other.
//
//ds:def id=workflowtest-runpromisestore-tgdywy8m owner=@khanakia stability=stable
func RunPromiseStore(t *testing.T, s workflow.PromiseStore) {
	ctx := context.Background()
	if _, ok, err := s.Get(ctx, "p-missing"); err != nil || ok {
		t.Fatalf("Get of an unknown id: %v %v", ok, err)
	}
	const n = 16
	var wg sync.WaitGroup
	results := make([][]byte, n)
	firsts := make([]bool, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], firsts[i], errs[i] = s.Settle(ctx, "p-race", fmt.Appendf(nil, "v%d", i))
		}()
	}
	wg.Wait()
	winners := 0
	var won []byte
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("Settle %d: %v", i, errs[i])
		}
		if firsts[i] {
			winners++
			won = results[i]
		}
	}
	if winners != 1 {
		t.Fatalf("%d settles won, want exactly 1", winners)
	}
	for i := range n {
		if !bytes.Equal(results[i], won) {
			t.Fatalf("Settle %d read %q, want the winner's %q", i, results[i], won)
		}
	}
	got, ok, err := s.Get(ctx, "p-race")
	if err != nil || !ok || !bytes.Equal(got, won) {
		t.Fatalf("Get after the race: %q %v %v", got, ok, err)
	}
	if _, first, err := s.Settle(ctx, "p-other", []byte("x")); err != nil || !first {
		t.Fatalf("another id: %v %v", first, err)
	}
	if err := s.Delete(ctx, "p-race"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get(ctx, "p-race"); ok {
		t.Fatal("Get after Delete still finds the value")
	}
	if err := s.Delete(ctx, "p-race"); err != nil {
		t.Fatalf("Delete of an unknown id: %v", err)
	}
	if v, first, err := s.Settle(ctx, "p-race", []byte("again")); err != nil || !first || string(v) != "again" {
		t.Fatalf("Settle after Delete: %q %v %v", v, first, err)
	}
	if v, _, _ := s.Get(ctx, "p-other"); string(v) != "x" {
		t.Fatalf("Delete touched another id: %q", v)
	}
}
