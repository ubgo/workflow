package middleware_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// claimThreshold is small so the tests' "big" payloads stay readable.
const claimThreshold = 64

func newClaimCheck(t *testing.T, store middleware.BlobStore) *middleware.ClaimCheck {
	t.Helper()
	cc, err := middleware.NewClaimCheck(store, claimThreshold)
	if err != nil {
		t.Fatal(err)
	}
	return cc
}

// runCtx is a step ctx inside a run carrying token in ClaimCheckLabel.
func runCtx(token string) context.Context {
	return workflow.WithStepInfo(context.Background(), workflow.StepInfo{
		Workflow: "w", Step: "s", Attempt: 1,
		Labels: map[string]string{middleware.ClaimCheckLabel: token},
	})
}

func claimRef(key string) []byte {
	b, _ := json.Marshal("wfclaim1:" + key)
	return b
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestClaimCheck_MemoryDriverEndToEnd enqueues a big payload on the real
// memory driver through a fan-in workflow and proves that every step sees
// exactly the bytes it would see without the claim check, that the driver
// only ever carried references, and that the run's blobs are gone once it
// finishes.
func TestClaimCheck_MemoryDriverEndToEnd(t *testing.T) {
	store := middleware.NewMemoryBlobStore()
	cc := newClaimCheck(t, store)

	// outer sits outside cc.Step, so it sees what the driver moves.
	var mu sync.Mutex
	driverBytes := map[string][2][]byte{} // step -> {in, out} as the driver moved them
	outer := func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			out, err := next(ctx, in)
			info, _ := workflow.StepInfoFrom(ctx)
			mu.Lock()
			driverBytes[info.Step] = [2][]byte{bytes.Clone(in), bytes.Clone(out)}
			mu.Unlock()
			return out, err
		}
	}
	mem := memory.New(memory.WithMiddleware(outer, cc.Step()))
	var enqueued []byte
	spy := func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			enqueued = job.Payload
			return next(ctx, job)
		}
	}
	c := middleware.WrapClient(mem, cc.Enqueue(), spy)
	if err := cc.CleanupOnFinish(c); err != nil {
		t.Fatal(err)
	}

	payload := []byte(strings.Repeat("p", 500))
	bigOut := []byte(`{"big":"` + strings.Repeat("o", 300) + `"}`)
	notJSON := []byte(strings.Repeat("\x01raw", 50))
	got := map[string][]byte{}
	record := func(name string, out []byte) workflow.Handler {
		return func(_ context.Context, in []byte) ([]byte, error) {
			mu.Lock()
			got[name] = bytes.Clone(in)
			mu.Unlock()
			return out, nil
		}
	}
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "a", Run: record("a", bigOut)},
		{Name: "b", After: []string{"a"}, Run: record("b", notJSON)},
		{Name: "c", Run: record("c", []byte(`"small"`))},
		{Name: "d", After: []string{"a", "b", "c"}, Run: record("d", nil)},
	}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mem.Stop()

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(enqueued) >= claimThreshold {
		t.Fatalf("driver received %d payload bytes, want a reference", len(enqueued))
	}
	waitFor(t, "run end", func() bool {
		st, err := c.Status(ctx, ref)
		return err == nil && st.IsTerminal()
	})
	if st, _ := c.Status(ctx, ref); st != workflow.StatusSucceeded {
		t.Fatalf("run ended %q", st)
	}

	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(got["a"], payload) || !bytes.Equal(got["c"], payload) {
		t.Fatalf("first steps saw a=%d c=%d bytes, want the %d-byte payload", len(got["a"]), len(got["c"]), len(payload))
	}
	if !bytes.Equal(got["b"], bigOut) {
		t.Fatalf("single-parent step saw %q", got["b"])
	}
	want := workflow.StepInput(nil, []string{"a", "b", "c"}, map[string][]byte{"a": bigOut, "b": notJSON, "c": []byte(`"small"`)})
	if !bytes.Equal(got["d"], want) {
		t.Fatalf("fan-in step saw\n%s\nwant\n%s", got["d"], want)
	}
	// Every big value the driver moved is a reference; d's input is the
	// object of a's and b's references and c's small output.
	isRef := func(b []byte) bool { return bytes.HasPrefix(b, []byte(`"wfclaim1:`)) }
	for _, v := range [][]byte{driverBytes["a"][0], driverBytes["a"][1], driverBytes["b"][0], driverBytes["b"][1], driverBytes["c"][0]} {
		if !isRef(v) {
			t.Fatalf("driver carried %q, want a reference", v)
		}
	}
	if string(driverBytes["c"][1]) != `"small"` || len(driverBytes["d"][0]) > 3*100 {
		t.Fatalf("small output claimed, or fan-in input carried the blobs: %q", driverBytes["d"][0])
	}
	waitFor(t, "cleanup", func() bool { return store.Len() == 0 })
}

// TestClaimCheck_Threshold pins the edge: exactly threshold bytes stay
// inline and untouched (and nothing is stored), one more is claimed.
func TestClaimCheck_Threshold(t *testing.T) {
	store := middleware.NewMemoryBlobStore()
	cc := newClaimCheck(t, store)
	var seen workflow.Job
	enq := cc.Enqueue()(func(_ context.Context, job workflow.Job) (workflow.RunRef, error) {
		seen = job
		return workflow.RunRef{}, nil
	})

	inline := bytes.Repeat([]byte("x"), claimThreshold)
	if _, err := enq(context.Background(), workflow.Job{Payload: inline}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen.Payload, inline) || store.Len() != 0 {
		t.Fatalf("payload of exactly threshold bytes was claimed (stored %d)", store.Len())
	}
	if seen.Labels[middleware.ClaimCheckLabel] == "" {
		t.Fatal("small job not stamped with a token, so its outputs could never be claimed")
	}

	big := append(bytes.Clone(inline), 'x')
	if _, err := enq(context.Background(), workflow.Job{Payload: big}); err != nil {
		t.Fatal(err)
	}
	token := seen.Labels[middleware.ClaimCheckLabel]
	if !bytes.Equal(seen.Payload, claimRef(token+"/payload")) || store.Len() != 1 {
		t.Fatalf("threshold+1 bytes not claimed: payload %q, stored %d", seen.Payload, store.Len())
	}

	// The step side: an output of exactly threshold bytes stays inline.
	step := cc.Step()(func(context.Context, []byte) ([]byte, error) { return inline, nil })
	out, err := step(runCtx("tok"), nil)
	if err != nil || !bytes.Equal(out, inline) {
		t.Fatalf("output of threshold bytes changed: %q, %v", out, err)
	}
}

// TestClaimCheck_LookalikeNotResolved proves that a value shaped like a
// reference is fetched only when its key is under the run's own token:
// another run's key, a bare prefix, and a step outside a stamped run all
// pass through untouched, even though the store holds the named blob.
func TestClaimCheck_LookalikeNotResolved(t *testing.T) {
	store := middleware.NewMemoryBlobStore()
	ctx := context.Background()
	_ = store.Put(ctx, "other/payload", []byte("another run's secret"))
	_ = store.Put(ctx, "tok", []byte("unscoped"))
	_ = store.Put(ctx, "tok/payload", []byte("mine"))
	cc := newClaimCheck(t, store)
	var seen []byte
	step := cc.Step()(func(_ context.Context, in []byte) ([]byte, error) {
		seen = in
		return nil, nil
	})

	other := claimRef("other/payload")
	cases := map[string]struct {
		ctx context.Context
		in  []byte
	}{
		"another run's key":       {runCtx("tok"), other},
		"no key under the token":  {runCtx("tok"), claimRef("tok")},
		"token as a mere prefix":  {runCtx("to"), claimRef("tok/payload")},
		"in a parents object":     {runCtx("tok"), []byte(`{"a":` + string(other) + `,"b":1}`)},
		"step outside a run":      {ctx, claimRef("tok/payload")},
		"run without the label":   {workflow.WithStepInfo(ctx, workflow.StepInfo{Labels: map[string]string{}}), claimRef("tok/payload")},
		"prefix in a bare string": {runCtx("tok"), []byte(`wfclaim1:tok/payload`)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := step(tc.ctx, tc.in); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(seen, tc.in) {
				t.Fatalf("lookalike was resolved to %q", seen)
			}
		})
	}

	// The control: the run's own reference does resolve.
	if _, err := step(runCtx("tok"), claimRef("tok/payload")); err != nil || string(seen) != "mine" {
		t.Fatalf("own reference not resolved: %q, %v", seen, err)
	}
}

// TestClaimCheck_RetriedStepSeesPayload fails the first attempt on the
// memory driver: the payload must still be there for the retry, so
// cleanup waits for the run's end.
func TestClaimCheck_RetriedStepSeesPayload(t *testing.T) {
	store := middleware.NewMemoryBlobStore()
	cc := newClaimCheck(t, store)
	mem := memory.New(memory.WithMiddleware(cc.Step()))
	c := middleware.WrapClient(mem, cc.Enqueue())
	if err := cc.CleanupOnFinish(c); err != nil {
		t.Fatal(err)
	}
	payload := []byte(strings.Repeat("r", 200))
	var mu sync.Mutex
	var inputs [][]byte
	wf := workflow.Workflow{Name: "w", Retry: workflow.RetryPolicy{Max: 2}, Steps: []workflow.Step{{
		Name: "s",
		Run: func(_ context.Context, in []byte) ([]byte, error) {
			mu.Lock()
			defer mu.Unlock()
			inputs = append(inputs, bytes.Clone(in))
			if len(inputs) == 1 {
				return nil, errors.New("transient")
			}
			return nil, nil
		},
	}}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mem.Stop()
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "run end", func() bool {
		st, err := c.Status(ctx, ref)
		return err == nil && st.IsTerminal()
	})
	if st, _ := c.Status(ctx, ref); st != workflow.StatusSucceeded {
		t.Fatalf("run ended %q", st)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(inputs) != 2 || !bytes.Equal(inputs[0], payload) || !bytes.Equal(inputs[1], payload) {
		t.Fatalf("attempts saw %d inputs, want the payload twice", len(inputs))
	}
	waitFor(t, "cleanup", func() bool { return store.Len() == 0 })
}

// TestClaimCheck_CleanupOnFinish drives run ends by hand: nothing is
// deleted before the end, everything under the run's token after it, and
// another run's blobs are left alone. A store without DeletePrefix loses
// only the payload.
func TestClaimCheck_CleanupOnFinish(t *testing.T) {
	ctx := context.Background()
	t.Run("prefix deleter", func(t *testing.T) {
		store := middleware.NewMemoryBlobStore()
		cc := newClaimCheck(t, store)
		n := &notifierStub{stubClient: &stubClient{}}
		if err := cc.CleanupOnFinish(n); err != nil {
			t.Fatal(err)
		}
		c := middleware.WrapClient(n, cc.Enqueue())
		big := []byte(strings.Repeat("x", 100))
		for range 2 {
			if _, err := c.Enqueue(ctx, workflow.Job{Payload: big}); err != nil {
				t.Fatal(err)
			}
		}
		token := n.jobs[0].Labels[middleware.ClaimCheckLabel]
		_ = store.Put(ctx, token+"/out/x", big) // a step output of run 0
		if store.Len() != 3 {
			t.Fatalf("stored %d blobs before any end, want 3", store.Len())
		}
		n.finish(0, workflow.StatusFailed)
		if store.Len() != 1 {
			t.Fatalf("after run 0 ended %d blobs remain, want run 1's payload only", store.Len())
		}
		if _, err := store.Get(ctx, n.jobs[1].Labels[middleware.ClaimCheckLabel]+"/payload"); err != nil {
			t.Fatalf("another run's payload was deleted: %v", err)
		}
		n.finish(0, workflow.StatusFailed) // at-least-once delivery
	})
	t.Run("plain store", func(t *testing.T) {
		store := plainStore{middleware.NewMemoryBlobStore()}
		cc := newClaimCheck(t, store)
		n := &notifierStub{stubClient: &stubClient{}}
		if err := cc.CleanupOnFinish(n); err != nil {
			t.Fatal(err)
		}
		c := middleware.WrapClient(n, cc.Enqueue())
		if _, err := c.Enqueue(ctx, workflow.Job{Payload: []byte(strings.Repeat("x", 100))}); err != nil {
			t.Fatal(err)
		}
		token := n.jobs[0].Labels[middleware.ClaimCheckLabel]
		_ = store.Put(ctx, token+"/out/x", []byte("o"))
		n.finish(0, workflow.StatusSucceeded)
		if store.Len() != 1 {
			t.Fatalf("plain store holds %d blobs, want the output left to expiry", store.Len())
		}
	})
	t.Run("no notifier", func(t *testing.T) {
		cc := newClaimCheck(t, middleware.NewMemoryBlobStore())
		var ue *workflow.UnsupportedError
		if err := cc.CleanupOnFinish(&stubClient{}); !errors.As(err, &ue) || ue.Missing != workflow.CapRunFinished {
			t.Fatalf("got %v, want an UnsupportedError naming CapRunFinished", err)
		}
	})
}

// plainStore hides MemoryBlobStore's DeletePrefix.
type plainStore struct{ m *middleware.MemoryBlobStore }

func (p plainStore) Put(ctx context.Context, k string, d []byte) error { return p.m.Put(ctx, k, d) }
func (p plainStore) Get(ctx context.Context, k string) ([]byte, error) { return p.m.Get(ctx, k) }
func (p plainStore) Delete(ctx context.Context, k string) error        { return p.m.Delete(ctx, k) }
func (p plainStore) Len() int                                          { return p.m.Len() }

// TestClaimCheck_MissingBlobIsPermanent: a blob that is gone does not come
// back on retry.
func TestClaimCheck_MissingBlobIsPermanent(t *testing.T) {
	cc := newClaimCheck(t, middleware.NewMemoryBlobStore())
	step := cc.Step()(echo)
	_, err := step(runCtx("tok"), claimRef("tok/payload"))
	if !errors.Is(err, middleware.ErrBlobNotFound) || !workflow.IsPermanent(err) {
		t.Fatalf("got %v, want a permanent ErrBlobNotFound", err)
	}
}

// TestClaimCheck_OwnsItsLabel: a caller-chosen token, or any other label
// under the internal prefix, is refused with ErrInvalid and nothing reaches
// the driver; a fresh token is stamped otherwise, without mutating the
// caller's map.
func TestClaimCheck_OwnsItsLabel(t *testing.T) {
	cc := newClaimCheck(t, middleware.NewMemoryBlobStore())
	var seen []workflow.Job
	enq := cc.Enqueue()(func(_ context.Context, job workflow.Job) (workflow.RunRef, error) {
		seen = append(seen, job)
		return workflow.RunRef{}, nil
	})
	for _, k := range []string{middleware.ClaimCheckLabel, workflow.LabelPrefixInternal + "other"} {
		if _, err := enq(context.Background(), workflow.Job{Labels: map[string]string{k: "x"}}); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("label %q: err = %v, want ErrInvalid", k, err)
		}
	}
	if len(seen) != 0 {
		t.Fatal("a job with a reserved label reached the driver")
	}
	labels := map[string]string{"tenant": "t1"}
	if _, err := enq(context.Background(), workflow.Job{Labels: labels}); err != nil {
		t.Fatal(err)
	}
	if seen[0].Labels[middleware.ClaimCheckLabel] == "" || seen[0].Labels["tenant"] != "t1" || len(labels) != 1 {
		t.Fatalf("token not stamped, app labels lost or caller map mutated: %v", seen[0].Labels)
	}
}

// TestInternalLabels_ChainOrder: Unique and ClaimCheck share the internal
// prefix, so each accepts the other's labels in either order.
func TestInternalLabels_ChainOrder(t *testing.T) {
	cc := newClaimCheck(t, middleware.NewMemoryBlobStore())
	by := func(workflow.Job) string { return "k" }
	for name, mws := range map[string][]workflow.EnqueueMiddleware{
		"unique first":     {middleware.Unique(by, middleware.NewMemoryUnique(nil), time.Hour), cc.Enqueue()},
		"claimcheck first": {cc.Enqueue(), middleware.Unique(by, middleware.NewMemoryUnique(nil), time.Hour)},
	} {
		stub := &stubClient{}
		if _, err := middleware.WrapClient(stub, mws...).Enqueue(context.Background(), workflow.Job{Workflow: "w"}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		l := stub.jobs[0].Labels
		if l[middleware.ClaimCheckLabel] == "" || l[middleware.UniqueKeyLabel] != "k" {
			t.Fatalf("%s: labels = %v", name, l)
		}
	}
}

// TestClaimCheck_Config refuses a bad store, threshold or key source.
func TestClaimCheck_Config(t *testing.T) {
	if _, err := middleware.NewClaimCheck(nil, 1); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil store: %v", err)
	}
	if _, err := middleware.NewClaimCheck(middleware.NewMemoryBlobStore(), 0); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("zero threshold: %v", err)
	}
	for _, k := range []string{"", "a/b"} {
		cc, err := middleware.NewClaimCheck(middleware.NewMemoryBlobStore(), 1,
			middleware.WithClaimKeys(func() (string, error) { return k, nil }))
		if err != nil {
			t.Fatal(err)
		}
		enq := cc.Enqueue()(func(context.Context, workflow.Job) (workflow.RunRef, error) { return workflow.RunRef{}, nil })
		if _, err := enq(context.Background(), workflow.Job{}); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("key %q: %v", k, err)
		}
	}
}

// TestClaimCheck_WithCodec stacks the claim check outside a codec on the
// memory driver: the store holds only sealed bytes and the step still sees
// the plain payload.
func TestClaimCheck_WithCodec(t *testing.T) {
	store := &recordingStore{MemoryBlobStore: middleware.NewMemoryBlobStore()}
	cc := newClaimCheck(t, store)
	codec, _ := middleware.Encrypt(testKey(7))
	mem := memory.New(memory.WithMiddleware(cc.Step(), middleware.DecodeStep(codec)))
	c := middleware.WithCodec(mem, codec, cc.Enqueue())

	secret := []byte(strings.Repeat("secret", 40))
	got := make(chan []byte, 1)
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }},
		{Name: "b", After: []string{"a"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			got <- bytes.Clone(in)
			return nil, nil
		}},
	}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mem.Stop()
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: secret}); err != nil {
		t.Fatal(err)
	}
	select {
	case in := <-got:
		if !bytes.Equal(in, secret) {
			t.Fatalf("step b saw %d bytes, want the plain payload", len(in))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("step b never ran")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.puts) != 2 {
		t.Fatalf("store got %d puts, want the payload and a's output", len(store.puts))
	}
	for _, b := range store.puts {
		if bytes.Contains(b, []byte("secret")) {
			t.Fatal("store holds plaintext: the claim check ran before the codec")
		}
	}
}

// recordingStore keeps a copy of every Put, so a test can read what
// reached the store.
type recordingStore struct {
	*middleware.MemoryBlobStore
	mu   sync.Mutex
	puts [][]byte
}

func (r *recordingStore) Put(ctx context.Context, k string, d []byte) error {
	r.mu.Lock()
	r.puts = append(r.puts, bytes.Clone(d))
	r.mu.Unlock()
	return r.MemoryBlobStore.Put(ctx, k, d)
}
