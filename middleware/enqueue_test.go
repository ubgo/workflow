package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

func testKey(n byte) []byte { return bytes.Repeat([]byte{n}, 32) }

func TestEncrypt_RoundTripThroughEnqueueAndStep(t *testing.T) {
	codec, err := middleware.Encrypt(testKey(1))
	if err != nil {
		t.Fatal(err)
	}
	stub := &stubClient{}
	c := middleware.WrapClient(stub, middleware.EncodeEnqueue(codec))
	plain := []byte(`{"card":"4111"}`)
	if _, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "w", Payload: plain}); err != nil {
		t.Fatal(err)
	}
	stored := stub.jobs[0].Payload
	if bytes.Contains(stored, []byte("4111")) {
		t.Fatal("driver received plaintext")
	}

	var seen []byte
	step := middleware.DecodeStep(codec)(func(_ context.Context, in []byte) ([]byte, error) {
		seen = in
		return []byte("out"), nil
	})
	out, err := step(context.Background(), stored)
	if err != nil || !bytes.Equal(seen, plain) {
		t.Fatalf("step saw %q, err %v", seen, err)
	}
	if bytes.Equal(out, []byte("out")) {
		t.Fatal("step output stored unencoded")
	}
	// The output opens as the next step's input.
	next := middleware.DecodeStep(codec)(func(_ context.Context, in []byte) ([]byte, error) { seen = in; return nil, nil })
	if _, err := next(context.Background(), out); err != nil || string(seen) != "out" {
		t.Fatalf("output does not decode as a child's input: %q %v", seen, err)
	}

	// Equal payloads never encrypt to equal bytes.
	a, _ := codec.Encode(plain)
	b, _ := codec.Encode(plain)
	if bytes.Equal(a, b) {
		t.Fatal("nonce reused")
	}
}

func TestEncrypt_WrongKeyIsPermanentAndQuiet(t *testing.T) {
	right, _ := middleware.Encrypt(testKey(1))
	wrong, _ := middleware.Encrypt(testKey(2))
	enc := sealWith(t, right, []byte("top-secret"))
	ran := false
	step := middleware.DecodeStep(wrong)(func(context.Context, []byte) ([]byte, error) { ran = true; return nil, nil })
	_, err := step(context.Background(), enc)
	if ran || !workflow.IsPermanent(err) || !errors.Is(err, middleware.ErrDecode) {
		t.Fatalf("ran=%v err=%v", ran, err)
	}
	if bytes.Contains([]byte(err.Error()), []byte("top-secret")) || bytes.Contains([]byte(err.Error()), enc) {
		t.Fatal("error carries payload bytes")
	}
	// Tampering and truncation fail the same way.
	raw, _ := right.Encode([]byte("x"))
	raw[len(raw)-1] ^= 1
	if _, err := right.Decode(raw); err == nil {
		t.Fatal("tampered ciphertext decoded")
	}
	if _, err := right.Decode(raw[:3]); err == nil {
		t.Fatal("truncated ciphertext decoded")
	}
	if _, err := middleware.Encrypt([]byte("short")); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("bad key length accepted: %v", err)
	}
}

func TestUnique(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	store := middleware.NewMemoryUnique(clk)
	stub := &stubClient{}
	by := func(j workflow.Job) string { return string(j.Payload) }
	c := middleware.WrapClient(stub, middleware.Unique(by, store, time.Minute))
	ctx := context.Background()

	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("a")}); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("duplicate passed: %v", err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"}); err != nil {
		t.Fatalf("job with no key refused: %v", err)
	}
	clk.Advance(time.Minute)
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("a")}); err != nil {
		t.Fatalf("lock outlived its ttl: %v", err)
	}
	if len(stub.jobs) != 3 {
		t.Fatalf("driver got %d jobs, want 3", len(stub.jobs))
	}

	// A refused enqueue releases the lock so a retry passes.
	stub.enqueueErr = errBoom
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("b")}); !errors.Is(err, errBoom) {
		t.Fatal(err)
	}
	stub.enqueueErr = nil
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("b")}); err != nil {
		t.Fatalf("lock kept after a failed enqueue: %v", err)
	}

	bad := middleware.Unique(by, store, 0)(stub.Enqueue)
	if _, err := bad(ctx, workflow.Job{Payload: []byte("z")}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("zero ttl accepted: %v", err)
	}
}

// TestUnique_ConcurrentEnqueuesOnlyOnePasses proves the memory store's
// Acquire is atomic: many goroutines racing on one key let exactly one
// job through.
func TestUnique_ConcurrentEnqueuesOnlyOnePasses(t *testing.T) {
	store := middleware.NewMemoryUnique(clock.NewFake(time.Unix(0, 0)))
	stub := &stubClient{}
	enq := middleware.Unique(func(workflow.Job) string { return "k" }, store, time.Hour)(stub.Enqueue)
	var wg sync.WaitGroup
	var passed atomic.Int32
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := enq(context.Background(), workflow.Job{Workflow: "w"}); err == nil {
				passed.Add(1)
			}
		}()
	}
	wg.Wait()
	if passed.Load() != 1 || len(stub.jobs) != 1 {
		t.Fatalf("passed=%d jobs=%d, want 1", passed.Load(), len(stub.jobs))
	}
}

func TestRoute(t *testing.T) {
	stub := &stubClient{}
	callerLabels := map[string]string{"queue": "explicit"}
	c := middleware.WrapClient(stub, middleware.Route(
		middleware.RouteRule{Match: func(j workflow.Job) bool { return j.Workflow == "mail" }, Workflow: "mail-v2", Labels: map[string]string{"queue": "emails", "tier": "low"}},
		middleware.RouteRule{Labels: map[string]string{"queue": "default"}},
	))
	ctx := context.Background()
	_, _ = c.Enqueue(ctx, workflow.Job{Workflow: "mail"})
	_, _ = c.Enqueue(ctx, workflow.Job{Workflow: "mail", Labels: callerLabels})
	_, _ = c.Enqueue(ctx, workflow.Job{Workflow: "report"})

	if j := stub.jobs[0]; j.Workflow != "mail-v2" || j.Labels["queue"] != "emails" || j.Labels["tier"] != "low" {
		t.Fatalf("first rule not applied: %+v", j)
	}
	if j := stub.jobs[1]; j.Labels["queue"] != "explicit" || j.Labels["tier"] != "low" {
		t.Fatalf("explicit label overridden: %+v", j)
	}
	if len(callerLabels) != 1 {
		t.Fatal("caller's labels map mutated")
	}
	if j := stub.jobs[2]; j.Workflow != "report" || j.Labels["queue"] != "default" {
		t.Fatalf("catch-all rule not applied: %+v", j)
	}
}

func TestDebounce(t *testing.T) {
	start := time.Unix(1000, 0)
	clk := clock.NewFake(start)
	stub := &stubClient{caps: workflow.CapDelayedStart | workflow.CapCancel}
	key := func(j workflow.Job) string { return j.Labels["user"] }
	mw, err := middleware.Debounce(stub, clk, key, 10*time.Second, 25*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(stub, mw)
	ctx := context.Background()
	job := workflow.Job{Workflow: "sync", Labels: map[string]string{"user": "u1"}}

	r1, _ := c.Enqueue(ctx, job)
	if got := stub.jobs[0].RunAt; !got.Equal(start.Add(10 * time.Second)) {
		t.Fatalf("first RunAt = %v", got)
	}
	clk.Advance(5 * time.Second)
	_, _ = c.Enqueue(ctx, job)
	if len(stub.cancelled) != 1 || stub.cancelled[0] != r1 {
		t.Fatalf("previous run not cancelled: %+v", stub.cancelled)
	}
	if got := stub.jobs[1].RunAt; !got.Equal(start.Add(15 * time.Second)) {
		t.Fatalf("window not slid: %v", got)
	}
	// Keep the key busy: maxWait caps the delay at first+25s.
	clk.Advance(9 * time.Second)
	_, _ = c.Enqueue(ctx, job)
	clk.Advance(9 * time.Second)
	_, _ = c.Enqueue(ctx, job)
	if got := stub.jobs[3].RunAt; !got.Equal(start.Add(25 * time.Second)) {
		t.Fatalf("maxWait not applied: %v", got)
	}
	// Past the last run's start, a job opens a new burst and cancels nothing.
	clk.Set(start.Add(25 * time.Second))
	cancels := len(stub.cancelled)
	_, _ = c.Enqueue(ctx, job)
	if len(stub.cancelled) != cancels {
		t.Fatal("cancelled a run that had already started")
	}
	if got := stub.jobs[4].RunAt; !got.Equal(start.Add(35 * time.Second)) {
		t.Fatalf("new burst RunAt = %v", got)
	}
	// Other keys and keyless jobs are independent.
	_, _ = c.Enqueue(ctx, workflow.Job{Workflow: "sync", Labels: map[string]string{"user": "u2"}})
	_, _ = c.Enqueue(ctx, workflow.Job{Workflow: "other"})
	if len(stub.cancelled) != cancels || !stub.jobs[6].RunAt.IsZero() {
		t.Fatal("independent jobs were debounced together")
	}

	// A failed cancel still returns the new run, with the error.
	stub.cancelErr = errBoom
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync", Labels: map[string]string{"user": "u2"}})
	if !errors.Is(err, errBoom) || ref.IsZero() {
		t.Fatalf("ref=%+v err=%v", ref, err)
	}
}

func TestDebounce_RefusesWithoutCapabilities(t *testing.T) {
	key := func(workflow.Job) string { return "k" }
	_, err := middleware.Debounce(&stubClient{caps: workflow.CapDelayedStart}, nil, key, time.Second, 0)
	var ue *workflow.UnsupportedError
	if !errors.Is(err, workflow.ErrUnsupported) || !errors.As(err, &ue) || ue.Missing != workflow.CapCancel {
		t.Fatalf("err = %v", err)
	}
	full := &stubClient{caps: workflow.CapDelayedStart | workflow.CapCancel}
	if _, err := middleware.Debounce(full, nil, key, time.Second, time.Millisecond); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("maxWait below window accepted: %v", err)
	}
}
