package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

// sharedDebounce is a DebounceStore two debouncers share, standing in for
// a store every process of a fleet reaches. err, when set, fails both
// methods.
type sharedDebounce struct {
	inner *middleware.MemoryDebounce
	err   error
	swaps int
}

func (s *sharedDebounce) Get(ctx context.Context, key string) (middleware.DebounceBurst, bool, error) {
	if s.err != nil {
		return middleware.DebounceBurst{}, false, s.err
	}
	return s.inner.Get(ctx, key)
}

func (s *sharedDebounce) Swap(ctx context.Context, key string, b middleware.DebounceBurst) (middleware.DebounceBurst, bool, error) {
	s.swaps++
	if s.err != nil {
		return middleware.DebounceBurst{}, false, s.err
	}
	return s.inner.Swap(ctx, key, b)
}

// TestDebounce_SharedStoreAcrossProcesses: two debouncers (two processes)
// sharing one store debounce a key together: the second process cancels
// the first process's run. With the default store each keeps its own.
func TestDebounce_SharedStoreAcrossProcesses(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	stub := &stubClient{caps: workflow.CapDelayedStart | workflow.CapCancel}
	key := func(j workflow.Job) string { return j.Labels["user"] }
	job := workflow.Job{Workflow: "sync", Labels: map[string]string{"user": "u1"}}
	ctx := context.Background()

	store := &sharedDebounce{inner: middleware.NewMemoryDebounce(clk)}
	mwA, err := middleware.Debounce(stub, clk, key, 10*time.Second, 0, middleware.WithDebounceStore(store))
	if err != nil {
		t.Fatal(err)
	}
	mwB, _ := middleware.Debounce(stub, clk, key, 10*time.Second, 0, middleware.WithDebounceStore(store))
	a, b := middleware.WrapClient(stub, mwA), middleware.WrapClient(stub, mwB)

	r1, _ := a.Enqueue(ctx, job)
	clk.Advance(time.Second)
	if _, err := b.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if len(stub.cancelled) != 1 || stub.cancelled[0] != r1 {
		t.Fatalf("shared store: process B did not cancel A's run: %+v", stub.cancelled)
	}

	// Default: two debouncers keep separate tables and cancel nothing.
	stub2 := &stubClient{caps: workflow.CapDelayedStart | workflow.CapCancel}
	mwC, _ := middleware.Debounce(stub2, clk, key, 10*time.Second, 0)
	mwD, _ := middleware.Debounce(stub2, clk, key, 10*time.Second, 0)
	_, _ = middleware.WrapClient(stub2, mwC).Enqueue(ctx, job)
	_, _ = middleware.WrapClient(stub2, mwD).Enqueue(ctx, job)
	if len(stub2.cancelled) != 0 {
		t.Fatalf("default store shared state between debouncers: %+v", stub2.cancelled)
	}
}

// TestDebounce_StoreErrorFailsClosed: a Get error enqueues nothing; a Swap
// error cancels the new run and reports the error, so it never fires
// beside the pending one.
func TestDebounce_StoreErrorFailsClosed(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	stub := &stubClient{caps: workflow.CapDelayedStart | workflow.CapCancel}
	key := func(workflow.Job) string { return "k" }
	ctx := context.Background()

	getFails := &sharedDebounce{inner: middleware.NewMemoryDebounce(clk), err: errBoom}
	mw, _ := middleware.Debounce(stub, clk, key, time.Second, 0, middleware.WithDebounceStore(getFails))
	if _, err := middleware.WrapClient(stub, mw).Enqueue(ctx, workflow.Job{Workflow: "w"}); !errors.Is(err, errBoom) {
		t.Fatalf("get error: err = %v", err)
	}
	if len(stub.jobs) != 0 {
		t.Fatal("a job was enqueued although the store failed")
	}

	swapFails := &swapFailStore{}
	mw, _ = middleware.Debounce(stub, clk, key, time.Second, 0, middleware.WithDebounceStore(swapFails))
	ref, err := middleware.WrapClient(stub, mw).Enqueue(ctx, workflow.Job{Workflow: "w"})
	if !errors.Is(err, errBoom) || !ref.IsZero() {
		t.Fatalf("swap error: ref=%+v err=%v", ref, err)
	}
	if len(stub.jobs) != 1 || len(stub.cancelled) != 1 || stub.cancelled[0].ScheduledID != "1" {
		t.Fatalf("new run not cancelled after a swap error: jobs=%d cancelled=%+v", len(stub.jobs), stub.cancelled)
	}
}

// swapFailStore finds nothing and fails every Swap.
type swapFailStore struct{}

func (swapFailStore) Get(context.Context, string) (middleware.DebounceBurst, bool, error) {
	return middleware.DebounceBurst{}, false, nil
}

func (swapFailStore) Swap(context.Context, string, middleware.DebounceBurst) (middleware.DebounceBurst, bool, error) {
	return middleware.DebounceBurst{}, false, errBoom
}

// fleetCircuit is a CircuitStore with a scripted answer.
type fleetCircuit struct {
	open     bool
	openErr  error
	recorded []string
	resets   []string
}

func (f *fleetCircuit) IsOpen(context.Context, string) (bool, error) { return f.open, f.openErr }
func (f *fleetCircuit) RecordError(_ context.Context, key string, p middleware.CircuitPolicy) error {
	if p.MaxErrors != 3 || p.Window != time.Minute || p.Cooldown != time.Hour {
		return errors.New("policy not passed through")
	}
	f.recorded = append(f.recorded, key)
	return nil
}
func (f *fleetCircuit) Reset(_ context.Context, key string) error {
	f.resets = append(f.resets, key)
	return nil
}

func TestThrottleErrors_CustomStore(t *testing.T) {
	store := &fleetCircuit{}
	mw, err := middleware.ThrottleErrors(middleware.ThrottleConfig{
		MaxErrors: 3, Window: time.Minute, Cooldown: time.Hour, Store: store,
		Key: func(_ context.Context, in []byte) string { return string(in) },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	calls := 0
	fail := mw(func(context.Context, []byte) ([]byte, error) { calls++; return nil, errBoom })
	ok := mw(func(context.Context, []byte) ([]byte, error) { calls++; return nil, nil })

	// Closed (allow): the step runs and the outcome reaches the store.
	if _, err := fail(ctx, []byte("api")); !errors.Is(err, errBoom) || calls != 1 {
		t.Fatalf("closed circuit: err=%v calls=%d", err, calls)
	}
	if _, err := ok(ctx, []byte("api")); err != nil {
		t.Fatal(err)
	}
	if len(store.recorded) != 1 || len(store.resets) != 1 {
		t.Fatalf("outcomes not recorded: %+v %+v", store.recorded, store.resets)
	}
	// Open (deny), as another worker of the fleet tripped it.
	store.open = true
	if _, err := ok(ctx, []byte("api")); !errors.Is(err, middleware.ErrThrottled) || calls != 2 {
		t.Fatalf("open circuit: err=%v calls=%d", err, calls)
	}
	// A store error fails closed: throttled, step not run.
	store.open, store.openErr = false, errBoom
	if _, err := ok(ctx, []byte("api")); !errors.Is(err, middleware.ErrThrottled) || !errors.Is(err, errBoom) || calls != 2 {
		t.Fatalf("store error: err=%v calls=%d", err, calls)
	}
}

func TestLogging_LevelFunc(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var mu sync.Mutex
	var seen []middleware.StepLogEvent
	level := middleware.WithLogLevel(func(_ context.Context, ev middleware.StepLogEvent) slog.Level {
		mu.Lock()
		seen = append(seen, ev)
		mu.Unlock()
		if ev.Phase == middleware.StepFailed && ev.Info.Attempt < 3 {
			return slog.LevelWarn
		}
		if ev.Phase == middleware.StepFailed {
			return slog.LevelError
		}
		return slog.LevelInfo
	})
	h := middleware.Logging(l, clock.NewFake(time.Unix(0, 0)), level)(func(context.Context, []byte) ([]byte, error) { return nil, errBoom })
	for attempt := 1; attempt <= 3; attempt++ {
		ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "w", Step: "s", Attempt: attempt})
		_, _ = h(ctx, nil)
	}
	out := buf.String()
	if strings.Count(out, `level=WARN msg="workflow step failed"`) != 2 || strings.Count(out, `level=ERROR msg="workflow step failed"`) != 1 {
		t.Fatalf("levels not applied:\n%s", out)
	}
	if strings.Count(out, `level=INFO msg="workflow step started"`) != 3 {
		t.Fatalf("start level not applied:\n%s", out)
	}
	if len(seen) != 6 || !errors.Is(seen[1].Err, errBoom) || !seen[1].HasInfo || seen[0].Err != nil {
		t.Fatalf("events = %+v", seen)
	}

	// Default unchanged: Debug start, Error failure.
	buf.Reset()
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "w", Attempt: 1})
	_, _ = middleware.Logging(l, nil)(func(context.Context, []byte) ([]byte, error) { return nil, errBoom })(ctx, nil)
	if !strings.Contains(buf.String(), `level=DEBUG msg="workflow step started"`) || !strings.Contains(buf.String(), `level=ERROR msg="workflow step failed"`) {
		t.Fatalf("default levels changed:\n%s", buf.String())
	}
}

func TestEncrypt_OldKeysDecryptOnly(t *testing.T) {
	oldCodec, _ := middleware.Encrypt(testKey(1))
	rotated, err := middleware.Encrypt(testKey(2), testKey(1))
	if err != nil {
		t.Fatal(err)
	}
	sealedOld, _ := oldCodec.Encode([]byte("before rotation"))
	if got, err := rotated.Decode(sealedOld); err != nil || string(got) != "before rotation" {
		t.Fatalf("old payload did not open: %q %v", got, err)
	}
	// New payloads are sealed with the new key only.
	sealedNew, _ := rotated.Encode([]byte("after"))
	if _, err := oldCodec.Decode(sealedNew); err == nil {
		t.Fatal("new payload opened with the old key: Encode did not use the new key")
	}
	newOnly, _ := middleware.Encrypt(testKey(2))
	if got, err := newOnly.Decode(sealedNew); err != nil || string(got) != "after" {
		t.Fatalf("new payload: %q %v", got, err)
	}
	// A key in neither set still fails, and so does the end-to-end envelope.
	stranger, _ := middleware.Encrypt(testKey(3))
	sealedStranger, _ := stranger.Encode([]byte("x"))
	if _, err := rotated.Decode(sealedStranger); err == nil {
		t.Fatal("payload under an unknown key opened")
	}
	enc := sealWith(t, oldCodec, []byte(`{"a":1}`))
	step := middleware.DecodeStep(rotated)(func(_ context.Context, in []byte) ([]byte, error) { return nil, nil })
	if _, err := step(context.Background(), enc); err != nil {
		t.Fatalf("envelope sealed under the old key: %v", err)
	}
	if _, err := middleware.Encrypt(testKey(2), []byte("short")); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("bad old key accepted: %v", err)
	}
}

// racedStore plays a store where another process swapped its run in
// between this process's Get and Swap: Get sees no burst, Swap hands back
// the other process's pending run.
type racedStore struct{ other middleware.DebounceBurst }

func (racedStore) Get(context.Context, string) (middleware.DebounceBurst, bool, error) {
	return middleware.DebounceBurst{}, false, nil
}

func (r racedStore) Swap(context.Context, string, middleware.DebounceBurst) (middleware.DebounceBurst, bool, error) {
	return r.other, true, nil
}

// TestDebounce_SwapDecidesWhatIsCancelled: the run Swap returns is the one
// cancelled, even when Get saw none, so a race between two processes
// never leaves both runs pending.
func TestDebounce_SwapDecidesWhatIsCancelled(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	stub := &stubClient{caps: workflow.CapDelayedStart | workflow.CapCancel}
	other := workflow.RunRef{ScheduledID: "other-process"}
	store := racedStore{other: middleware.DebounceBurst{First: clk.Now(), RunAt: clk.Now().Add(5 * time.Second), Ref: other}}
	mw, _ := middleware.Debounce(stub, clk, func(workflow.Job) string { return "k" }, 10*time.Second, 0, middleware.WithDebounceStore(store))
	if _, err := middleware.WrapClient(stub, mw).Enqueue(context.Background(), workflow.Job{Workflow: "w"}); err != nil {
		t.Fatal(err)
	}
	if len(stub.cancelled) != 1 || stub.cancelled[0] != other {
		t.Fatalf("cancelled = %+v, want the other process's run", stub.cancelled)
	}
}
