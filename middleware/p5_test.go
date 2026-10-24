package middleware_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

// statusClient answers Status from a map; every other call panics through
// the nil embedded Client, which these tests never make.
type statusClient struct {
	workflow.Client
	mu sync.Mutex
	st map[workflow.RunID]workflow.Status
}

func (s *statusClient) Status(_ context.Context, ref workflow.RunRef) (workflow.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.st[ref.RunID]
	if !ok {
		return "", workflow.ErrNotFound
	}
	return st, nil
}

// counting is an EnqueueFunc handing out run-1, run-2, ...
func counting() (workflow.EnqueueFunc, *int) {
	n := 0
	return func(context.Context, workflow.Job) (workflow.RunRef, error) {
		n++
		return workflow.RunRef{RunID: workflow.RunID("run-" + strconv.Itoa(n))}, nil
	}, &n
}

func byPayload(j workflow.Job) string { return string(j.Payload) }

func TestUniqueWith_States(t *testing.T) {
	store := middleware.NewMemoryUnique(clock.NewFake(time.Unix(0, 0)))
	sc := &statusClient{st: map[workflow.RunID]workflow.Status{}}
	mw, err := middleware.UniqueWith(middleware.UniqueConfig{By: byPayload, Store: store, TTL: time.Hour,
		States: []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued}, Client: sc})
	if err != nil {
		t.Fatal(err)
	}
	next, n := counting()
	enq := mw(next)
	ctx := context.Background()
	ref, err := enq(ctx, workflow.Job{Workflow: "w", Payload: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	sc.st[ref.RunID] = workflow.StatusQueued
	if _, err := enq(ctx, workflow.Job{Workflow: "w", Payload: []byte("k")}); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("a queued holder: %v, want ErrDuplicate", err)
	}
	sc.st[ref.RunID] = workflow.StatusRunning // not in States: the key is free for a new job
	if _, err := enq(ctx, workflow.Job{Workflow: "w", Payload: []byte("k")}); err != nil {
		t.Fatalf("a running holder: %v, want the job taken", err)
	}
	if *n != 2 {
		t.Fatalf("driver saw %d enqueues, want 2", *n)
	}
	// The new holder is run-2, unknown to Status: a forgotten holder holds nothing.
	if _, err := enq(ctx, workflow.Job{Workflow: "w", Payload: []byte("k")}); err != nil {
		t.Fatalf("a forgotten holder: %v, want the job taken", err)
	}
}

func TestUniqueWith_ReturnExisting(t *testing.T) {
	store := middleware.NewMemoryUnique(clock.NewFake(time.Unix(0, 0)))
	mw, err := middleware.UniqueWith(middleware.UniqueConfig{By: byPayload, Store: store, TTL: time.Hour, ReturnExisting: true})
	if err != nil {
		t.Fatal(err)
	}
	next, n := counting()
	enq := mw(next)
	first, err := enq(context.Background(), workflow.Job{Workflow: "w", Payload: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, out := workflow.WithEnqueueOutcome(context.Background())
	got, err := enq(ctx, workflow.Job{Workflow: "w", Payload: []byte("k")})
	if err != nil || got != first || !out.Duplicate || *n != 1 {
		t.Fatalf("duplicate = %+v, %v, outcome %+v, driver %d; want the holder, nil, a duplicate, 1", got, err, *out, *n)
	}
}

func TestUniqueWith_RefusesWhatItCannotHonour(t *testing.T) {
	store := middleware.NewMemoryUnique(nil)
	cases := map[string]middleware.UniqueConfig{
		"no by":            {Store: store, TTL: time.Hour},
		"no ttl":           {By: byPayload, Store: store},
		"states no client": {By: byPayload, Store: store, TTL: time.Hour, States: []workflow.Status{workflow.StatusQueued}},
		"bad status":       {By: byPayload, Store: store, TTL: time.Hour, States: []workflow.Status{"nope"}, Client: &statusClient{}},
		"no holders":       {By: byPayload, Store: lockOnlyStore{store}, TTL: time.Hour, ReturnExisting: true},
	}
	for name, cfg := range cases {
		if _, err := middleware.UniqueWith(cfg); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
}

// lockOnlyStore hides MemoryUnique's UniqueHolders.
type lockOnlyStore struct{ s *middleware.MemoryUnique }

func (p lockOnlyStore) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return p.s.Acquire(ctx, key, owner, ttl)
}
func (p lockOnlyStore) Release(ctx context.Context, key, owner string) error {
	return p.s.Release(ctx, key, owner)
}

func TestArgsTagged(t *testing.T) {
	type args struct {
		Shop   string `json:"shop_id" unique:"true"`
		Region string `unique:"true"`
		Cursor string `json:"cursor"`
	}
	by := middleware.UniqueByArgs(middleware.ArgsTagged[args]())
	a := by(workflow.Job{Workflow: "w", Payload: []byte(`{"shop_id":"s1","Region":"eu","cursor":"1"}`)})
	b := by(workflow.Job{Workflow: "w", Payload: []byte(`{"shop_id":"s1","Region":"eu","cursor":"2"}`)})
	c := by(workflow.Job{Workflow: "w", Payload: []byte(`{"shop_id":"s2","Region":"eu","cursor":"1"}`)})
	if a != b {
		t.Error("jobs differing only in an untagged field have different keys")
	}
	if a == c {
		t.Error("jobs differing in a tagged field share a key")
	}
	whole := middleware.UniqueByArgs(middleware.ArgsTagged[int]())
	if whole(workflow.Job{Workflow: "w", Payload: []byte("1")}) == whole(workflow.Job{Workflow: "w", Payload: []byte("2")}) {
		t.Error("a non-struct T must make the whole payload count")
	}
}

func TestMaxPayload(t *testing.T) {
	next, n := counting()
	enq := middleware.MaxPayload(4)(next)
	if _, err := enq(context.Background(), workflow.Job{Workflow: "w", Payload: []byte("12345")}); !errors.Is(err, workflow.ErrPayloadTooLarge) {
		t.Fatalf("over the limit: %v", err)
	}
	if _, err := enq(context.Background(), workflow.Job{Workflow: "w", Payload: []byte("1234")}); err != nil || *n != 1 {
		t.Fatalf("at the limit: %v (driver saw %d)", err, *n)
	}
	if _, err := middleware.MaxPayload(0)(next)(context.Background(), workflow.Job{Workflow: "w"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("limit 0: %v, want ErrInvalid", err)
	}
}

func TestPayloadVersion(t *testing.T) {
	var got workflow.Job
	next := func(_ context.Context, j workflow.Job) (workflow.RunRef, error) {
		got = j
		return workflow.RunRef{RunID: "r"}, nil
	}
	enq := middleware.PayloadVersion(3)(next)
	in := map[string]string{"a": "b"}
	if _, err := enq(context.Background(), workflow.Job{Workflow: "w", Labels: in}); err != nil {
		t.Fatal(err)
	}
	if got.Labels[workflow.LabelPayloadVersion] != "3" || len(in) != 1 {
		t.Fatalf("labels = %v (input %v), want version 3 on a copy", got.Labels, in)
	}
	old := map[string]string{workflow.LabelPayloadVersion: "1"}
	if _, err := enq(context.Background(), workflow.Job{Workflow: "w", Labels: old}); err != nil || got.Labels[workflow.LabelPayloadVersion] != "1" {
		t.Fatalf("an older version must be kept: %v %v", err, got.Labels)
	}
	newer := map[string]string{workflow.LabelPayloadVersion: "4"}
	if _, err := enq(context.Background(), workflow.Job{Workflow: "w", Labels: newer}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a newer version: %v, want ErrInvalid", err)
	}
}

func TestStuck(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	reported := make(chan middleware.StuckEvent, 1)
	release := make(chan struct{})
	h := middleware.Stuck(middleware.StuckConfig{Default: time.Minute, Clock: clk,
		OnStuck: func(_ context.Context, ev middleware.StuckEvent) { reported <- ev }})(
		func(ctx context.Context, in []byte) ([]byte, error) { <-release; return in, nil })
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Workflow: "w", Step: "s"})
	done := make(chan error, 1)
	go func() { _, err := h(ctx, nil); done <- err }()
	if err := clk.BlockUntil(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(59 * time.Second)
	select {
	case <-reported:
		t.Fatal("reported before its threshold")
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(time.Second)
	select {
	case ev := <-reported:
		if ev.Info.Step != "s" || ev.Running != time.Minute {
			t.Errorf("event = %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("a stuck attempt was not reported")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("the attempt itself: %v (Stuck must not touch it)", err)
	}
	if _, err := middleware.Stuck(middleware.StuckConfig{})(noopHandler)(ctx, nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("an empty config: %v, want ErrInvalid", err)
	}
}

func noopHandler(context.Context, []byte) ([]byte, error) { return nil, nil }
