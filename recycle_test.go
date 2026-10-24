package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

var recycleT0 = time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)

func TestRecycleWatchMaxJobs(t *testing.T) {
	w := workflow.NewRecycleWatch(workflow.Recycle{MaxJobs: 3}, recycleT0)
	for i := 1; i < 3; i++ {
		if hit := w.JobDone(recycleT0); hit != nil {
			t.Fatalf("job %d: recycled early: %v", i, hit)
		}
	}
	hit := w.JobDone(recycleT0)
	if hit == nil || hit.Reason != workflow.RecycleMaxJobs || hit.Jobs != 3 {
		t.Fatalf("third job = %+v, want max_jobs after 3", hit)
	}
	if again := w.Check(recycleT0.Add(time.Hour)); again != hit {
		t.Fatalf("watch did not latch: %+v", again)
	}
}

func TestRecycleWatchMaxTime(t *testing.T) {
	w := workflow.NewRecycleWatch(workflow.Recycle{MaxTime: time.Minute}, recycleT0)
	if d, ok := w.Deadline(); !ok || !d.Equal(recycleT0.Add(time.Minute)) {
		t.Fatalf("Deadline = %v, %v", d, ok)
	}
	if hit := w.Check(recycleT0.Add(time.Minute - time.Nanosecond)); hit != nil {
		t.Fatalf("recycled before MaxTime: %v", hit)
	}
	if hit := w.Check(recycleT0.Add(time.Minute)); hit == nil || hit.Reason != workflow.RecycleMaxTime {
		t.Fatalf("at MaxTime = %+v, want max_time", hit)
	}
	if _, ok := workflow.NewRecycleWatch(workflow.Recycle{}, recycleT0).Deadline(); ok {
		t.Fatal("Deadline without MaxTime")
	}
}

func TestRecycleWatchMaxMemory(t *testing.T) {
	mem := uint64(100)
	w := workflow.NewRecycleWatch(workflow.Recycle{MaxMemory: 100, Memory: func() uint64 { return mem }}, recycleT0)
	if w.CheckEvery() != workflow.DefaultRecycleCheck {
		t.Fatalf("CheckEvery = %s", w.CheckEvery())
	}
	if hit := w.Check(recycleT0); hit != nil {
		t.Fatalf("recycled at the limit, want only above it: %v", hit)
	}
	mem = 101
	hit := w.JobDone(recycleT0)
	if hit == nil || hit.Reason != workflow.RecycleMaxMemory || hit.Memory != 101 {
		t.Fatalf("above MaxMemory = %+v", hit)
	}
	if !errors.Is(hit, workflow.ErrRecycle) {
		t.Fatal("RecycleError does not match ErrRecycle")
	}
	if workflow.NewRecycleWatch(workflow.Recycle{MaxJobs: 1}, recycleT0).CheckEvery() != 0 {
		t.Fatal("CheckEvery without MaxMemory, want 0")
	}
}

func TestRecycleZeroNeverRecycles(t *testing.T) {
	r := workflow.Recycle{}
	if r.Enabled() {
		t.Fatal("zero Recycle is enabled")
	}
	w := workflow.NewRecycleWatch(r, recycleT0)
	for range 1000 {
		if hit := w.JobDone(recycleT0.Add(1000 * time.Hour)); hit != nil {
			t.Fatalf("zero Recycle recycled: %v", hit)
		}
	}
}

func TestValidateRecycle(t *testing.T) {
	for _, r := range []workflow.Recycle{{MaxJobs: -1}, {MaxTime: -1}, {CheckEvery: -1}} {
		if err := workflow.ValidateRecycle(r); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("ValidateRecycle(%+v) = %v, want ErrInvalid", r, err)
		}
	}
	if err := workflow.ValidateRecycle(workflow.Recycle{MaxJobs: 1, MaxTime: time.Second, MaxMemory: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestHeapObjectBytes(t *testing.T) {
	if workflow.HeapObjectBytes() == 0 {
		t.Fatal("HeapObjectBytes = 0 in a running program")
	}
}

func TestServeReturnsRecycle(t *testing.T) {
	c := memory.New(memory.WithRecycle(workflow.Recycle{MaxJobs: 2}), memory.WithWorkers(1))
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- workflow.Serve(ctx, c) }()
	enqueued := 0
	for enqueued < 2 {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"}); err == nil {
			enqueued++
		} else if !errors.Is(err, workflow.ErrNotStarted) {
			t.Fatal(err)
		}
	}
	select {
	case err := <-errc:
		var re *workflow.RecycleError
		if !errors.As(err, &re) || re.Reason != workflow.RecycleMaxJobs || re.Jobs != 2 {
			t.Fatalf("Serve = %v, want a max_jobs RecycleError after 2", err)
		}
	case <-ctx.Done():
		t.Fatal("Serve did not return after MaxJobs")
	}
}

func TestServeNilOnRequestedStop(t *testing.T) {
	c := memory.New(memory.WithRecycle(workflow.Recycle{MaxJobs: 5}))
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- workflow.Serve(ctx, c) }()
	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Serve after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
	if err := c.StopCause(); err != nil {
		t.Fatalf("StopCause after a requested stop = %v", err)
	}
}

func TestServeStartError(t *testing.T) {
	c := memory.New(memory.WithRecycle(workflow.Recycle{MaxJobs: -1}))
	if err := workflow.Serve(context.Background(), c); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Serve with a negative MaxJobs = %v, want ErrInvalid", err)
	}
}

func TestMemoryRecycleMaxTimeOnFakeClock(t *testing.T) {
	clk := clock.NewFake(recycleT0)
	c := memory.New(memory.WithClock(clk), memory.WithRecycle(workflow.Recycle{MaxTime: time.Hour}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := clk.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(59 * time.Minute)
	select {
	case <-c.Done():
		t.Fatal("stopped before MaxTime")
	case <-time.After(50 * time.Millisecond):
	}
	clk.Advance(time.Minute)
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("not stopped at MaxTime")
	}
	if err := c.StopCause(); !errors.Is(err, workflow.ErrRecycle) {
		t.Fatalf("StopCause = %v, want ErrRecycle", err)
	}
}

func TestMemoryRecycleMaxMemoryOnFakeClock(t *testing.T) {
	clk := clock.NewFake(recycleT0)
	var mem atomic.Uint64
	c := memory.New(memory.WithClock(clk), memory.WithRecycle(workflow.Recycle{MaxMemory: 1 << 20, Memory: mem.Load, CheckEvery: time.Second}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := clk.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	if err := clk.BlockUntil(ctx, 1); err != nil { // re-armed: still under the limit
		t.Fatal(err)
	}
	mem.Store(2 << 20)
	clk.Advance(time.Second)
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("not stopped above MaxMemory")
	}
	var re *workflow.RecycleError
	if err := c.StopCause(); !errors.As(err, &re) || re.Reason != workflow.RecycleMaxMemory {
		t.Fatalf("StopCause = %v, want max_memory", err)
	}
}

func TestRecycleLetsRunningAttemptsFinish(t *testing.T) {
	c := memory.New(memory.WithRecycle(workflow.Recycle{MaxJobs: 1}), memory.WithWorkers(2))
	release := make(chan struct{})
	slowDone := make(chan struct{})
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) {
		if string(in) == "slow" {
			<-release
			close(slowDone)
		}
		return nil, nil
	}}}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	slow, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("slow")})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if st, _ := c.Status(ctx, slow); st == workflow.StatusRunning {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("fast")}); err != nil {
		t.Fatal(err)
	}
	// The fast job reaches MaxJobs; the client drains but must wait for
	// the slow attempt rather than cut it off.
	select {
	case <-c.Done():
		t.Fatal("stopped while an attempt was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-slowDone
	select {
	case <-c.Done():
	case <-ctx.Done():
		t.Fatal("not stopped after the running attempt finished")
	}
	if !errors.Is(c.StopCause(), workflow.ErrRecycle) {
		t.Fatalf("StopCause = %v", c.StopCause())
	}
}

func TestRunQueryLabelPrefixes(t *testing.T) {
	info := workflow.RunInfo{Labels: map[string]string{"order": "eu-12"}}
	for _, tc := range []struct {
		p    map[string]string
		want bool
	}{
		{map[string]string{"order": "eu-"}, true},
		{map[string]string{"order": ""}, true},
		{map[string]string{"order": "us-"}, false},
		{map[string]string{"other": ""}, false},
	} {
		if got := (workflow.RunQuery{LabelPrefixes: tc.p}).Match(info); got != tc.want {
			t.Errorf("Match(%v) = %v, want %v", tc.p, got, tc.want)
		}
	}
	q := workflow.RunQuery{LabelPrefixes: map[string]string{"order": "eu-"}}
	if err := workflow.CheckRunQuery(workflow.CapRunQuery, q); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("CheckRunQuery without CapLabelPrefix = %v", err)
	}
	if err := workflow.CheckRunQuery(workflow.CapLabelPrefix, q); err != nil {
		t.Fatal(err)
	}
	if err := workflow.CheckRunQuery(0, workflow.RunQuery{}); err != nil {
		t.Fatal(err)
	}
	if workflow.CapLabelPrefix.String() != "LabelPrefix" {
		t.Fatalf("name = %s", workflow.CapLabelPrefix)
	}
}
