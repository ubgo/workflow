package hatchet

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// startOffline puts c in the state Start leaves it in, without an engine:
// no worker IDs (so Drain pauses nothing), a stop that closes Done, and
// the recycle watch with its timers. It returns the Start ctx's cancel.
func startOffline(t *testing.T, c *Client) context.CancelFunc {
	t.Helper()
	ctx, cancelStart := context.WithCancel(t.Context())
	runCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.started = true
	c.runCtx = runCtx
	c.stop = cancel
	if c.cfg.Recycle.Enabled() {
		c.watch = workflow.NewRecycleWatch(c.cfg.Recycle, c.cfg.Clock.Now())
	}
	c.mu.Unlock()
	go func() {
		<-runCtx.Done()
		close(c.done)
	}()
	if c.watch != nil {
		go c.watchRecycle(runCtx, c.watch)
	}
	t.Cleanup(cancelStart)
	return cancelStart
}

// waitDone fails unless c's workers stop within a second.
func waitDone(t *testing.T, c *Client) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("client did not stop")
	}
}

// stillRunning fails if c has stopped.
func stillRunning(t *testing.T, c *Client) {
	t.Helper()
	select {
	case <-c.Done():
		t.Fatalf("client stopped early, cause %v", c.StopCause())
	case <-time.After(50 * time.Millisecond):
	}
}

func wantRecycle(t *testing.T, c *Client, reason workflow.RecycleReason) *workflow.RecycleError {
	t.Helper()
	err := c.StopCause()
	var re *workflow.RecycleError
	if !errors.Is(err, workflow.ErrRecycle) || !errors.As(err, &re) || re.Reason != reason {
		t.Fatalf("StopCause = %v, want a %s RecycleError", err, reason)
	}
	return re
}

func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

// TestRecycleMaxJobs pins that the attempt reaching MaxJobs, and no
// earlier one, stops the workers, whatever the attempt's outcome, and
// that afterwards Enqueue-side calls report ErrNotStarted.
func TestRecycleMaxJobs(t *testing.T) {
	c := New(Config{Recycle: workflow.Recycle{MaxJobs: 2}})
	startOffline(t, c)
	h := c.wrap(func(context.Context, []byte) ([]byte, error) { return nil, errors.New("boom") })
	_, _ = h(t.Context(), nil)
	stillRunning(t, c)
	if c.StopCause() != nil {
		t.Fatalf("StopCause while running = %v, want nil", c.StopCause())
	}
	_, _ = h(t.Context(), nil)
	waitDone(t, c)
	if re := wantRecycle(t, c, workflow.RecycleMaxJobs); re.Jobs != 2 {
		t.Fatalf("Jobs = %d, want 2", re.Jobs)
	}
	if _, err := c.ready(); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("ready after recycle = %v, want ErrNotStarted", err)
	}
}

// TestRecycleWaitsForInflight pins the drain: a handler still running when
// another one crosses the limit finishes before Done closes.
func TestRecycleWaitsForInflight(t *testing.T) {
	c := New(Config{Recycle: workflow.Recycle{MaxJobs: 1}})
	startOffline(t, c)
	release, entered := make(chan struct{}), make(chan struct{})
	slow := c.wrap(func(context.Context, []byte) ([]byte, error) {
		close(entered)
		<-release
		return nil, nil
	})
	slowDone := make(chan struct{})
	go func() { _, _ = slow(context.Background(), nil); close(slowDone) }()
	<-entered
	_, _ = c.wrap(noop)(t.Context(), nil)
	stillRunning(t, c)
	close(release)
	<-slowDone
	waitDone(t, c)
	wantRecycle(t, c, workflow.RecycleMaxJobs)
}

// TestRecycleMaxTime pins the MaxTime timer on the driver clock.
func TestRecycleMaxTime(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC))
	c := New(Config{Clock: clk, Recycle: workflow.Recycle{MaxTime: time.Hour}})
	startOffline(t, c)
	clk.Advance(59 * time.Minute)
	stillRunning(t, c)
	clk.Advance(time.Minute)
	waitDone(t, c)
	if re := wantRecycle(t, c, workflow.RecycleMaxTime); re.Elapsed < time.Hour {
		t.Fatalf("Elapsed = %s, want at least 1h", re.Elapsed)
	}
}

// TestRecycleMaxMemory pins the heap check every CheckEvery through the
// injected reader.
func TestRecycleMaxMemory(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC))
	var heap atomic.Uint64
	heap.Store(100)
	read := heap.Load
	c := New(Config{Clock: clk, Recycle: workflow.Recycle{MaxMemory: 1000, Memory: read, CheckEvery: time.Second}})
	startOffline(t, c)
	clk.Advance(time.Second)
	stillRunning(t, c)
	heap.Store(2000)
	// The checker re-arms its timer after each check; keep advancing
	// until it reads the new value.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		clk.Advance(time.Second)
		select {
		case <-c.Done():
			if re := wantRecycle(t, c, workflow.RecycleMaxMemory); re.Memory != 2000 {
				t.Fatalf("Memory = %d, want 2000", re.Memory)
			}
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("client did not stop above MaxMemory")
}

// TestRecycleRequestedStop pins that a requested stop leaves StopCause
// nil, even when a limit is crossed afterwards.
func TestRecycleRequestedStop(t *testing.T) {
	c := New(Config{Recycle: workflow.Recycle{MaxJobs: 1}})
	cancel := startOffline(t, c)
	cancel()
	waitDone(t, c)
	_, _ = c.wrap(noop)(t.Context(), nil)
	if err := c.StopCause(); err != nil {
		t.Fatalf("StopCause after a requested stop = %v, want nil", err)
	}

	d := New(Config{Recycle: workflow.Recycle{MaxJobs: 1}})
	startOffline(t, d)
	if err := d.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, _ = d.wrap(noop)(t.Context(), nil)
	if err := d.StopCause(); err != nil {
		t.Fatalf("StopCause after Drain = %v, want nil", err)
	}
}

// TestRecycleOff pins that the zero Recycle never stops the client.
func TestRecycleOff(t *testing.T) {
	c := New(Config{})
	startOffline(t, c)
	for range 5 {
		_, _ = c.wrap(noop)(t.Context(), nil)
	}
	stillRunning(t, c)
}
