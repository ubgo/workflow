package hatchet

import (
	"context"
	"time"

	"github.com/ubgo/workflow"
)

var _ workflow.StopCauser = (*Client)(nil)

// StopCause reports why this client's workers stopped
// (workflow.StopCauser): the *workflow.RecycleError when a Config.Recycle
// limit stopped them, nil while they run and after every requested stop
// (the Start ctx ended, Drain). It is final once Done is closed.
func (c *Client) StopCause() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopCause == nil {
		return nil
	}
	return c.stopCause
}

// jobDone tells the recycle watch that one handler attempt finished, and
// recycles the client when that crossed a limit.
func (c *Client) jobDone() {
	if c.watch == nil {
		return
	}
	if e := c.watch.JobDone(c.cfg.Clock.Now()); e != nil {
		c.recycle(e)
	}
}

// recycle records e as the stop cause and drains the client in the
// background, as Drain does with no deadline: the workers are paused, the
// handlers running here finish, then the workers stop and Done closes.
// It runs in a goroutine because the handler that crossed MaxJobs is
// itself in flight, and Drain waits for it. A stop already requested (the
// Start ctx ended, or Drain was called) wins: the cause stays nil.
func (c *Client) recycle(e *workflow.RecycleError) {
	c.mu.Lock()
	if c.draining || c.stopCause != nil || c.runCtx == nil || c.runCtx.Err() != nil {
		c.mu.Unlock()
		return
	}
	c.stopCause = e
	c.mu.Unlock()
	go func() {
		if err := c.Drain(context.Background()); err != nil {
			c.cfg.Logger.Error("hatchet driver: recycle: drain", "reason", string(e.Reason), "error", err)
		}
	}()
}

// watchRecycle fires the MaxTime and MaxMemory limits, which no finished
// handler reports: a timer at the watch's deadline and a check every
// CheckEvery, both on the driver clock, until ctx ends or a limit hits.
func (c *Client) watchRecycle(ctx context.Context, w *workflow.RecycleWatch) {
	var deadline <-chan time.Time
	if at, ok := w.Deadline(); ok {
		t := c.cfg.Clock.NewTimer(at.Sub(c.cfg.Clock.Now()))
		defer t.Stop()
		deadline = t.C()
	}
	every := w.CheckEvery()
	for {
		var tick <-chan time.Time
		stopTick := func() bool { return false }
		if every > 0 {
			t := c.cfg.Clock.NewTimer(every)
			tick, stopTick = t.C(), t.Stop
		}
		select {
		case <-ctx.Done():
			stopTick()
			return
		case <-deadline:
			deadline = nil // a timer fires once
		case <-tick:
		}
		stopTick()
		if e := w.Check(c.cfg.Clock.Now()); e != nil {
			c.recycle(e)
			return
		}
	}
}
