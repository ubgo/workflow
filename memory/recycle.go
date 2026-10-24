package memory

import (
	"context"
	"time"

	"github.com/ubgo/workflow"
)

// WithRecycle makes the client stop itself once a limit in r is reached
// (finished attempts, time since Start, memory), so a supervisor can
// replace the process; see workflow.Recycle. The client then drains as
// Drain does, its Done channel closes and StopCause returns the
// *workflow.RecycleError; workflow.Serve returns it. Since nothing in
// memory outlives the client, runs left unfinished become
// StatusCancelled. Start refuses an invalid r (workflow.ValidateRecycle).
func WithRecycle(r workflow.Recycle) Option {
	return func(m *Client) { m.recycle = r }
}

// StopCause returns the *workflow.RecycleError when a WithRecycle limit
// stopped the client, and nil otherwise; see workflow.StopCauser.
func (c *Client) StopCause() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopCause == nil {
		return nil // a typed nil *RecycleError must not become a non-nil error
	}
	return c.stopCause
}

// startRecycleLocked arms the time and memory checks at Start. Callers
// hold c.mu.
func (c *Client) startRecycleLocked() {
	if !c.recycle.Enabled() {
		return
	}
	c.recycleWatch = workflow.NewRecycleWatch(c.recycle, c.clk.Now())
	deadline, hasDeadline := c.recycleWatch.Deadline()
	every := c.recycleWatch.CheckEvery()
	if every > 0 {
		every = max(every, recycleCheckFloor)
	}
	if !hasDeadline && every == 0 {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			wait := every
			if hasDeadline {
				if until := deadline.Sub(c.clk.Now()); wait == 0 || until < wait {
					wait = max(until, 0)
				}
			}
			t := c.clk.NewTimer(wait)
			select {
			case <-t.C():
			case <-c.ctx.Done():
				t.Stop()
				return
			}
			if hit := c.recycleWatch.Check(c.clk.Now()); hit != nil {
				c.recycleNow(hit)
				return
			}
		}
	}()
}

// recycleAfterAttempt counts a finished attempt and recycles when a limit
// is reached.
func (c *Client) recycleAfterAttempt() {
	if c.recycleWatch == nil {
		return
	}
	if hit := c.recycleWatch.JobDone(c.clk.Now()); hit != nil {
		c.recycleNow(hit)
	}
}

// recycleNow records hit as the stop cause, once, and drains the client
// without a deadline: running attempts finish, nothing new is claimed.
func (c *Client) recycleNow(hit *workflow.RecycleError) {
	c.mu.Lock()
	first := c.stopCause == nil && !c.stopped
	if first {
		c.stopCause = hit
	}
	c.mu.Unlock()
	if first {
		go func() { _ = c.Drain(context.Background()) }() // not in wg: Drain waits for wg
	}
}

// recycleCheckFloor keeps a misconfigured tiny CheckEvery from spinning.
const recycleCheckFloor = time.Millisecond
