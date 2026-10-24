package sqlstore

import (
	"context"
	"time"

	"github.com/ubgo/workflow"
)

// WithRecycle makes the client stop itself once a limit in r is reached
// (finished attempts, time since Start on the scheduling clock, memory),
// so a supervisor can replace the process; see workflow.Recycle. The
// client then drains as Drain does: it claims nothing more, lets the
// attempts it holds finish and write their outcomes, then stops, its Done
// channel closes and StopCause returns the *workflow.RecycleError
// (workflow.Serve returns it). Runs it never claimed stay queued in the
// database for the next client. Start refuses an invalid r
// (workflow.ValidateRecycle).
func WithRecycle(r workflow.Recycle) Option {
	return func(cl *Client) { cl.recycle = r }
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

// recycleCheckFloor keeps a misconfigured tiny CheckEvery from spinning.
const recycleCheckFloor = time.Millisecond

// startRecycleLocked arms the time and memory checks at Start, on the
// scheduling clock. Callers hold c.mu, after c.ctx is set.
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
		return // MaxJobs alone: checked after each attempt
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

// recycleAfterAttempt counts an attempt whose outcome was written and
// recycles when a limit is reached.
func (c *Client) recycleAfterAttempt() {
	if c.recycleWatch == nil {
		return
	}
	if hit := c.recycleWatch.JobDone(c.clk.Now()); hit != nil {
		c.recycleNow(hit)
	}
}

// recycleNow records hit as the stop cause, once, stops the workers
// claiming at once and drains the client without a deadline. A client
// already stopping (Drain, the Start ctx, stop-when-idle) keeps a nil
// cause: that stop was asked for.
func (c *Client) recycleNow(hit *workflow.RecycleError) {
	c.mu.Lock()
	first := c.stopCause == nil && !c.stopped && !c.draining.Load()
	if first {
		c.stopCause = hit
		c.draining.Store(true)
	}
	c.mu.Unlock()
	if !first {
		return
	}
	// Drain waits for every goroutine in c.wg, which may include the
	// caller, so it runs on its own goroutine; Done reports the end.
	go func() {
		if err := c.Drain(context.Background()); err != nil {
			c.log.Error("sqlstore: recycle", "error", err)
		}
	}()
}
