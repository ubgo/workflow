package memory

import (
	"fmt"
	"maps"

	"github.com/ubgo/workflow"
)

// errShutdown is the Err of runs cancelled because the client shut down.
var errShutdown = fmt.Errorf("%w: client shut down", workflow.ErrCancelled)

// OnRunFinished registers fn to observe every run end (see
// workflow.RunNotifier). Observers run on one notifier goroutine, in
// registration order and in the order runs ended, never on the goroutine
// that enqueued or cancelled the run, and never while the client's lock is
// held, so an observer may call back into the client.
func (c *Client) OnRunFinished(fn workflow.RunFinishedFunc) error {
	if fn == nil {
		return fmt.Errorf("%w: OnRunFinished with a nil func", workflow.ErrInvalid)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	c.observers = append(c.observers, fn)
	return nil
}

// OnTick registers fn to observe every schedule tick (see
// workflow.TickNotifier). Tick observers share the run-finished
// observers' goroutine and guarantees: in registration order, in the
// order ticks happened, never while the client's lock is held.
func (c *Client) OnTick(fn workflow.TickObserver) error {
	if fn == nil {
		return fmt.Errorf("%w: OnTick with a nil func", workflow.ErrInvalid)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	c.tickObserver = append(c.tickObserver, fn)
	return nil
}

// notice is one item for the notifier goroutine: a run end or a tick.
type notice struct {
	run  *workflow.RunEvent
	tick *workflow.Tick
	life *workflow.LifecycleEvent
}

// queueTickLocked queues t for the tick observers. Callers hold c.mu.
func (c *Client) queueTickLocked(t workflow.Tick) {
	if len(c.tickObserver) == 0 {
		return
	}
	t.Labels = maps.Clone(t.Labels)
	c.ended = append(c.ended, notice{tick: &t})
	c.pokeNotifier()
}

// cancelLocked records why r is cancelled, then finishes it.
func (c *Client) cancelLocked(r *run, why error) {
	if !r.status.IsTerminal() {
		r.err = why
	}
	c.finishLocked(r, workflow.StatusCancelled)
}

// queueEndedLocked queues r's end for the observers. finishLocked calls it
// after setting the terminal status, so an observer calling Status sees
// that status. Callers hold c.mu.
func (c *Client) queueEndedLocked(r *run) {
	c.emitLocked(r, workflow.LifecycleFinished, r.failedStep, 0, r.err)
	if len(c.observers) == 0 {
		return
	}
	c.ended = append(c.ended, notice{run: &workflow.RunEvent{
		Ref:        r.ref(),
		Workflow:   r.reg.wf.Name,
		Status:     r.status,
		Err:        r.err,
		Labels:     maps.Clone(r.job.Labels),
		FinishedAt: c.clk.Now(),
		Step:       r.failedStep,
		Tenant:     r.job.Tenant,
	}})
	c.pokeNotifier()
}

func (c *Client) pokeNotifier() {
	select {
	case c.endedPoke <- struct{}{}:
	default: // already poked
	}
}

// notifier delivers queued run ends until the client has stopped and the
// queue is empty. It drains after shutdown too, so the runs shutdown
// cancels are delivered (with the Start ctx already cancelled); Stop waits
// for that.
func (c *Client) notifier() {
	defer c.wg.Done()
	for {
		c.mu.Lock()
		batch := c.ended
		c.ended = nil
		stopped := c.stopped
		c.mu.Unlock()
		if len(batch) == 0 {
			if stopped {
				return
			}
			<-c.endedPoke
			continue
		}
		for _, n := range batch {
			if n.run != nil {
				for _, fn := range c.observers {
					c.observe(fn, *n.run)
				}
				continue
			}
			if n.life != nil {
				for _, fn := range c.lifecycle {
					c.observeLife(fn, *n.life)
				}
				continue
			}
			for _, fn := range c.tickObserver {
				c.observeTick(fn, *n.tick)
			}
		}
	}
}

// observe calls one observer, recovering a panic so it cannot kill the
// notifier or touch the run.
func (c *Client) observe(fn workflow.RunFinishedFunc, ev workflow.RunEvent) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("memory: run-finished observer panicked", "workflow", ev.Workflow, "run", ev.Ref.RunID, "panic", p)
		}
	}()
	fn(c.ctx, ev)
}

// observeTick calls one tick observer, recovering a panic.
func (c *Client) observeTick(fn workflow.TickObserver, t workflow.Tick) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("memory: tick observer panicked", "workflow", t.Workflow, "schedule", t.ScheduleID, "panic", p)
		}
	}()
	fn(c.ctx, t)
}
