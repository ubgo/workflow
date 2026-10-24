package sqlstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/ubgo/workflow"
)

// lifecycle holds the workflow.LifecycleNotifier observers and keeps each
// run's events in order: a run's Started can be seen by a worker before the
// enqueue that wrote it has reported Inserted, so events of a run whose
// insert is still being reported wait for it.
type lifecycle struct {
	mu        sync.Mutex
	observers []workflow.LifecycleFunc
	inserting map[string][]workflow.LifecycleEvent // run id -> events held until Inserted
}

// OnLifecycle registers fn to observe the lifecycle of runs on this client
// (workflow.LifecycleNotifier): Inserted for runs this client enqueued
// (Enqueue and EnqueueMany; not EnqueueTx, whose run exists only once the
// caller commits), Started, Retrying and Snoozed for attempts it ran,
// Progress its steps reported, and Finished for runs whose end it wrote.
// Best-effort and local to the process: another client's events are seen
// there. Observers run on the goroutine that caused the event, in order per
// run; a panic is recovered and logged. It must be called before Start.
func (c *Client) OnLifecycle(fn workflow.LifecycleFunc) error {
	if fn == nil {
		return fmt.Errorf("%w: OnLifecycle needs a function", workflow.ErrInvalid)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	c.lc.mu.Lock()
	defer c.lc.mu.Unlock()
	c.lc.observers = append(c.lc.observers, fn)
	return nil
}

// observing reports whether any lifecycle observer is registered (fixed at
// Start, so read without a lock afterwards would race only before Start).
func (c *Client) observing() bool {
	c.lc.mu.Lock()
	defer c.lc.mu.Unlock()
	return len(c.lc.observers) > 0
}

// holdEvents marks run id as being inserted: its events wait for Inserted.
func (c *Client) holdEvents(id string) {
	c.lc.mu.Lock()
	defer c.lc.mu.Unlock()
	if len(c.lc.observers) == 0 {
		return
	}
	if c.lc.inserting == nil {
		c.lc.inserting = map[string][]workflow.LifecycleEvent{}
	}
	if _, ok := c.lc.inserting[id]; !ok {
		c.lc.inserting[id] = nil
	}
}

// releaseEvents ends the hold on run id after its enqueue: with inserted,
// Inserted is reported first, then whatever waited; without (the enqueue
// failed or was a duplicate), the held events are reported as they are.
func (c *Client) releaseEvents(ctx context.Context, id string, inserted *workflow.LifecycleEvent) {
	c.lc.mu.Lock()
	held, ok := c.lc.inserting[id]
	delete(c.lc.inserting, id)
	obs := c.lc.observers
	c.lc.mu.Unlock()
	if !ok && inserted == nil {
		return
	}
	if inserted != nil {
		c.deliverEvent(ctx, obs, *inserted)
	}
	for _, ev := range held {
		c.deliverEvent(ctx, obs, ev)
	}
}

// emit reports ev to the lifecycle observers, after the run's Inserted
// when its enqueue is still being reported. At and Node are filled here.
func (c *Client) emit(ctx context.Context, ev workflow.LifecycleEvent) {
	c.lc.mu.Lock()
	obs := c.lc.observers
	if len(obs) == 0 {
		c.lc.mu.Unlock()
		return
	}
	if ev.At.IsZero() {
		ev.At = c.clk.Now()
	}
	ev.Node = c.nodeID
	if held, ok := c.lc.inserting[string(ev.Ref.RunID)]; ok {
		c.lc.inserting[string(ev.Ref.RunID)] = append(held, ev)
		c.lc.mu.Unlock()
		return
	}
	c.lc.mu.Unlock()
	c.deliverEvent(ctx, obs, ev)
}

// deliverEvent calls each observer, recovering a panic.
func (c *Client) deliverEvent(ctx context.Context, obs []workflow.LifecycleFunc, ev workflow.LifecycleEvent) {
	for _, fn := range obs {
		func() {
			defer func() {
				if p := recover(); p != nil {
					c.log.Error("sqlstore: lifecycle observer panicked", "run", ev.Ref.RunID, "kind", ev.Kind, "panic", fmt.Sprint(p))
				}
			}()
			fn(ctx, ev)
		}()
	}
}

// insertedEvent is the Inserted event of a run row this client wrote.
func (c *Client) insertedEvent(row *runRow, job workflow.Job, queue string) *workflow.LifecycleEvent {
	st := workflow.StatusQueued
	if row.status == runScheduled {
		st = workflow.StatusScheduled
	}
	return &workflow.LifecycleEvent{Kind: workflow.LifecycleInserted, Ref: row.ref(), Workflow: row.workflow, Queue: queue,
		Tenant: job.Tenant, Status: st, Labels: job.Labels, At: c.clk.Now(), Node: c.nodeID}
}

// ended is a run whose end a transaction wrote, for the Finished event.
type ended struct {
	id, workflow, status, msg, step, kind string
	schedule                              string // the schedule that fired the run, for KeepRuns
}

// emitFinished reports the Finished events of the runs a committed
// transaction ended.
func (c *Client) emitFinished(ctx context.Context, list []ended) {
	for _, e := range list {
		ev := workflow.LifecycleEvent{Kind: workflow.LifecycleFinished, Ref: workflow.RunRef{RunID: workflow.RunID(e.id)},
			Workflow: e.workflow, Status: runStatus(e.status, nil), Step: ""}
		ev.Err = runError(e.id, e.workflow, e.step, e.status, e.msg, e.kind, false, false)
		c.emit(ctx, ev)
	}
}
