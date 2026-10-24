package memory

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/ubgo/workflow"
)

// QueueLoads reports the load of every queue given to WithQueues, sorted
// by name; see workflow.WorkerScaler. Depth counts the attempts ready for
// a worker (a pause holding them still counts them: they are waiting).
// A queue whose Workers is zero (no per-queue cap) reports the pool size
// (WithWorkers), the most it can run at once. Without WithQueues there
// are no queues to balance and the list is empty.
func (c *Client) QueueLoads(ctx context.Context) ([]workflow.QueueLoad, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return nil, err
	}
	now := c.clk.Now()
	loads := make(map[string]*workflow.QueueLoad, len(c.queues))
	for name, q := range c.queues {
		w := q.Workers
		if w == 0 {
			w = c.workers
		}
		loads[name] = &workflow.QueueLoad{Queue: name, Workers: w, Running: c.queueExec[name]}
	}
	for _, t := range c.tasks {
		l, ok := loads[t.r.queue]
		if !ok || t.r.ctx.Err() != nil {
			continue
		}
		l.Depth++
		l.OldestWait = max(l.OldestWait, now.Sub(t.readyAt))
	}
	out := make([]workflow.QueueLoad, 0, len(loads))
	for _, name := range slices.Sorted(maps.Keys(loads)) {
		out = append(out, *loads[name])
	}
	return out, nil
}

// SetWorkers sets the worker cap of a queue given to WithQueues; see
// workflow.WorkerScaler. The cap is within the pool (WithWorkers): a cap
// above the pool size cannot run more than the pool at once, so give the
// pool at least the balancer's budget. Lowering it lets running attempts
// finish; raising it wakes idle workers at once.
func (c *Client) SetWorkers(ctx context.Context, queue string, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n < 1 {
		return fmt.Errorf("%w: queue %q: %d workers; a queue keeps at least one", workflow.ErrInvalid, queue, n)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return err
	}
	q, ok := c.queues[queue]
	if !ok {
		return fmt.Errorf("%w: queue %q is not one of this client's queues (WithQueues)", workflow.ErrNotFound, queue)
	}
	q.Workers = n
	c.queues[queue] = q
	c.taskCond.Broadcast()
	return nil
}
