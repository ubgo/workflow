package sqlstore

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
)

// QueueLoads reports the load of every queue given to WithQueues on this
// client, sorted by name (workflow.WorkerScaler). Depth and OldestWait
// count the steps of this client's workflows that are ready and due on the
// queue, whichever client enqueued them, since any of them is work this
// client's workers could take; a pause holding them still counts them,
// as they are waiting. OldestWait runs from when a step became claimable
// (its due time) on the scheduling clock. Workers and Running are this
// client's own: its cap (the pool size, WithWorkers, for a queue without
// one) and the attempts executing here. Without WithQueues the list is
// empty.
func (c *Client) QueueLoads(ctx context.Context) ([]workflow.QueueLoad, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.usable(); err != nil {
		return nil, err
	}
	if len(c.queues) == 0 {
		return nil, nil
	}
	now := c.nowNanos()
	type depth struct {
		n      int64
		oldest int64
	}
	depths := map[string]depth{}
	var q string
	var d depth
	err := c.st.each(ctx, c.st.db, "SELECT "+queueOf+", COUNT(*), MIN(st.due) FROM {steps} st WHERE st.workflow IN ("+inList(len(c.names))+
		") AND st.status IN (?, ?) AND st.due <= ? GROUP BY 1", c.nameArgs(stepReady, stepSnoozed, now),
		func() error { depths[q] = d; return nil }, &q, &d.n, &d.oldest)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read queue depths: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]workflow.QueueLoad, 0, len(c.queues))
	for _, qc := range c.queues {
		l := workflow.QueueLoad{Queue: qc.Name, Workers: qc.Workers, Running: c.running[qc.Name]}
		if l.Workers == 0 {
			l.Workers = c.workers
		}
		if dp, ok := depths[qc.Name]; ok {
			l.Depth = int(dp.n)
			l.OldestWait = time.Duration(now - dp.oldest)
		}
		out = append(out, l)
	}
	slices.SortFunc(out, func(a, b workflow.QueueLoad) int { return strings.Compare(a.Queue, b.Queue) })
	return out, nil
}

// SetWorkers sets the worker cap of a queue given to WithQueues on this
// client (workflow.WorkerScaler). The cap is within the pool (WithWorkers):
// a cap above the pool size cannot run more than the pool at once, so give
// the pool at least the balancer's budget. Lowering it lets running
// attempts finish; raising it wakes idle workers at once. The cap is this
// client's alone and is not stored: another client keeps its own, and a
// restart starts from WithQueues again.
func (c *Client) SetWorkers(ctx context.Context, queue string, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n < 1 {
		return fmt.Errorf("%w: queue %q: %d workers; a queue keeps at least one", workflow.ErrInvalid, queue, n)
	}
	if err := c.usable(); err != nil {
		return err
	}
	c.mu.Lock()
	found := false
	for i := range c.queues {
		if c.queues[i].Name == queue {
			c.queues[i].Workers = n
			found = true
		}
	}
	for _, tier := range c.tiers {
		for i := range tier {
			if tier[i].Name == queue {
				tier[i].Workers = n
			}
		}
	}
	c.mu.Unlock()
	if !found {
		return fmt.Errorf("%w: queue %q is not one of this client's queues (WithQueues)", workflow.ErrNotFound, queue)
	}
	c.nudge()
	return nil
}
