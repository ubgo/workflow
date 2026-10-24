package redis

import (
	"context"
	"fmt"
	"sort"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
)

// Worker scaling. Each ready set {prefix}:ready:<queue>/<workflow> has a
// companion {prefix}:since:<queue>/<workflow> holding the same runs scored
// by when they became claimable (scheduling clock), kept by the same
// scripts that add and take runs, so QueueLoads reads a queue's depth and
// its oldest wait without scanning runs.

// QueueLoads returns the load of every queue this client was configured
// with (WithQueues), sorted by name: the runs of its workflows ready on
// the queue, how long the oldest has waited, its worker cap here and its
// runs executing here (workflow.WorkerScaler). A client given no queues
// reports none.
func (c *Client) QueueLoads(ctx context.Context) ([]workflow.QueueLoad, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	cfgs := make([]workflow.QueueConfig, 0, len(c.queueCfg))
	for _, q := range c.queueCfg {
		cfgs = append(cfgs, q)
	}
	names := c.names
	c.mu.Unlock()
	sort.Slice(cfgs, func(i, j int) bool { return cfgs[i].Name < cfgs[j].Name })
	pipe := c.rdb.Pipeline()
	type pair struct {
		depth *goredis.IntCmd
		head  *goredis.ZSliceCmd
	}
	cmds := make([][]pair, len(cfgs))
	for i, q := range cfgs {
		for _, wf := range names {
			cmds[i] = append(cmds[i], pair{
				depth: pipe.ZCard(ctx, c.base+":ready:"+q.Name+"/"+wf),
				head:  pipe.ZRangeWithScores(ctx, c.base+":since:"+q.Name+"/"+wf, 0, 0),
			})
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("redis: queue loads: %w", classify(err))
	}
	now := c.clk.Now()
	out := make([]workflow.QueueLoad, len(cfgs))
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, q := range cfgs {
		l := workflow.QueueLoad{Queue: q.Name, Workers: q.Workers, Running: c.heldQ[q.Name]}
		if l.Workers == 0 {
			l.Workers = c.workers
		}
		var oldest int64
		for _, p := range cmds[i] {
			l.Depth += int(p.depth.Val())
			if h := p.head.Val(); len(h) > 0 && (oldest == 0 || int64(h[0].Score) < oldest) {
				oldest = int64(h[0].Score)
			}
		}
		if l.Depth > 0 && oldest > 0 {
			l.OldestWait = max(now.Sub(time.UnixMilli(oldest)), 0)
		}
		out[i] = l
	}
	return out, nil
}

// SetWorkers sets how many runs of queue this client executes at once,
// from its next claim; running runs are not interrupted. n below one
// wraps ErrInvalid; a queue not given to WithQueues wraps ErrNotFound.
func (c *Client) SetWorkers(_ context.Context, queue string, n int) error {
	if err := c.usable(); err != nil {
		return err
	}
	if n < 1 {
		return fmt.Errorf("%w: redis: SetWorkers(%q, %d): a queue needs at least one worker", workflow.ErrInvalid, queue, n)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q, ok := c.queueCfg[queue]
	if !ok {
		return fmt.Errorf("%w: redis: queue %q is not one this client serves (WithQueues)", workflow.ErrNotFound, queue)
	}
	q.Workers = n
	c.queueCfg[queue] = q
	return nil
}

var _ workflow.WorkerScaler = (*Client)(nil)
