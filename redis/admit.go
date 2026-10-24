package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
)

// Admission. A run of a workflow with a concurrency or rate policy, or of
// a workflow the enqueuing client did not register, waits in
// {base}:pending:<workflow> (StatusQueued) until the dispatcher of a
// client that registered the workflow admits it: that client evaluates the
// run's queue and its concurrency and rate keys in Go (workflow.KeyFunc),
// and admitScript decides every run against the stored bucket members
// ({base}:conc:<workflow>\n<bucket>, the runs holding a slot until they
// end) and rate windows ({base}:rate:<workflow>\n<limit>\n<key>, the
// starts inside the window) atomically, so the limits hold across any
// number of clients. Rate windows slide: no span of Per holds more than
// Limit starts.

// admitAll admits the pending runs of every workflow this client
// registered and returns the earliest moment a run held back by a rate
// limit may start (zero when none waits on a window).
func (c *Client) admitAll(ctx context.Context) time.Time {
	c.mu.Lock()
	regs := make([]*registered, 0, len(c.names))
	for _, n := range c.names {
		if r := c.wfs[n]; r != nil {
			regs = append(regs, r)
		}
	}
	c.mu.Unlock()
	pipe := c.rdb.Pipeline()
	cmds := make([]*goredis.StringSliceCmd, len(regs))
	for i, r := range regs {
		cmds[i] = pipe.ZRange(ctx, c.base+":pending:"+r.wf.Name, 0, admitBatch-1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		if ctx.Err() == nil {
			c.log.Error("redis: read pending runs", "error", classify(err))
		}
		return time.Time{}
	}
	var wake time.Time
	for i, r := range regs {
		ids := cmds[i].Val()
		if len(ids) == 0 {
			continue
		}
		at, err := c.admit(ctx, r, ids)
		if err != nil {
			if ctx.Err() == nil {
				c.log.Error("redis: admit runs", "workflow", r.wf.Name, "error", err)
			}
			continue
		}
		if !at.IsZero() && (wake.IsZero() || at.Before(wake)) {
			wake = at
		}
	}
	return wake
}

// admit evaluates the keys of pending runs ids of reg and runs the
// admission script over them, in their pending order.
func (c *Client) admit(ctx context.Context, reg *registered, ids []string) (time.Time, error) {
	pipe := c.rdb.Pipeline()
	cmds := make([]*goredis.SliceCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HMGet(ctx, c.base+":run:"+id, "payload", "labels", "jq", "prio", "runat")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return time.Time{}, classify(err)
	}
	wf := reg.wf
	maxRuns, strategy := 0, ""
	if wf.Concurrency != nil {
		maxRuns, strategy = wf.Concurrency.MaxRuns, string(wf.Concurrency.Strategy)
		if strategy == "" {
			strategy = string(workflow.ConcurrencyQueueNewest)
		}
	}
	now := c.clk.Now()
	fifo := "0"
	if wf.Concurrency != nil && wf.Concurrency.FIFO {
		fifo = "1"
	}
	args := []any{c.base, wf.Name, now.UnixMilli(), now.UnixNano(), c.retention.Milliseconds(), maxRuns, strategy, len(wf.RateLimits), fifo}
	for _, rl := range wf.RateLimits {
		args = append(args, rl.Limit, rl.Per.Milliseconds())
	}
	for i, id := range ids {
		v := cmds[i].Val()
		if len(v) != 5 || v[0] == nil {
			continue // gone meanwhile; the script drops it from pending
		}
		job := workflow.Job{Workflow: wf.Name, Queue: str(v[2])}
		if p := str(v[0]); p != "" {
			job.Payload = []byte(p)
		}
		if l := str(v[1]); l != "" && l != "{}" {
			if err := json.Unmarshal([]byte(l), &job.Labels); err != nil {
				return time.Time{}, fmt.Errorf("run %s labels: %w", id, err)
			}
		}
		bucket := ""
		if wf.Concurrency != nil {
			bucket = evalKey(wf.Concurrency.Key, job)
		}
		job.RunAt = nanos(str(v[4]))
		args = append(args, id, workflow.EffectiveQueue(wf, job), bucket, runDeadline(wf, job, now))
		for _, rl := range wf.RateLimits {
			args = append(args, evalKey(rl.Key, job))
		}
	}
	ms, err := admitScript.Run(ctx, c.rdb, c.keys(), args...).Int64()
	if err != nil {
		return time.Time{}, classify(err)
	}
	c.pokeNotifier() // a concurrency strategy may have ended runs
	if ms == 0 {
		return time.Time{}, nil
	}
	return time.UnixMilli(ms), nil
}

// evalKey is a concurrency or rate key's value for job: its function's
// result, or "" (one bucket) for a zero key. Start refused expression
// keys.
func evalKey(k workflow.ConcurrencyKey, job workflow.Job) string {
	if k.Func == nil {
		return ""
	}
	return k.Func(job)
}
