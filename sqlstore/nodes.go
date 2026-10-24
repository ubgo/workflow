package sqlstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/ubgo/workflow"
)

// WithNodeID names this client in the cluster (StepInfo.Node, the error
// history, RunInfo.AttemptedBy, Nodes). The default is
// workflow.DefaultNodeID(), "host:pid". Empty is ignored.
func WithNodeID(id string) Option {
	return func(cl *Client) {
		if id != "" {
			cl.nodeID = id
		}
	}
}

// WithSchedulerOnly makes a client that fires schedules and moves
// time-driven work forward (admissions, delayed starts, sleeps, waits) but
// runs no step (workflow.NodeScheduler): a small scheduler process beside
// worker processes started WithoutScheduler. It overrides WithWorkers.
func WithSchedulerOnly() Option { return func(cl *Client) { cl.schedulerOnly = true } }

// WithoutScheduler makes a client that runs steps but never fires cron
// ticks (workflow.NodeWorker); some other client of the tables must fire
// them (WithSchedulerOnly, or a full client).
func WithoutScheduler() Option { return func(cl *Client) { cl.noScheduler = true } }

// WithPayloadLimit refuses, at Enqueue, EnqueueTx and EnqueueMany, a job
// whose payload (or that of any job in its Chain) is over n bytes, with a
// *workflow.PayloadTooLargeError; the database has no limit of its own
// worth enforcing. Zero or less (the default) sets none.
func WithPayloadLimit(n int) Option {
	return func(cl *Client) { cl.payloadLimit = max(n, 0) }
}

// PayloadLimit is WithPayloadLimit's bound (workflow.PayloadLimiter); 0:
// none.
func (c *Client) PayloadLimit() int { return c.payloadLimit }

// mode is the node mode this client reports.
func (c *Client) mode() workflow.NodeMode {
	switch {
	case c.enqueueOnly:
		return workflow.NodeEnqueueOnly
	case c.schedulerOnly:
		return workflow.NodeScheduler
	case c.noScheduler:
		return workflow.NodeWorker
	}
	return workflow.NodeFull
}

// firesSchedules reports whether this client's dispatcher fires cron
// ticks: not WithoutScheduler, and the leader when an elector is set.
func (c *Client) firesSchedules(leader bool) bool { return !c.noScheduler && leader }

// touchNode writes this client's nodes row when a third of the lease has
// passed since the last write (dispatcher goroutine only).
func (c *Client) touchNode(ctx context.Context) error {
	now := c.leaseNanos()
	if c.nodeSeen != 0 && now-c.nodeSeen < int64(c.lease/3) {
		return nil
	}
	queues := make([]string, 0, len(c.queues))
	for _, q := range c.queues {
		queues = append(queues, q.Name)
	}
	raw, err := json.Marshal(queues)
	if err != nil {
		return err
	}
	c.mu.Lock()
	running := 0
	for _, n := range c.running {
		running += n
	}
	c.mu.Unlock()
	workers := c.workers
	if c.schedulerOnly {
		workers = 0
	}
	n, err := c.st.execN(ctx, c.st.db, "UPDATE {nodes} SET mode = ?, queues = ?, workers = ?, running = ?, started_at = ?, last_seen = ? WHERE id = ?",
		string(c.mode()), string(raw), workers, running, c.startedAt, now, c.nodeID)
	if err == nil && n == 0 {
		_, err = c.st.exec(ctx, c.st.db, "INSERT INTO {nodes} (id, mode, queues, workers, running, started_at, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?)",
			c.nodeID, string(c.mode()), string(raw), workers, running, c.startedAt, now)
		if err != nil && c.st.isUnique(err) {
			err = nil // another client with this id inserted first; the next touch updates it
		}
	}
	if err == nil {
		c.nodeSeen = now
	}
	return err
}

// forgetNode removes this client's nodes row on a clean stop.
func (c *Client) forgetNode() {
	if c.crashed.Load() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	if _, err := c.st.exec(ctx, c.st.db, "DELETE FROM {nodes} WHERE id = ?", c.nodeID); err != nil {
		c.log.Error("sqlstore: remove this node from the registry (it ages out in an hour)", "node", c.nodeID, "error", err)
	}
}

// pruneNodes drops nodes silent for longer than defaultNodesTTL.
func (c *Client) pruneNodes(ctx context.Context) error {
	_, err := c.st.exec(ctx, c.st.db, "DELETE FROM {nodes} WHERE last_seen < ?", c.leaseNanos()-int64(defaultNodesTTL))
	return err
}

// Nodes lists the clients of these tables that reported in within the last
// hour (workflow.NodeLister), by ID. A client reports every third of its
// lease while it runs and leaves the list when it stops cleanly; a crashed
// one stays until it ages out, so read LastSeen for liveness.
func (c *Client) Nodes(ctx context.Context) ([]workflow.NodeInfo, error) {
	var out []workflow.NodeInfo
	var id, mode, queues string
	var workers, running, started, seen int64
	err := c.st.each(ctx, c.st.db, "SELECT id, mode, queues, workers, running, started_at, last_seen FROM {nodes} WHERE last_seen >= ?",
		[]any{c.leaseNanos() - int64(defaultNodesTTL)}, func() error {
			n := workflow.NodeInfo{ID: id, Mode: workflow.NodeMode(mode), Workers: int(workers), Running: int(running),
				StartedAt: unixTime(started), LastSeen: unixTime(seen)}
			if err := json.Unmarshal([]byte(queues), &n.Queues); err != nil {
				c.log.Error("sqlstore: unreadable node queues", "node", id, "error", err)
			}
			if len(n.Queues) == 0 {
				n.Queues = nil
			}
			out = append(out, n)
			return nil
		}, &id, &mode, &queues, &workers, &running, &started, &seen)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: list nodes: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Health reports this client's liveness and readiness
// (workflow.HealthChecker): live unless it has stopped, ready when it has
// started (an enqueue-only client: at once) and the database answers a
// ping within ctx.
func (c *Client) Health(ctx context.Context) workflow.HealthReport {
	c.mu.Lock()
	started, stopped := c.started || c.enqueueOnly, c.stopped
	c.mu.Unlock()
	r := workflow.HealthReport{Live: !stopped}
	r.Checks = append(r.Checks, workflow.HealthCheck{Name: "started", OK: started && !stopped, Detail: string(c.mode())})
	db := workflow.HealthCheck{Name: "database", OK: true, Detail: "ping ok"}
	pctx, cancel := context.WithTimeout(ctx, healthPingTimeout)
	defer cancel()
	if err := c.st.db.PingContext(pctx); err != nil {
		db.OK, db.Detail = false, err.Error()
	}
	r.Checks = append(r.Checks, db)
	r.Ready = r.Live && started && db.OK
	return r
}

// healthPingTimeout bounds Health's database ping when the caller's ctx
// sets no earlier deadline, so a probe never hangs on a dead pool.
const healthPingTimeout = 5 * time.Second
