package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/ubgo/workflow"
)

// Nodes. Every started client reports in to {prefix}:nodes (one field per
// node ID, a JSON workflow.NodeInfo) every third of WithLease on the lease
// clock; Nodes lists those seen within three leases and drops the rest,
// so a crashed node leaves the list after at most three leases. A stopped
// client removes itself.

// maxPayloadBytes is the largest value Redis stores in one string (its
// proto-max-bulk-len default), the driver's hard payload cap.
const maxPayloadBytes = 512 << 20

// WithNodeID sets this client's node identity: StepInfo.Node,
// AttemptError.Node, RunInfo.AttemptedBy and its NodeInfo. The default is
// workflow.DefaultNodeID (host name and process ID).
func WithNodeID(id string) Option {
	return func(c *Client) {
		if id != "" {
			c.node = id
		}
	}
}

// WithPayloadLimit refuses, at Enqueue, a job whose payload (or a chained
// job's) is larger than n bytes, with a *workflow.PayloadTooLargeError.
// The default and the ceiling is the 512 MiB Redis stores in one value.
func WithPayloadLimit(n int) Option {
	return func(c *Client) {
		if n > 0 && n < maxPayloadBytes {
			c.payloadLimit = n
		}
	}
}

// WithSchedulerOnly makes a scheduler node: Start runs no worker, only the
// dispatcher, which fires schedules and admits pending runs for the
// worker nodes (workflow.NodeScheduler).
func WithSchedulerOnly() Option { return func(c *Client) { c.mode = workflow.NodeScheduler } }

// WithoutScheduler makes a worker node: it runs workers and admits
// pending runs but fires no schedule (workflow.NodeWorker), so a separate
// scheduler node (WithSchedulerOnly) fires them. Declared Workflow.Cron
// entries are still written at Start.
func WithoutScheduler() Option { return func(c *Client) { c.mode = workflow.NodeWorker } }

// PayloadLimit is the largest payload Enqueue accepts (workflow.PayloadLimiter).
func (c *Client) PayloadLimit() int {
	if c.payloadLimit > 0 {
		return c.payloadLimit
	}
	return maxPayloadBytes
}

// nodeMode is what this client does.
func (c *Client) nodeMode() workflow.NodeMode {
	switch {
	case c.enqueueOnly:
		return workflow.NodeEnqueueOnly
	case c.mode != "":
		return c.mode
	}
	return workflow.NodeFull
}

// reportIn writes this node's NodeInfo with LastSeen now.
func (c *Client) reportIn(ctx context.Context) {
	c.mu.Lock()
	info := workflow.NodeInfo{ID: c.node, Mode: c.nodeMode(), Workers: c.workers, Running: c.attemptsRunning,
		StartedAt: c.startedAt, LastSeen: c.leaseClk.Now()}
	for _, q := range c.queues {
		info.Queues = append(info.Queues, q.Name)
	}
	c.mu.Unlock()
	if info.Mode == workflow.NodeScheduler {
		info.Workers = 0
	}
	sort.Strings(info.Queues)
	b, err := json.Marshal(info)
	if err != nil { // plain fields always encode
		return
	}
	if err := c.rdb.HSet(ctx, c.base+":nodes", c.node, string(b)).Err(); err != nil && ctx.Err() == nil {
		c.log.Error("redis: report this node in", "error", classify(err))
	}
}

// reporter reports this node in every third of the lease until the client
// stops, then removes it.
func (c *Client) reporter() {
	defer c.wg.Done()
	for {
		c.reportIn(c.ctx)
		t := c.leaseClk.NewTimer(c.lease / 3)
		select {
		case <-c.ctx.Done():
			t.Stop()
			if !c.isCrashed() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(c.ctx), releaseTimeout)
				_ = c.rdb.HDel(ctx, c.base+":nodes", c.node).Err()
				cancel()
			}
			return
		case <-t.C():
		}
	}
}

// Nodes lists the nodes of the prefix seen within three leases, by ID,
// and drops older entries (workflow.NodeLister).
func (c *Client) Nodes(ctx context.Context) ([]workflow.NodeInfo, error) {
	all, err := c.rdb.HGetAll(ctx, c.base+":nodes").Result()
	if err != nil {
		return nil, fmt.Errorf("redis: list nodes: %w", classify(err))
	}
	cutoff := c.leaseClk.Now().Add(-3 * c.lease)
	var out []workflow.NodeInfo
	for id, v := range all {
		var n workflow.NodeInfo
		if err := json.Unmarshal([]byte(v), &n); err != nil || n.LastSeen.Before(cutoff) {
			c.rdb.HDel(ctx, c.base+":nodes", id)
			continue
		}
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Health reports the client live unless it stopped, and ready while it
// is started (an enqueue-only client: always) and Redis answers a PING
// within ctx's deadline (workflow.HealthChecker).
func (c *Client) Health(ctx context.Context) workflow.HealthReport {
	c.mu.Lock()
	started, stopped, draining := c.started, c.stopped, c.draining
	c.mu.Unlock()
	rep := workflow.HealthReport{Live: !stopped}
	run := workflow.HealthCheck{Name: "client", OK: (started || c.enqueueOnly) && !stopped && !draining}
	switch {
	case stopped:
		run.Detail = "stopped"
	case draining:
		run.Detail = "draining"
	case !started && !c.enqueueOnly:
		run.Detail = "not started"
	default:
		run.Detail = string(c.nodeMode())
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, releaseTimeout)
		defer cancel()
	}
	db := workflow.HealthCheck{Name: "redis", OK: true, Detail: "PING ok"}
	if err := c.rdb.Ping(ctx).Err(); err != nil {
		db.OK, db.Detail = false, classify(err).Error()
	}
	rep.Checks = []workflow.HealthCheck{run, db}
	rep.Ready = run.OK && db.OK
	return rep
}

// Result returns a succeeded run's output: its one final step's output,
// or a JSON object of every final step's output (workflow.RunOutput). A
// run that has not succeeded is ErrWrongState (workflow.ResultReader).
func (c *Client) Result(ctx context.Context, ref workflow.RunRef) ([]byte, error) {
	id, err := runID(ref)
	if err != nil {
		return nil, err
	}
	r, err := c.load(ctx, id, ref)
	if err != nil {
		return nil, err
	}
	if r.status != workflow.StatusSucceeded {
		return nil, fmt.Errorf("%w: run %+v is %s, not succeeded", workflow.ErrWrongState, ref, r.status)
	}
	return r.output, nil
}

// Reschedule moves a delayed run that has not started to at (now when at
// is zero or past), keeping its RunRef (workflow.RunRescheduler).
func (c *Client) Reschedule(ctx context.Context, ref workflow.RunRef, at time.Time) (workflow.RunRef, error) {
	id, err := runID(ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	r, err := c.load(ctx, id, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	now := c.clk.Now()
	if at.IsZero() || at.Before(now) {
		at = now
	}
	res, err := rescheduleScript.Run(ctx, c.rdb, c.keys(), c.base, id, at.UnixMilli(), at.UnixNano(), now.UnixMilli()).Text()
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("redis: reschedule: %w", classify(err))
	}
	switch res {
	case "ok":
		c.wakeAll()
		c.pokeDispatcher()
		if at.After(now) {
			return workflow.RunRef{ScheduledID: workflow.ScheduledID(r.sid)}, nil
		}
		return refOf(id, workflow.StatusQueued, r.sid), nil
	case "notfound":
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s, not scheduled", workflow.ErrWrongState, ref, res)
}

// OnLifecycle registers fn to observe the lifecycle of the runs this
// process touches (workflow.LifecycleNotifier): best-effort and local,
// delivered in order on one goroutine; an event that finds the buffer of
// lifecycleBuffer full is dropped and logged.
func (c *Client) OnLifecycle(fn workflow.LifecycleFunc) error {
	if fn == nil {
		return fmt.Errorf("%w: OnLifecycle needs a function", workflow.ErrInvalid)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	c.lifecycle = append(c.lifecycle, fn)
	return nil
}

// lifecycleBuffer is how many lifecycle events wait for the observers
// before new ones are dropped.
const lifecycleBuffer = 4096

// emit queues ev for the lifecycle observers, filling Node.
func (c *Client) emit(ev workflow.LifecycleEvent) {
	c.mu.Lock()
	n := len(c.lifecycle)
	c.mu.Unlock()
	if n == 0 {
		return
	}
	ev.Node = c.node
	select {
	case c.lifeCh <- ev:
	default:
		c.log.Warn("redis: lifecycle event dropped (observers too slow)", "kind", ev.Kind, "run", ev.Ref.RunID)
	}
}

// lifecycleLoop calls the observers for each queued event until the
// client stops.
func (c *Client) lifecycleLoop() {
	defer c.wg.Done()
	c.mu.Lock()
	obs := c.lifecycle
	c.mu.Unlock()
	for {
		select {
		case <-c.ctx.Done():
			return
		case ev := <-c.lifeCh:
			for _, fn := range obs {
				func() {
					defer func() {
						if p := recover(); p != nil {
							c.log.Error("redis: lifecycle observer panicked", "run", ev.Ref.RunID, "panic", fmt.Sprint(p))
						}
					}()
					fn(c.ctx, ev)
				}()
			}
		}
	}
}

// wfOf is the registered workflow named name, or one with only the name.
func (c *Client) wfOf(name string) workflow.Workflow {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r := c.wfs[name]; r != nil {
		return r.wf
	}
	return workflow.Workflow{Name: name}
}

// progressReporter stores a held run's latest progress (fenced by its
// claim token) and emits LifecycleProgress.
type progressReporter struct {
	c       *Client
	h       *held
	step    string
	attempt int
}

func (p progressReporter) ReportProgress(_ context.Context, pr workflow.Progress) error {
	c, h := p.c, p.h
	pr = pr.Clone()
	pr.Step, pr.At = p.step, c.clk.Now()
	b, err := json.Marshal(pr)
	if err != nil {
		return fmt.Errorf("redis: encode progress: %w", err)
	}
	ctx, cancel := c.writeCtx()
	defer cancel()
	ok, err := fieldScript.Run(ctx, c.rdb, c.keys(), c.base, h.id, h.token, "prog", string(b)).Int()
	if err != nil {
		return fmt.Errorf("redis: store progress: %w", classify(err))
	}
	if ok == 0 {
		return fmt.Errorf("%w: the run is no longer held by this worker", workflow.ErrCancelled)
	}
	c.emit(workflow.LifecycleEvent{Kind: workflow.LifecycleProgress, Ref: refOf(h.id, workflow.StatusRunning, h.sid), Workflow: h.wf,
		Queue: h.queue, Tenant: h.tenant, Step: p.step, Attempt: p.attempt, Status: workflow.StatusRunning, Progress: &pr, Labels: h.labels, At: pr.At})
	return nil
}

// noteNode records this node in h's AttemptedBy before its first attempt
// here.
func (c *Client) noteNode(h *held) {
	if slices.Contains(h.by, c.node) {
		return
	}
	h.by = append(h.by, c.node)
	b, err := json.Marshal(h.by)
	if err != nil {
		return
	}
	ctx, cancel := c.writeCtx()
	defer cancel()
	if err := fieldScript.Run(ctx, c.rdb, c.keys(), c.base, h.id, h.token, "by", string(b)).Err(); err != nil {
		c.log.Error("redis: record the node of an attempt", "run", h.id, "error", classify(err))
	}
}

var (
	_ workflow.NodeLister        = (*Client)(nil)
	_ workflow.HealthChecker     = (*Client)(nil)
	_ workflow.ResultReader      = (*Client)(nil)
	_ workflow.RunRescheduler    = (*Client)(nil)
	_ workflow.LifecycleNotifier = (*Client)(nil)
	_ workflow.PayloadLimiter    = (*Client)(nil)
)
