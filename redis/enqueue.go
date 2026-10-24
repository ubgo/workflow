package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
)

// scheduledPrefix starts the ScheduledID of a delayed run; the rest is
// its RunID, so either half of a RunRef finds the run.
const scheduledPrefix = "s-"

// isConnErr reports errors that mean the connection to Redis broke.
func isConnErr(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET)
}

// idemField is the field of the idempotency hash that holds key's run:
// the workflow and the key, separated by a newline (neither may hold one
// that matters: a workflow name cannot).
func idemField(wf, key string) string { return wf + "\n" + key }

// storedRetry is Job.Retry as stored: the data fields of a RetryPolicy.
// ValidateJob refuses NoRetryOn and DelayFor on a job, so nothing is lost.
type storedRetry struct {
	Max       int           `json:"max"`
	Initial   time.Duration `json:"initial,omitempty"`
	Factor    float64       `json:"factor,omitempty"`
	MaxDelay  time.Duration `json:"max_delay,omitempty"`
	Jitter    float64       `json:"jitter,omitempty"`
	Until     time.Duration `json:"until,omitempty"`
	MaxErrors int           `json:"max_errors,omitempty"`

	// DiscardOnExhaust is RetryPolicy.DiscardOnExhaust; absent in older
	// documents, which decode to the default (false).
	DiscardOnExhaust bool `json:"discard_on_exhaust,omitempty"`
}

func encodeRetry(p *workflow.RetryPolicy) (string, error) {
	if p == nil {
		return "", nil
	}
	b, err := json.Marshal(storedRetry{Max: p.Max, Initial: p.Backoff.Initial, Factor: p.Backoff.Factor,
		MaxDelay: p.Backoff.Max, Jitter: p.Backoff.Jitter, Until: p.Until, MaxErrors: p.MaxErrors, DiscardOnExhaust: p.DiscardOnExhaust})
	return string(b), err
}

func decodeRetry(s string) (*workflow.RetryPolicy, error) {
	if s == "" {
		return nil, nil
	}
	var r storedRetry
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		return nil, fmt.Errorf("redis: stored Job.Retry: %w", err)
	}
	return &workflow.RetryPolicy{Max: r.Max, Until: r.Until, MaxErrors: r.MaxErrors, DiscardOnExhaust: r.DiscardOnExhaust,
		Backoff: workflow.Backoff{Initial: r.Initial, Factor: r.Factor, Max: r.MaxDelay, Jitter: r.Jitter}}, nil
}

// createArgs is the createArity values the Lua create helper reads for one
// new run, in its order. A job of a workflow this client registered, with
// no concurrency or rate policy, is admitted at once on its effective
// queue; any other job waits in its workflow's pending set until a worker
// that registered the workflow admits it (its queue and keys come from
// that registration).
func (c *Client) createArgs(id string, job workflow.Job, idem string, delayed bool, now time.Time) ([]any, error) {
	labels := "{}"
	if len(job.Labels) > 0 {
		b, err := json.Marshal(job.Labels)
		if err != nil {
			return nil, fmt.Errorf("redis: encode labels: %w", err)
		}
		labels = string(b)
	}
	retry, err := encodeRetry(job.Retry)
	if err != nil {
		return nil, fmt.Errorf("redis: encode Job.Retry: %w", err)
	}
	chain, err := encodeChain(job.Chain)
	if err != nil {
		return nil, err
	}
	due, sid := int64(0), ""
	if delayed {
		due, sid = job.RunAt.UnixMilli(), scheduledPrefix+id
	}
	queue, adm, rto := "", "0", int64(0)
	c.mu.Lock()
	reg, ok := c.wfs[job.Workflow]
	c.mu.Unlock()
	if ok && !hasPolicy(reg.wf) {
		queue, adm = workflow.EffectiveQueue(reg.wf, job), "1"
		rto = runDeadline(reg.wf, job, now)
	}
	runAt := int64(0)
	if !job.RunAt.IsZero() {
		runAt = job.RunAt.UnixNano()
	}
	return []any{id, job.Workflow, string(job.Payload), labels, job.Priority, idem,
		now.UnixNano(), now.UnixMilli(), due, sid, retry, now.UnixMilli(), queue, job.Queue, adm, chain,
		job.Tenant, runAt, rto}, nil
}

// runDeadline is when a run of wf admitted now as job times out
// (Workflow.RunTimeout from admission, or from RunAt for a delayed run),
// in Unix ms on the scheduling clock; 0 for none.
func runDeadline(wf workflow.Workflow, job workflow.Job, now time.Time) int64 {
	if wf.RunTimeout <= 0 {
		return 0
	}
	start := now
	if job.RunAt.After(now) {
		start = job.RunAt
	}
	return start.Add(wf.RunTimeout).UnixMilli()
}

// hasPolicy reports whether runs of wf must be admitted under a
// concurrency or rate policy before a worker may claim them.
func hasPolicy(wf workflow.Workflow) bool { return wf.Concurrency != nil || len(wf.RateLimits) > 0 }

// idemFieldFor applies the workflow's idempotency policy (when this client
// registered it) to job's key and returns the field of the idempotency
// hash, or "" for no key.
func (c *Client) idemFieldFor(job workflow.Job) (string, error) {
	c.mu.Lock()
	reg, ok := c.wfs[job.Workflow]
	c.mu.Unlock()
	key := job.IdempotencyKey
	if ok {
		switch reg.wf.Idempotency {
		case workflow.IdempotencyOff:
			key = ""
		case workflow.IdempotencyRequired:
			if key == "" {
				return "", fmt.Errorf("%w: workflow %q requires an IdempotencyKey", workflow.ErrInvalid, job.Workflow)
			}
		}
	}
	if key == "" {
		return "", nil
	}
	return idemField(job.Workflow, key), nil
}

// chainArgs is the idempotency field and create values of the next job of
// chain, handed on when the run holding chain succeeds; nil when the chain
// is empty.
func (c *Client) chainArgs(chain []workflow.Job, now time.Time) ([]any, error) {
	next, ok := workflow.NextInChain(chain)
	if !ok {
		return nil, nil
	}
	field, err := c.idemFieldFor(next)
	if err != nil {
		return nil, err
	}
	delayed := !next.RunAt.IsZero() && next.RunAt.After(now)
	args, err := c.createArgs(c.newID(), next, field, delayed, now)
	if err != nil {
		return nil, err
	}
	return append([]any{field}, args...), nil
}

// Enqueue writes a run, ready now or at job.RunAt. With an idempotency key
// held by an unfinished run of the same workflow it writes nothing and
// returns that run's RunRef; the check and the write are one script, so
// two clients racing on one key make one run.
func (c *Client) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJobLabels(ctx, job); err != nil {
		return workflow.RunRef{}, err
	}
	if err := c.usable(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.ValidateJob(job); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJob(caps, job); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckPayload(job, c.PayloadLimit(), "redis"); err != nil {
		return workflow.RunRef{}, err
	}
	c.mu.Lock()
	_, ok := c.wfs[job.Workflow]
	c.mu.Unlock()
	if !ok && !c.enqueueOnly {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %q is not registered", workflow.ErrNotFound, job.Workflow)
	}
	field, err := c.idemFieldFor(job)
	if err != nil {
		return workflow.RunRef{}, err
	}
	now := c.clk.Now()
	delayed := !job.RunAt.IsZero() && job.RunAt.After(now)
	id := c.newID()
	args, err := c.createArgs(id, job, field, delayed, now)
	if err != nil {
		return workflow.RunRef{}, err
	}
	res, err := enqueueScript.Run(ctx, c.rdb, c.keys(), append([]any{c.base, field}, args...)...).Slice()
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("redis: enqueue: %w", classify(err))
	}
	if len(res) == 4 && res[0] == "dup" {
		workflow.MarkDuplicate(ctx)
		return refOf(str(res[1]), workflow.Status(str(res[2])), str(res[3])), nil
	}
	workflow.MarkInserted(ctx)
	c.wakeAll()
	c.pokeDispatcher()
	ref := workflow.RunRef{RunID: workflow.RunID(id)}
	status := workflow.StatusQueued
	if delayed {
		ref, status = workflow.RunRef{ScheduledID: workflow.ScheduledID(scheduledPrefix + id)}, workflow.StatusScheduled
	}
	c.emit(workflow.LifecycleEvent{Kind: workflow.LifecycleInserted, Ref: ref, Workflow: job.Workflow, Queue: workflow.EffectiveQueue(c.wfOf(job.Workflow), job),
		Tenant: job.Tenant, Status: status, Labels: job.Labels, At: now})
	return ref, nil
}

// keys is the KEYS argument of every script: one key in the prefix's slot,
// for cluster routing.
func (c *Client) keys() []string { return []string{c.base + ":runs"} }

// refOf is the RunRef a caller sees for a run: only the ScheduledID while
// a delayed run has not started, both IDs after.
func refOf(id string, st workflow.Status, sid string) workflow.RunRef {
	switch {
	case sid == "":
		return workflow.RunRef{RunID: workflow.RunID(id)}
	case st == workflow.StatusScheduled:
		return workflow.RunRef{ScheduledID: workflow.ScheduledID(sid)}
	}
	return workflow.RunRef{RunID: workflow.RunID(id), ScheduledID: workflow.ScheduledID(sid)}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// runID resolves either half of a RunRef to the run's ID.
func runID(ref workflow.RunRef) (string, error) {
	if !ref.RunID.IsZero() {
		return string(ref.RunID), nil
	}
	if id, ok := cutPrefix(string(ref.ScheduledID), scheduledPrefix); ok && id != "" {
		return id, nil
	}
	return "", fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
}

// Cancel stops an unfinished run: a delayed or queued one never starts, a
// running one has its step's context cancelled on the client that holds
// it (told through a published message). Finished runs are left alone.
func (c *Client) Cancel(ctx context.Context, ref workflow.RunRef) error {
	id, err := runID(ref)
	if err != nil {
		return err
	}
	r, err := c.load(ctx, id, ref)
	if err != nil {
		return err
	}
	res, err := cancelScript.Run(ctx, c.rdb, c.keys(), c.base, id, c.clk.Now().UnixNano(),
		workflow.ErrCancelled.Error(), c.retention.Milliseconds()).Text()
	if err != nil {
		return fmt.Errorf("redis: cancel: %w", classify(err))
	}
	if res == "notfound" {
		return fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	c.pokeNotifier()
	if res == "ok" {
		c.emit(workflow.LifecycleEvent{Kind: workflow.LifecycleFinished, Ref: refOf(id, workflow.StatusCancelled, r.sid), Workflow: r.wf, Queue: r.queue,
			Tenant: r.tenant, Status: workflow.StatusCancelled, Err: workflow.ErrCancelled, Labels: r.labels, At: c.clk.Now()})
	}
	return nil
}

// Status reports where a run is.
func (c *Client) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	id, err := runID(ref)
	if err != nil {
		return "", err
	}
	r, err := c.load(ctx, id, ref)
	if err != nil {
		return "", err
	}
	return r.status, nil
}

// Signal keeps ev and resumes every run waiting on its name and key:
// each such run takes it once, and a run that reaches a matching wait step
// later takes it then, provided the run was enqueued before the signal was
// sent. A signal nobody waits for is kept until no unfinished run may take
// it, and is not an error.
func (c *Client) Signal(ctx context.Context, ev workflow.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.usable(); err != nil {
		return err
	}
	if err := signalScript.Run(ctx, c.rdb, c.keys(), c.base, sigKey(ev.Name, ev.Key), string(ev.Payload), c.clk.Now().UnixMilli()).Err(); err != nil {
		return fmt.Errorf("redis: signal: %w", classify(err))
	}
	return nil
}

// sigKey is the key a signal is kept and waited for under: its name and
// key, separated by a newline.
func sigKey(name, key string) string { return name + "\n" + key }

// stored is one run hash, decoded.
type stored struct {
	id          string
	sid         string
	wf          string
	payload     []byte
	labels      map[string]string
	priority    int
	retry       *workflow.RetryPolicy
	status      workflow.Status
	steps       map[string]*stepRec
	attempts    int
	reclaims    int
	enqueued    time.Time
	started     time.Time
	finished    time.Time
	err         string
	failedStep  string
	errs        []workflow.AttemptError
	queue       string // the run's queue: q once admitted, else Job.Queue or DefaultQueue
	chain       []workflow.Job
	waitVals    map[string][]byte // payloads of signals that resolved waits, by step
	forced      bool
	finishedRaw string          // the stored finished field, compared by Retry
	rto         int64           // RunTimeout deadline, Unix ms on the scheduling clock; 0 for none
	stepCancels map[string]bool // steps an operator withdrew (CancelStep)
	tenant      string
	runAt       time.Time
	output      []byte
	progress    *workflow.Progress
	by          []string // nodes that ran attempts, first attempt first
}

// load reads run id and checks that ref's ScheduledID, when set, is the
// run's, so a forged ScheduledID cannot reach a run enqueued without one.
func (c *Client) load(ctx context.Context, id string, ref workflow.RunRef) (*stored, error) {
	h, err := c.rdb.HGetAll(ctx, c.base+":run:"+id).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: read run: %w", classify(err))
	}
	if len(h) == 0 {
		return nil, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	r, err := decodeRun(id, h)
	if err != nil {
		return nil, err
	}
	if !ref.ScheduledID.IsZero() && string(ref.ScheduledID) != r.sid {
		return nil, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	return r, nil
}

// decodeRun turns a run hash into a stored run.
func decodeRun(id string, h map[string]string) (*stored, error) {
	r := &stored{id: id, sid: h["sid"], wf: h["wf"], payload: []byte(h["payload"]),
		status: workflow.Status(h["status"]), err: h["err"], failedStep: h["fstep"]}
	if !r.status.Valid() {
		return nil, fmt.Errorf("redis: run %s has an unknown status %q", id, h["status"])
	}
	if h["payload"] == "" {
		r.payload = nil
	}
	if s := h["labels"]; s != "" && s != "{}" {
		if err := json.Unmarshal([]byte(s), &r.labels); err != nil {
			return nil, fmt.Errorf("redis: run %s labels: %w", id, err)
		}
	}
	if s := h["steps"]; s != "" {
		if err := json.Unmarshal([]byte(s), &r.steps); err != nil {
			return nil, fmt.Errorf("redis: run %s steps: %w", id, err)
		}
	}
	if r.steps == nil {
		r.steps = map[string]*stepRec{}
	}
	if s := h["errs"]; s != "" {
		if err := json.Unmarshal([]byte(s), &r.errs); err != nil {
			return nil, fmt.Errorf("redis: run %s error history: %w", id, err)
		}
	}
	var err error
	if r.retry, err = decodeRetry(h["retry"]); err != nil {
		return nil, err
	}
	if r.chain, err = decodeChain(h["chain"]); err != nil {
		return nil, fmt.Errorf("redis: run %s chain: %w", id, err)
	}
	r.queue = h["q"]
	if r.queue == "" {
		r.queue = h["jq"]
	}
	if r.queue == "" {
		r.queue = workflow.DefaultQueue
	}
	r.forced = h["forced"] == "1"
	r.finishedRaw = h["finished"]
	r.tenant = h["tn"]
	r.rto, _ = strconv.ParseInt(h["rto"], 10, 64)
	for k := range h {
		if step, ok := cutPrefix(k, "xs:"); ok {
			if r.stepCancels == nil {
				r.stepCancels = map[string]bool{}
			}
			r.stepCancels[step] = true
		}
	}
	r.runAt = nanos(h["runat"])
	if v, ok := h["out"]; ok && v != "" {
		r.output = []byte(v)
	}
	if v := h["prog"]; v != "" {
		var p workflow.Progress
		if err := json.Unmarshal([]byte(v), &p); err != nil {
			return nil, fmt.Errorf("redis: run %s progress: %w", id, err)
		}
		r.progress = &p
	}
	if v := h["by"]; v != "" {
		if err := json.Unmarshal([]byte(v), &r.by); err != nil {
			return nil, fmt.Errorf("redis: run %s nodes: %w", id, err)
		}
	}
	for k, v := range h {
		if step, ok := cutPrefix(k, "wv:"); ok {
			if r.waitVals == nil {
				r.waitVals = map[string][]byte{}
			}
			r.waitVals[step] = []byte(v)
		}
	}
	r.priority, _ = strconv.Atoi(h["prio"])
	r.attempts, _ = strconv.Atoi(h["attempts"])
	r.reclaims, _ = strconv.Atoi(h["reclaims"])
	r.enqueued = nanos(h["enq"])
	r.started = nanos(h["started"])
	r.finished = nanos(h["finished"])
	return r, nil
}

// nanos parses a stored Unix-nanosecond time; empty or invalid is zero.
func nanos(s string) time.Time {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// info is the RunInfo of a stored run.
func (r *stored) info() workflow.RunInfo {
	info := workflow.RunInfo{
		Ref:         refOf(r.id, r.status, r.sid),
		Workflow:    r.wf,
		Queue:       r.queue,
		Status:      r.status,
		Error:       r.err,
		FailedStep:  r.failedStep,
		Labels:      r.labels,
		Payload:     r.payload,
		Priority:    r.priority,
		EnqueuedAt:  r.enqueued,
		StartedAt:   r.started,
		FinishedAt:  r.finished,
		Attempts:    r.attempts,
		Errors:      r.errs,
		Tenant:      r.tenant,
		RunAt:       r.runAt,
		Progress:    r.progress,
		AttemptedBy: r.by,
	}
	if r.status == workflow.StatusSucceeded {
		info.Output = r.output
	}
	if r.status == workflow.StatusFailed {
		info.Error = r.runError().Error()
	}
	if r.status == workflow.StatusRetrying {
		// The step in backoff that is ready first gives the next attempt.
		var at int64
		for _, s := range r.steps {
			if !s.Done && !s.Z && s.N > 0 && s.Due > 0 && (at == 0 || s.Due < at) {
				at = s.Due
				info.NextAttempt, info.NextAttemptAt, info.Error = s.N+1, time.Unix(0, s.Due).UTC(), s.Err
			}
		}
	}
	return info
}

// runsBatch is how many index entries Runs reads per round trip.
const runsBatch = 200

// Runs lists runs newest enqueue first (ties by RunID, descending), across
// every client of the prefix. It walks the enqueue-time index and filters
// in Go with RunQuery.Match, so a narrow filter over a long history reads
// more than it returns; finished runs leave the index when Delete or
// retention removes them.
func (c *Client) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	if err := workflow.ValidateRunQuery(q); err != nil {
		return workflow.RunPage{}, err
	}
	if err := workflow.CheckRunQuery(caps, q); err != nil {
		return workflow.RunPage{}, err
	}
	limit := q.PageLimit()
	maxScore, afterID := "+inf", ""
	if q.Cursor != "" {
		ms, id, ok := strings.Cut(q.Cursor, ":")
		if _, err := strconv.ParseInt(ms, 10, 64); !ok || err != nil || id == "" {
			return workflow.RunPage{}, fmt.Errorf("%w: run query: cursor %q was not issued by this driver", workflow.ErrInvalid, q.Cursor)
		}
		maxScore, afterID = ms, id
	}
	var page workflow.RunPage
	var lastScore float64
	var lastID string
	for offset := int64(0); ; offset += runsBatch {
		zs, err := c.rdb.ZRevRangeByScoreWithScores(ctx, c.base+":runs", &goredis.ZRangeBy{
			Max: maxScore, Min: "-inf", Offset: offset, Count: runsBatch}).Result()
		if err != nil {
			return workflow.RunPage{}, fmt.Errorf("redis: list runs: %w", classify(err))
		}
		if len(zs) == 0 {
			return page, nil
		}
		pipe := c.rdb.Pipeline()
		cmds := make([]*goredis.MapStringStringCmd, len(zs))
		for i, z := range zs {
			cmds[i] = pipe.HGetAll(ctx, c.base+":run:"+str(z.Member))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return workflow.RunPage{}, fmt.Errorf("redis: read runs: %w", classify(err))
		}
		for i, z := range zs {
			id := str(z.Member)
			if afterID != "" && strconv.FormatInt(int64(z.Score), 10) == maxScore && id >= afterID {
				continue // on or before the cursor
			}
			h := cmds[i].Val()
			if len(h) == 0 {
				c.rdb.ZRem(ctx, c.base+":runs", id) // expired by retention
				continue
			}
			r, err := decodeRun(id, h)
			if err != nil {
				return workflow.RunPage{}, err
			}
			info := r.info()
			if !q.Match(info) {
				continue
			}
			if len(page.Runs) == limit {
				page.Next = strconv.FormatInt(int64(lastScore), 10) + ":" + lastID
				return page, nil
			}
			page.Runs = append(page.Runs, info.Clone())
			lastScore, lastID = z.Score, id
		}
	}
}

// Delete forgets a terminal run: Status then returns ErrNotFound and Runs
// no longer lists it.
func (c *Client) Delete(ctx context.Context, ref workflow.RunRef) error {
	id, err := runID(ref)
	if err != nil {
		return err
	}
	if _, err := c.load(ctx, id, ref); err != nil {
		return err
	}
	res, err := deleteScript.Run(ctx, c.rdb, c.keys(), c.base, id).Text()
	if err != nil {
		return fmt.Errorf("redis: delete: %w", classify(err))
	}
	switch res {
	case "ok":
		return nil
	case "notfound":
		return fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	return fmt.Errorf("%w: run %+v is %s, not terminal", workflow.ErrWrongState, ref, res)
}

// ForceComplete ends an unfinished or failed run as succeeded, from
// outside: steps still executing are cancelled with cause
// workflow.ErrForced and their results ignored, and the idempotency key
// is released. A succeeded or cancelled run is ErrWrongState.
func (c *Client) ForceComplete(ctx context.Context, ref workflow.RunRef) error {
	return c.force(ctx, ref, "complete", "")
}

// ForceFail ends an unfinished run as failed with
// workflow.ForcedFailure(reason) as its error: no retry, and OnFailure
// does not run. A finished run is ErrWrongState.
func (c *Client) ForceFail(ctx context.Context, ref workflow.RunRef, reason string) error {
	return c.force(ctx, ref, "fail", workflow.ForcedFailure(reason).Error())
}

func (c *Client) force(ctx context.Context, ref workflow.RunRef, mode, msg string) error {
	id, err := runID(ref)
	if err != nil {
		return err
	}
	r, err := c.load(ctx, id, ref)
	if err != nil {
		return err
	}
	args := []any{c.base, id, mode, c.clk.Now().UnixNano(), msg, c.retention.Milliseconds()}
	if mode == "complete" {
		next, err := c.chainArgs(r.chain, c.clk.Now())
		if err != nil {
			return err
		}
		args = append(args, next...)
	}
	res, err := forceScript.Run(ctx, c.rdb, c.keys(), args...).Text()
	if err != nil {
		return fmt.Errorf("redis: force %s: %w", mode, classify(err))
	}
	switch res {
	case "ok":
		c.pokeNotifier()
		c.wakeAll()
		return nil
	case "notfound":
		return fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	return fmt.Errorf("%w: run %+v is %s", workflow.ErrWrongState, ref, res)
}

var (
	_ workflow.RunForcer  = (*Client)(nil)
	_ workflow.Client     = (*Client)(nil)
	_ workflow.RunQuerier = (*Client)(nil)
	_ workflow.RunDeleter = (*Client)(nil)
	_ workflow.Drainer    = (*Client)(nil)
)
