package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// ErrQueueFull matches the error Bounded refuses an enqueue with when the
// job's queue holds its limit of waiting runs. It is backpressure, a
// verdict on the app's load, not an outage: it deliberately does not
// match workflow.ErrUnavailable, so a failover client never sends the job
// to its secondary backend because the primary is busy.
//
//ds:def id=mw-errqueuefull-6qbdqgbm owner=@khanakia stability=stable
var ErrQueueFull = errors.New("workflow/middleware: queue is full")

// ErrQuotaExceeded matches the error Quota refuses an enqueue with when
// the job's tenant holds its limit of unfinished runs. Like ErrQueueFull
// it is not an outage and does not match workflow.ErrUnavailable.
//
//ds:def id=mw-errquotaexceeded-bppgny9j owner=@khanakia stability=stable
var ErrQuotaExceeded = errors.New("workflow/middleware: tenant quota exceeded")

// MaxDepthScan bounds how many runs DepthFromRuns and TenantCount read
// for one count: ten pages of workflow.MaxRunsLimit. A queue or tenant
// deeper than that is reported as MaxDepthScan, so a limit above it is
// never reached through them; plug in a DepthFunc that asks the store
// directly for deeper bounds.
//
//ds:def id=mw-maxdepthscan-mqa588yu owner=@khanakia stability=stable
const MaxDepthScan = 10 * workflow.MaxRunsLimit

// DepthFunc counts what a job would join, such as the waiting runs of
// its queue. Bounded calls it on every enqueue that has a limit. An error
// fails the enqueue: a bound that cannot be checked is not let through.
type DepthFunc func(ctx context.Context, job workflow.Job) (int, error)

// CountFunc counts what a job counts against, such as its tenant's
// unfinished runs. It has DepthFunc's contract; Quota calls it.
//
//ds:def id=mw-countfunc-mfn4zqn8 owner=@khanakia stability=api
type CountFunc = DepthFunc

// BoundedOption configures Bounded and Quota. The modes are exclusive:
// the last BoundedWait or BoundedDelay given wins; with neither, a full
// queue refuses.
type BoundedOption func(*boundedConfig)

type boundedMode int

const (
	boundedRefuse boundedMode = iota
	boundedWait
	boundedDelay
)

type boundedConfig struct {
	mode  boundedMode
	poll  time.Duration
	delay time.Duration
	clk   clock.Clock
}

// BoundedWait makes a full queue block the enqueue instead of refusing
// it: the count is read again every poll (on clk, nil means clock.Real;
// a poll of zero or less means one second) until it is below the limit,
// or until ctx ends, which returns an error matching both ErrQueueFull
// (or ErrQuotaExceeded) and ctx's error. Use it for producers that should
// slow down rather than drop work, such as a batch import.
//
//ds:def id=mw-boundedwait-tuvbt8nb owner=@khanakia stability=stable
func BoundedWait(poll time.Duration, clk clock.Clock) BoundedOption {
	return func(c *boundedConfig) {
		if poll <= 0 {
			poll = time.Second
		}
		if clk == nil {
			clk = clock.Real{}
		}
		c.mode, c.poll, c.clk = boundedWait, poll, clk
	}
}

// BoundedDelay makes the limit soft: a job that finds its queue full is
// enqueued to start d later (Job.RunAt = now + d, or its own RunAt when
// that is later) instead of being refused, so a burst is spread out
// rather than dropped. Now is read from clk (nil means clock.Real). The
// delayed start needs CapDelayedStart; a driver without it refuses the
// job (workflow.ErrUnsupported), never runs it at once. Delayed runs
// still count as waiting under DepthFromRuns, so a long burst keeps being
// pushed back by d. A d of zero or less is one second.
//
//ds:def id=mw-boundeddelay-qsyzff75 owner=@khanakia stability=stable
func BoundedDelay(d time.Duration, clk clock.Clock) BoundedOption {
	return func(c *boundedConfig) {
		if d <= 0 {
			d = time.Second
		}
		if clk == nil {
			clk = clock.Real{}
		}
		c.mode, c.delay, c.clk = boundedDelay, d, clk
	}
}

// Bounded is enqueue middleware that caps a queue: when depth reports at
// least limit(job) for the job, the enqueue is refused with an error
// matching ErrQueueFull, or waits (BoundedWait), or is delayed
// (BoundedDelay). A limit of zero or less means no bound and the job
// passes without a count. QueueLimits builds limit from a table;
// DepthFromRuns is the stock depth, and any DepthFunc can be plugged.
//
// The check is approximate. Count and insert are two steps, so enqueues
// racing each other, in one process or several, can all see room and
// overshoot the limit by up to the number of racers; a check against a
// count read through RunQuerier also lags what the driver has just
// accepted. Use it as backpressure, not as a hard cap: a cap that must
// hold exactly belongs to the driver (CapQueueLimitGlobal limits running
// runs, Concurrency limits runs per key). It runs on every driver; the
// depth's own needs are its own (DepthFromRuns needs CapRunQuery).
//
//ds:def id=mw-bounded-825vj7pf owner=@khanakia stability=stable
func Bounded(depth DepthFunc, limit func(workflow.Job) int, opts ...BoundedOption) workflow.EnqueueMiddleware {
	return gate("Bounded", ErrQueueFull, depth, limit, opts)
}

// Quota is enqueue middleware that caps each tenant: a job whose
// Job.Tenant already counts maxRuns runs (count, usually TenantCount) is
// refused with an error matching ErrQuotaExceeded, or waits or is
// delayed under the same options as Bounded. A job without a tenant, or
// a maxRuns of zero or less, passes without a count. The count is as
// approximate as Bounded's: concurrent enqueues for one tenant can
// overshoot by the number of racers.
//
//ds:def id=mw-quota-e5mx6ukd owner=@khanakia stability=stable
func Quota(count CountFunc, maxRuns int, opts ...BoundedOption) workflow.EnqueueMiddleware {
	limit := func(job workflow.Job) int {
		if job.Tenant == "" {
			return 0
		}
		return maxRuns
	}
	return gate("Quota", ErrQuotaExceeded, count, limit, opts)
}

// gate is the shared body of Bounded and Quota.
//
//ds:def id=mw-gate-qm9khb2k owner=@khanakia stability=stable
func gate(name string, full error, count DepthFunc, limit func(workflow.Job) int, opts []BoundedOption) workflow.EnqueueMiddleware {
	var cfg boundedConfig
	for _, o := range opts {
		o(&cfg)
	}
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if count == nil || limit == nil {
				return workflow.RunRef{}, fmt.Errorf("%w: %s needs a count and a limit function", workflow.ErrInvalid, name)
			}
			bound := limit(job)
			if bound <= 0 {
				return next(ctx, job)
			}
			for {
				n, err := count(ctx, job)
				if err != nil {
					return workflow.RunRef{}, fmt.Errorf("workflow/middleware: %s count: %w", name, err)
				}
				if n < bound {
					return next(ctx, job)
				}
				switch cfg.mode {
				case boundedDelay:
					at := cfg.clk.Now().Add(cfg.delay)
					if job.RunAt.Before(at) {
						job.RunAt = at
					}
					return next(ctx, job)
				case boundedWait:
					t := cfg.clk.NewTimer(cfg.poll)
					select {
					case <-ctx.Done():
						t.Stop()
						return workflow.RunRef{}, fmt.Errorf("%w: %d of %d for workflow %q, gave up waiting: %w", full, n, bound, job.Workflow, ctx.Err())
					case <-t.C():
					}
				default:
					return workflow.RunRef{}, fmt.Errorf("%w: %d of %d for workflow %q", full, n, bound, job.Workflow)
				}
			}
		}
	}
}

// QueueLimits returns a limit function for Bounded from a table of queue
// name to limit. A job's queue is Job.Queue, or workflow.DefaultQueue
// when it is empty; a queue missing from the table has no bound. The
// middleware cannot see Workflow.Queue, so jobs of a workflow declared on
// its own queue should name it in Job.Queue. The table is copied.
//
//ds:def id=mw-queuelimits-yvqej46h owner=@khanakia stability=stable
func QueueLimits(limits map[string]int) func(workflow.Job) int {
	table := make(map[string]int, len(limits))
	for k, v := range limits {
		table[k] = v
	}
	return func(job workflow.Job) int { return table[jobQueue(job)] }
}

// DepthFromRuns returns the stock DepthFunc: the runs of the job's queue
// (Job.Queue, or workflow.DefaultQueue) that wait to start, StatusQueued
// and StatusScheduled, counted by paging through c's workflow.RunQuerier.
// Each count costs one Runs call per thousand waiting runs, up to
// MaxDepthScan runs (ten calls), so it suits bounds in the hundreds or
// low thousands; a deeper queue reads as MaxDepthScan. It needs
// CapRunQuery and refuses with an UnsupportedError without it (memory,
// sqlstore, Redis and Hatchet have it).
//
//ds:def id=mw-depthfromruns-vmgnha7q owner=@khanakia stability=stable
func DepthFromRuns(c workflow.Client) (DepthFunc, error) {
	rq, err := runQuerier(c, "DepthFromRuns", workflow.Caps{})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, job workflow.Job) (int, error) {
		return countRuns(ctx, rq, workflow.RunQuery{
			Queue:    jobQueue(job),
			Statuses: []workflow.Status{workflow.StatusQueued, workflow.StatusScheduled},
		})
	}, nil
}

// TenantCount returns a CountFunc for Quota: the unfinished runs (every
// status that is not terminal) of the job's tenant, counted through c's
// workflow.RunQuerier with RunQuery.Tenant, up to MaxDepthScan as
// DepthFromRuns. A job without a tenant counts zero. It needs CapRunQuery
// and CapTenant and refuses with an UnsupportedError without them
// (memory, sqlstore, Redis and Hatchet have both).
//
//ds:def id=mw-tenantcount-4dmx9yc6 owner=@khanakia stability=stable
func TenantCount(c workflow.Client) (CountFunc, error) {
	rq, err := runQuerier(c, "TenantCount", workflow.CapTenant)
	if err != nil {
		return nil, err
	}
	open := []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued, workflow.StatusRunning, workflow.StatusWaiting}
	if c.Capabilities().Has(workflow.CapRunStates) {
		// Without CapRunStates a run in retry backoff reads as queued, and
		// such a driver refuses a query naming StatusRetrying.
		open = append(open, workflow.StatusRetrying)
	}
	return func(ctx context.Context, job workflow.Job) (int, error) {
		if job.Tenant == "" {
			return 0, nil
		}
		return countRuns(ctx, rq, workflow.RunQuery{Tenant: job.Tenant, Statuses: open})
	}, nil
}

func runQuerier(c workflow.Client, field string, extra workflow.Caps) (workflow.RunQuerier, error) {
	if missing := c.Capabilities().Missing(workflow.CapRunQuery.With(extra)); !missing.IsZero() {
		return nil, &workflow.UnsupportedError{Field: field, Missing: missing}
	}
	rq, ok := c.(workflow.RunQuerier)
	if !ok {
		return nil, &workflow.UnsupportedError{Field: field, Missing: workflow.CapRunQuery, Reason: "the client does not implement workflow.RunQuerier"}
	}
	return rq, nil
}

// countRuns pages through q, stopping at MaxDepthScan.
func countRuns(ctx context.Context, rq workflow.RunQuerier, q workflow.RunQuery) (int, error) {
	q.Limit = workflow.MaxRunsLimit
	n := 0
	for n < MaxDepthScan {
		page, err := rq.Runs(ctx, q)
		if err != nil {
			return 0, err
		}
		n += len(page.Runs)
		if page.Next == "" {
			break
		}
		q.Cursor = page.Next
	}
	return min(n, MaxDepthScan), nil
}

func jobQueue(job workflow.Job) string {
	if job.Queue != "" {
		return job.Queue
	}
	return workflow.DefaultQueue
}
