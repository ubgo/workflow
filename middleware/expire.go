package middleware

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// LabelExpiresAt is the label that carries a run's expiry: the instant
// after which the run must not start, as RFC 3339 with nanoseconds in
// UTC. ExpireAt and ExpireAfter write it; the step middleware Expire
// reads it. It sits under workflow.LabelPrefixInternal, so a driver
// refuses it unless it arrives through ExpireAfter, which allows it.
const LabelExpiresAt = workflow.LabelPrefixInternal + "expires-at"

// ErrExpired matches the error Expire ends a run with: the run started
// after its expiry (LabelExpiresAt), or a step waited longer than
// ExpireMaxWait for a worker.
var ErrExpired = errors.New("workflow/middleware: run expired before it started")

// ExpireAt returns job with its expiry set to t: a run that has not
// started by t is ended by Expire instead of running late (a one-time
// code mailed an hour after it lapsed, a price quote past its validity).
// The labels map is copied, so the caller's map is never changed. A zero
// t removes the expiry.
//
// The label is reserved, so the job must be enqueued through a client
// wrapped with ExpireAfter (with a d of zero when no default expiry is
// wanted), which lets it through; enqueued straight on the driver, it is
// refused with workflow.ErrInvalid. Unique and ClaimCheck refuse every
// reserved label but their own, so a job carrying an expiry cannot pass
// through them; set the expiry with ExpireAfter placed after them in the
// chain instead.
func ExpireAt(job workflow.Job, t time.Time) workflow.Job {
	labels := make(map[string]string, len(job.Labels)+1)
	maps.Copy(labels, job.Labels)
	if t.IsZero() {
		delete(labels, LabelExpiresAt)
	} else {
		labels[LabelExpiresAt] = formatExpiry(t)
	}
	job.Labels = labels
	return job
}

// ExpireAfter is enqueue middleware that gives every job an expiry of d
// after its start time: after now (read from clk, nil means clock.Real)
// for a job that starts at once, after Job.RunAt for a delayed job, so a
// job delayed past d is not born expired. A job that already carries an
// expiry (ExpireAt) keeps it; a d of zero or less sets no default and
// only lets those through. An expiry label that does not parse as RFC
// 3339 is refused with workflow.ErrInvalid.
//
// It allows LabelExpiresAt on the ctx it passes on
// (workflow.AllowReservedLabels), so the driver accepts it. Place it
// after Unique and ClaimCheck in ChainEnqueue or WrapClient: they refuse
// reserved labels they do not own, and would refuse the expiry it
// stamped. The expiry is enforced on the worker by Expire; without Expire
// in the worker's step middleware it is only a label. It runs on every
// driver.
func ExpireAfter(d time.Duration, clk clock.Clock) workflow.EnqueueMiddleware {
	if clk == nil {
		clk = clock.Real{}
	}
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if v, ok := job.Labels[LabelExpiresAt]; ok {
				if _, err := parseExpiry(v); err != nil {
					return workflow.RunRef{}, err
				}
				return next(workflow.AllowReservedLabels(ctx, LabelExpiresAt), job)
			}
			if d <= 0 {
				return next(ctx, job)
			}
			base := clk.Now()
			if job.RunAt.After(base) {
				base = job.RunAt
			}
			job = ExpireAt(job, base.Add(d))
			return next(workflow.AllowReservedLabels(ctx, LabelExpiresAt), job)
		}
	}
}

// ExpireOption configures Expire.
type ExpireOption func(*expireConfig)

type expireConfig struct {
	fail    bool
	maxWait time.Duration
	steps   []string
}

// ExpireFail ends an expired run failed (workflow.Permanent) instead of
// discarded, for work whose lapse an operator must see.
func ExpireFail() ExpireOption { return func(c *expireConfig) { c.fail = true } }

// ExpireMaxWait also expires a step attempt that waited more than d for
// a worker: now minus workflow.StepInfo.ReadyAt (the schedule-to-start
// time of that attempt, retries included). It needs a driver that fills
// ReadyAt (memory, sqlstore and Redis do; Hatchet leaves it zero, and the
// check is then skipped). A d of zero or less turns it off.
func ExpireMaxWait(d time.Duration) ExpireOption { return func(c *expireConfig) { c.maxWait = d } }

// ExpireSteps limits Expire to the named steps. For a workflow of more
// than one step, name its first steps: a run that started in time is then
// never ended between two of its steps because a later one began after
// the expiry. With no names, every step is checked.
func ExpireSteps(steps ...string) ExpireOption {
	return func(c *expireConfig) { c.steps = append(c.steps, steps...) }
}

// Expire is step middleware that ends a run which starts too late,
// instead of running the step:
//
//   - the first attempt of a step that begins after the run's expiry
//     (LabelExpiresAt, set by ExpireAt or ExpireAfter); a retry of a step
//     whose first attempt began in time is left alone, since the run has
//     started;
//   - with ExpireMaxWait(d), any attempt that waited more than d since
//     it became ready.
//
// The run ends with workflow.Discard of an error matching ErrExpired
// (StatusDiscarded with CapRunStates, StatusCancelled with only
// CapDiscard, failed on a driver without CapDiscard), or with
// workflow.Permanent under ExpireFail. Time is read from clk (nil means
// clock.Real), which must be the driver's clock, since ReadyAt is on it.
// A ctx without a workflow.StepInfo passes straight through, as does a
// run without an expiry; an expiry label that does not parse fails the
// attempt permanently with workflow.ErrInvalid. It runs on every driver.
//
// It only acts when a step runs. A run that no worker ever takes (its
// queue unserved) is not seen here; workflow.ExpireWaiting sweeps those.
func Expire(clk clock.Clock, opts ...ExpireOption) workflow.Middleware {
	if clk == nil {
		clk = clock.Real{}
	}
	var cfg expireConfig
	for _, o := range opts {
		o(&cfg)
	}
	end := func(err error) error {
		if cfg.fail {
			return workflow.Permanent(err)
		}
		return workflow.Discard(err)
	}
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := workflow.StepInfoFrom(ctx)
			if !ok || (len(cfg.steps) > 0 && !slices.Contains(cfg.steps, info.Step)) {
				return next(ctx, in)
			}
			now := clk.Now()
			if v, ok := info.Labels[LabelExpiresAt]; ok && info.Attempt <= 1 {
				at, err := parseExpiry(v)
				if err != nil {
					return nil, workflow.Permanent(err)
				}
				if now.After(at) {
					return nil, end(fmt.Errorf("%w: step %q started at %s, after the run's expiry %s", ErrExpired, info.Step, now.UTC().Format(time.RFC3339Nano), v))
				}
			}
			if cfg.maxWait > 0 && !info.ReadyAt.IsZero() {
				if waited := now.Sub(info.ReadyAt); waited > cfg.maxWait {
					return nil, end(fmt.Errorf("%w: step %q waited %s for a worker, more than %s", ErrExpired, info.Step, waited, cfg.maxWait))
				}
			}
			return next(ctx, in)
		}
	}
}

func formatExpiry(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func parseExpiry(v string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: label %s=%q is not an RFC 3339 time", workflow.ErrInvalid, LabelExpiresAt, v)
	}
	return t, nil
}
