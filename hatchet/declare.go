package hatchet

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
)

// declare builds the Hatchet workflow for wf: workflow options (cron,
// concurrency, idempotency), one task per step in dependency order, and
// the on-failure task. checkSupported has already accepted wf.
func (c *Client) declare(wf workflow.Workflow) (*hatchetsdk.Workflow, error) {
	opts, err := c.workflowOptions(wf)
	if err != nil {
		return nil, err
	}
	hw := c.sdk.NewWorkflow(wf.Name, opts...)
	tasks := make(map[string]*hatchetsdk.Task, len(wf.Steps))
	for _, s := range topoOrder(wf.Steps) {
		topts, err := c.taskOptions(s, wf, tasks)
		if err != nil {
			return nil, err
		}
		if s.Kind() == workflow.StepWait {
			tasks[s.Name] = hw.NewDurableTask(s.Name, c.waitFunc(s, wf), topts...)
			continue
		}
		tasks[s.Name] = hw.NewTask(s.Name, c.taskFunc(s, wf, tasks), topts...)
	}
	if wf.OnFailure != nil {
		hw.OnFailure(c.onFailureFunc(wf, tasks))
	}
	return hw, nil
}

// workflowOptions maps the workflow-level policies.
func (c *Client) workflowOptions(wf workflow.Workflow) ([]hatchetsdk.WorkflowOption, error) {
	var opts []hatchetsdk.WorkflowOption
	if len(wf.Cron) > 0 {
		// A workflow-level cron is part of the workflow version, so a
		// re-register replaces it instead of adding a second trigger.
		opts = append(opts, hatchetsdk.WithWorkflowCron(wf.Cron...))
	}
	if wf.Concurrency != nil {
		conc, err := workflowConcurrency(*wf.Concurrency)
		if err != nil {
			return nil, err
		}
		opts = append(opts, hatchetsdk.WithWorkflowConcurrency(conc))
	}
	if wf.Idempotency != workflow.IdempotencyOff {
		opts = append(opts, hatchetsdk.WithWorkflowIdempotency(hatchetsdk.IdempotencyConfig{
			Expression: idempotencyExpression(engineName(c.cfg.Namespace, wf.Name)),
			// STATUS holds the key while its run is unfinished, the
			// Job.IdempotencyKey contract; TTL only caps a stuck run.
			Method: hatchetsdk.IdempotencyMethodStatus,
			TTL:    c.cfg.IdempotencyMaxHold,
		}))
	}
	return opts, nil
}

// taskOptions maps one step's parents, timeout, retries, rate limits and
// wait or sleep condition.
func (c *Client) taskOptions(s workflow.Step, wf workflow.Workflow, tasks map[string]*hatchetsdk.Task) ([]hatchetsdk.TaskOption, error) {
	var opts []hatchetsdk.TaskOption
	if len(s.After) > 0 {
		parents := make([]*hatchetsdk.Task, len(s.After))
		for i, p := range s.After {
			parents[i] = tasks[p]
		}
		opts = append(opts, hatchetsdk.WithParents(parents...))
	}
	opts = append(opts, hatchetsdk.WithExecutionTimeout(stepTimeout(s, wf, c.cfg.StepTimeout)), hatchetsdk.WithScheduleTimeout(c.cfg.ScheduleTimeout))
	switch s.Kind() {
	case workflow.StepRun:
		policy := workflow.EffectiveRetries(s, wf)
		if policy.Max > 0 {
			opts = append(opts, hatchetsdk.WithRetries(policy.Max))
			b, ok, err := retryBackoff(policy.Backoff)
			if err != nil {
				return nil, err
			}
			if ok {
				opts = append(opts, hatchetsdk.WithRetryBackoff(b.factor, b.maxSeconds))
			}
		}
	case workflow.StepSleep:
		opts = append(opts, hatchetsdk.WithWaitFor(hatchetsdk.SleepCondition(s.Sleep)))
	}
	if len(s.After) == 0 && len(wf.RateLimits) > 0 {
		limits := make([]*hatchetsdk.RateLimit, len(wf.RateLimits))
		for i, r := range wf.RateLimits {
			rl, err := rateLimit(r, engineName(c.cfg.Namespace, wf.Name), i)
			if err != nil {
				return nil, err
			}
			limits[i] = rl
		}
		opts = append(opts, hatchetsdk.WithRateLimits(limits...))
	}
	return opts, nil
}

// taskFunc is the Hatchet task body of step s.
func (c *Client) taskFunc(s workflow.Step, wf workflow.Workflow, tasks map[string]*hatchetsdk.Task) func(hatchetsdk.Context, any) (any, error) {
	switch s.Kind() {
	case workflow.StepSleep:
		return func(hctx hatchetsdk.Context, input any) (any, error) {
			in, err := stepInput(hctx, input, s, tasks)
			if err != nil {
				return nil, finalErr(err)
			}
			return encodeOutput(in), nil
		}
	}
	h := c.wrap(s.Run)
	policy := workflow.EffectiveRetries(s, wf)
	return func(hctx hatchetsdk.Context, input any) (any, error) {
		in, err := stepInput(hctx, input, s, tasks)
		if err != nil {
			return nil, finalErr(err)
		}
		out, err := h(withStepInfo(hctx, wf.Name, s.Name, c.cfg.NodeID), in)
		if err != nil {
			if nonRetryable(err, policy) {
				return nil, finalErr(err)
			}
			return nil, err
		}
		return encodeOutput(out), nil
	}
}

// waitFunc is the durable task body of wait step s. It resolves the key
// for this run, then suspends on the event (or the event or the timeout)
// with the engine holding no worker slot, and hands the event payload on
// as the step's output. Events are considered from the moment the run was
// enqueued, not from when the wait began: Status cannot see when the
// engine has registered the wait, so a Signal sent right after Status
// reports waiting would otherwise be lost. On Hatchet a matching Signal
// that arrives before the run reaches its wait step is therefore not
// dropped but resumes the wait. The key and the deadline are memoized, so a replay
// after a worker restart waits on the same key until the same time.
func (c *Client) waitFunc(s workflow.Step, wf workflow.Workflow) func(hatchetsdk.DurableContext, any) (any, error) {
	wait := *s.Wait
	return func(dctx hatchetsdk.DurableContext, input any) (any, error) {
		key, err := memoString(dctx, "wf_wait_key", func() (string, error) {
			payload, err := decodeEnvelope(input, payloadField)
			if err != nil {
				return "", err
			}
			return wait.ResolveKey(workflow.Job{Workflow: wf.Name, Payload: payload, Labels: jobLabels(dctx.AdditionalMetadata())})
		})
		if err != nil {
			return nil, finalErr(err)
		}
		evKey := eventKey(wait.Event, key)
		// Look-back needs a scope; Signal pushes with the same one.
		evOpts := []hatchetsdk.UserEventConditionOpt{hatchetsdk.WithEventScope(evKey)}
		if since, ok := enqueuedAt(dctx.AdditionalMetadata()); ok {
			evOpts = append(evOpts, hatchetsdk.WithConsiderEventsSince(since))
		}
		cond := hatchetsdk.UserEventCondition(evKey, "", evOpts...)
		limit, err := waitLimit(dctx, wait)
		if err != nil {
			return nil, finalErr(err)
		}
		if limit > 0 {
			cond = hatchetsdk.OrCondition(cond, hatchetsdk.SleepCondition(limit))
		}
		res, err := dctx.WaitFor(cond)
		if err != nil {
			return nil, err
		}
		trigger, ok := eventTrigger(res.Keys())
		if !ok {
			return nil, finalErr(fmt.Errorf("workflow: step %q: wait for %q timed out", s.Name, wait.Event))
		}
		var doc map[string]any
		if err := res.Unmarshal(trigger, &doc); err != nil {
			return nil, finalErr(err)
		}
		payload, err := decodeEnvelope(doc, eventPayloadField)
		if err != nil {
			return nil, finalErr(err)
		}
		return encodeOutput(payload), nil
	}
}

// waitLimit is how long a wait may last from now: its Timeout, or the time
// left to its Deadline (at least a millisecond, so a passed deadline times
// out at once), or zero for no limit. Now is the engine-memoized clock.
func waitLimit(dctx hatchetsdk.DurableContext, w workflow.WaitSpec) (time.Duration, error) {
	switch {
	case w.Timeout > 0:
		return w.Timeout, nil
	case w.Deadline.IsZero():
		return 0, nil
	}
	now, err := dctx.Now()
	if err != nil {
		return 0, err
	}
	return max(w.Deadline.Sub(now), time.Millisecond), nil
}

// memoString runs fn once per durable task and returns the value it gave
// the first time on every replay.
func memoString(dctx hatchetsdk.DurableContext, key string, fn func() (string, error)) (string, error) {
	raw, err := dctx.Memo(key, func() (any, error) { return fn() })
	if err != nil {
		return "", err
	}
	var out string
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("hatchet driver: memo %s: %w", key, err)
	}
	return out, nil
}

// withStepInfo puts the attempt's StepInfo on the context the handler
// receives. Labels are the run's metadata without the driver's and
// Hatchet's own keys.
//
// Queue is the queue Enqueue stamped on the run. ReadyAt stays zero: the
// task context carries no time at which the attempt became claimable (only
// the run's creation, which is wrong for a later step or a retry), and a
// guessed value would make a queue-wait metric lie.
//
// Tenant is the Job.Tenant Enqueue stamped, and Node this client's
// Config.NodeID, the label its workers carry on the engine.
func withStepInfo(hctx hatchetsdk.Context, wfName, step, node string) context.Context {
	md := hctx.AdditionalMetadata()
	return workflow.WithStepInfo(hctx, workflow.StepInfo{
		Workflow: wfName,
		Step:     step,
		RunID:    workflow.RunID(hctx.WorkflowRunId()),
		Attempt:  hctx.RetryCount() + 1,
		Labels:   jobLabels(md),
		Queue:    runQueue(md),
		Tenant:   md[metaTenant],
		Node:     node,
	})
}

// failedStep names a step the on-failure task reports as failed, or ""
// when none is reported.
func failedStep(hctx hatchetsdk.Context) string {
	for name := range hctx.StepRunErrors() {
		return name
	}
	return ""
}

// wrap applies the configured middleware, outermost first, counts the
// handler as in flight while it runs, so Drain can wait for it, and
// reports each finished attempt to the recycle watch (Config.Recycle).
func (c *Client) wrap(h workflow.Handler) workflow.Handler {
	for i := len(c.cfg.Middleware) - 1; i >= 0; i-- {
		if mw := c.cfg.Middleware[i]; mw != nil {
			h = mw(h)
		}
	}
	return func(ctx context.Context, in []byte) ([]byte, error) {
		c.inflight.add()
		defer c.inflight.done()
		info, _ := workflow.StepInfoFrom(ctx)
		defer c.inflight.track.Begin(workflow.DrainCut{RunID: info.RunID, Workflow: info.Workflow, Step: info.Step, Attempt: info.Attempt})()
		// Deferred so a panicking attempt counts too; it runs before
		// inflight.done, so the recycle drain waits for this handler.
		defer c.jobDone()
		// Deferred last so it runs first, while the attempt still counts as in
		// flight: Drain reads the engine for the run's end after the handlers go quiet.
		defer c.inflight.noteReturned(info.RunID)
		return h(ctx, in)
	}
}

// stepInput is the input of step s: the run payload for a first step, its
// parents' output otherwise (workflow.StepInput, the rule every driver shares).
func stepInput(hctx hatchetsdk.Context, input any, s workflow.Step, tasks map[string]*hatchetsdk.Task) ([]byte, error) {
	if len(s.After) == 0 {
		return decodeEnvelope(input, payloadField)
	}
	outs := make(map[string][]byte, len(s.After))
	for _, p := range s.After {
		var doc map[string]any
		if err := hctx.ParentOutput(tasks[p], &doc); err != nil {
			return nil, fmt.Errorf("hatchet driver: step %q: output of %q: %w", s.Name, p, err)
		}
		out, err := decodeEnvelope(doc, outputField)
		if err != nil {
			return nil, err
		}
		outs[p] = out
	}
	return workflow.StepInput(nil, s.After, outs), nil
}

// onFailureFunc runs wf.OnFailure once when the run fails, with the failing
// step's input when it can be rebuilt and the run payload otherwise.
func (c *Client) onFailureFunc(wf workflow.Workflow, tasks map[string]*hatchetsdk.Task) func(hatchetsdk.Context, any) (any, error) {
	h := c.wrap(wf.OnFailure)
	byName := make(map[string]workflow.Step, len(wf.Steps))
	for _, s := range wf.Steps {
		byName[s.Name] = s
	}
	return func(hctx hatchetsdk.Context, input any) (any, error) {
		in, err := decodeEnvelope(input, payloadField)
		if err != nil {
			return nil, finalErr(err)
		}
		for name := range hctx.StepRunErrors() {
			if s, ok := byName[name]; ok {
				if stepIn, err := stepInput(hctx, input, s, tasks); err == nil {
					in = stepIn
				}
				break
			}
		}
		out, err := h(withStepInfo(hctx, wf.Name, failedStep(hctx), c.cfg.NodeID), in)
		if err != nil {
			return nil, finalErr(err)
		}
		return encodeOutput(out), nil
	}
}
