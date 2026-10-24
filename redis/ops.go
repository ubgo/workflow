package redis

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ubgo/workflow"
)

// Pauses live in {base}:paused, one field per target ("all", "w:<workflow>",
// "q:<queue>"), so every client of the prefix obeys them and they survive
// a restart. The claim script passes over held runs; a client holding a
// run checks between rounds and hands it back when a pause now holds it.

// pauseField is the field of the pause hash for t.
func pauseField(t workflow.PauseTarget) string {
	switch {
	case t.All:
		return "all"
	case t.Workflow != "":
		return "w:" + t.Workflow
	}
	return "q:" + t.Queue
}

// Pause holds the runs t names: no step attempt of them is claimed until
// Resume; attempts already executing finish. See workflow.Pauser.
func (c *Client) Pause(ctx context.Context, t workflow.PauseTarget) error {
	if err := workflow.CheckPause(caps, t); err != nil {
		return err
	}
	if err := c.rdb.HSet(ctx, c.base+":paused", pauseField(t), "1").Err(); err != nil {
		return fmt.Errorf("redis: pause: %w", classify(err))
	}
	return nil
}

// Resume removes the pause on t; Resume(PauseAll()) removes every pause.
// Resuming a target that is not paused is a no-op.
func (c *Client) Resume(ctx context.Context, t workflow.PauseTarget) error {
	if err := workflow.CheckPause(caps, t); err != nil {
		return err
	}
	var err error
	if t.All {
		err = c.rdb.Del(ctx, c.base+":paused").Err()
	} else {
		err = c.rdb.HDel(ctx, c.base+":paused", pauseField(t)).Err()
	}
	if err != nil {
		return fmt.Errorf("redis: resume: %w", classify(err))
	}
	if err := c.rdb.Publish(ctx, c.base+":wake", "e").Err(); err != nil {
		return fmt.Errorf("redis: resume: %w", classify(err))
	}
	return nil
}

// Paused lists the targets in force: All first, then workflows, then
// queues, each by name.
func (c *Client) Paused(ctx context.Context) ([]workflow.PauseTarget, error) {
	fields, err := c.rdb.HKeys(ctx, c.base+":paused").Result()
	if err != nil {
		return nil, fmt.Errorf("redis: list pauses: %w", classify(err))
	}
	sort.Strings(fields)
	var all, wfs, qs []workflow.PauseTarget
	for _, f := range fields {
		switch {
		case f == "all":
			all = append(all, workflow.PauseAll())
		case strings.HasPrefix(f, "w:"):
			wfs = append(wfs, workflow.PauseWorkflow(f[2:]))
		case strings.HasPrefix(f, "q:"):
			qs = append(qs, workflow.PauseQueue(f[2:]))
		}
	}
	return append(append(all, wfs...), qs...), nil
}

// Retry re-opens a failed run from the step that failed: steps that
// succeeded keep their outputs, the failed step and every step it held
// back start again with fresh attempt counts. The run keeps its RunRef,
// is admitted again under its workflow's policies, takes its idempotency
// key again (ErrDuplicate when another unfinished run holds it) and is
// reported again to run-finished observers when it ends. See
// workflow.RunRetrier.
func (c *Client) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	id, err := runID(ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	r, err := c.load(ctx, id, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if r.status != workflow.StatusFailed {
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s, not failed", workflow.ErrWrongState, ref, r.status)
	}
	for name, rec := range r.steps {
		if !rec.Done {
			r.steps[name] = &stepRec{}
		}
	}
	h := &held{stored: r}
	res, err := retryScript.Run(ctx, c.rdb, c.keys(), c.base, id, r.finishedRaw, encodeSteps(h), c.clk.Now().UnixMilli()).Slice()
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("redis: retry: %w", classify(err))
	}
	switch str(res[0]) {
	case "ok":
		c.wakeAll()
		c.pokeDispatcher()
		return refOf(id, workflow.StatusQueued, r.sid), nil
	case "notfound":
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	case "dup":
		return workflow.RunRef{}, fmt.Errorf("%w: run %+v: its idempotency key is held by run %s", workflow.ErrDuplicate, ref, str(res[1]))
	}
	return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s, not failed", workflow.ErrWrongState, ref, str(res[1]))
}

// CancelAll cancels every run matching q in the statuses
// workflow.CancelStatuses picks (by default the runs not yet started),
// each exactly as Cancel would, and returns how many it cancelled. A run
// that moved to another status meanwhile is left alone. See
// workflow.BulkCanceller.
func (c *Client) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return 0, err
	}
	if err := workflow.CheckRunQuery(caps, q); err != nil {
		return 0, err
	}
	statuses := workflow.CancelStatuses(q)
	q.Statuses = statuses
	extra := make([]any, len(statuses))
	for i, s := range statuses {
		extra[i] = string(s)
	}
	n := 0
	for {
		page, err := c.Runs(ctx, q)
		if err != nil {
			return n, err
		}
		for _, info := range page.Runs {
			id, err := runID(info.Ref)
			if err != nil {
				return n, err
			}
			args := append([]any{c.base, id, c.clk.Now().UnixNano(), workflow.ErrCancelled.Error(), c.retention.Milliseconds()}, extra...)
			res, err := cancelScript.Run(ctx, c.rdb, c.keys(), args...).Text()
			if err != nil {
				return n, fmt.Errorf("redis: cancel: %w", classify(err))
			}
			if res == "ok" {
				n++
			}
		}
		if page.Next == "" {
			break
		}
		q.Cursor = page.Next
	}
	if n > 0 {
		c.pokeNotifier()
	}
	return n, nil
}

var (
	_ workflow.Pauser        = (*Client)(nil)
	_ workflow.RunRetrier    = (*Client)(nil)
	_ workflow.BulkCanceller = (*Client)(nil)
)

// CancelStep withdraws one step of a run (workflow.StepCanceller): a
// running attempt's ctx is cancelled with cause workflow.ErrStepCancelled
// on the client that holds the run, and what the handler returns decides
// (an output: the step succeeded; an error: it fails for good); a step in
// its backoff, snooze, wait or sleep fails for good at once, with an error
// matching workflow.ErrStepCancelled. A step not started or ended is
// ErrWrongState; an unknown run or step ErrNotFound.
func (c *Client) CancelStep(ctx context.Context, ref workflow.RunRef, step string) error {
	id, err := runID(ref)
	if err != nil {
		return err
	}
	r, err := c.load(ctx, id, ref)
	if err != nil {
		return err
	}
	wf, known := c.registered(r.wf)
	var spec workflow.Step
	if known {
		found := false
		for _, s := range wf.Steps {
			if s.Name == step {
				spec, found = s, true
			}
		}
		if !found {
			return fmt.Errorf("%w: run %+v has no step %q", workflow.ErrNotFound, ref, step)
		}
	}
	if r.status.IsTerminal() {
		return fmt.Errorf("%w: run %+v is %s", workflow.ErrWrongState, ref, r.status)
	}
	rec := r.steps[step]
	if rec == nil {
		rec = &stepRec{}
	}
	if rec.Done || !c.stepStarted(r, wf, known, spec, rec) {
		return fmt.Errorf("%w: step %q of run %+v has not started or has ended", workflow.ErrWrongState, step, ref)
	}
	res, err := cancelStepScript.Run(ctx, c.rdb, c.keys(), c.base, id, step, c.clk.Now().UnixMilli()).Text()
	if err != nil {
		return fmt.Errorf("redis: cancel step: %w", classify(err))
	}
	switch res {
	case "ok":
		c.wakeAll()
		return nil
	case "notfound":
		return fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
	}
	return fmt.Errorf("%w: run %+v is %s", workflow.ErrWrongState, ref, res)
}

// stepStarted reports whether a step of r has begun: an attempt made or in
// backoff or snooze, a wait registered, a sleep begun, or (for a running
// run) its parents all done, so its first attempt is executing.
func (c *Client) stepStarted(r *stored, wf workflow.Workflow, known bool, s workflow.Step, rec *stepRec) bool {
	if rec.N > 0 || rec.Z || rec.Due > 0 {
		return true
	}
	if !known {
		return r.status == workflow.StatusRunning || r.status == workflow.StatusWaiting
	}
	for _, p := range s.After {
		if pr := r.steps[p]; pr == nil || !pr.Done {
			return false
		}
	}
	switch s.Kind() {
	case workflow.StepWait:
		return r.status == workflow.StatusWaiting || r.status == workflow.StatusRunning
	}
	return r.status == workflow.StatusRunning
}

// registered returns the workflow named name when this client registered
// it.
func (c *Client) registered(name string) (workflow.Workflow, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r := c.wfs[name]; r != nil {
		return r.wf, true
	}
	return workflow.Workflow{}, false
}

var _ workflow.StepCanceller = (*Client)(nil)
