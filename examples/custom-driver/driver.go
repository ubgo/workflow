package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// snippet:caps
// tinyCaps is everything this driver can really do: nothing beyond the
// core contract. Run steps as a DAG, retries, timeouts, OnFailure and
// Status need no capability; delayed starts, idempotency keys, cron,
// cancel, waits and sleeps do, and tiny has none of them, so it refuses
// each one instead of ignoring it.
const tinyCaps workflow.Caps = 0

// snippet:end

// snippet:type
// Tiny is a minimal in-process driver of workflow.Client. Runs live in a
// map and each run executes on its own goroutine, step by step in DAG
// order. Every timer comes from the injected clock, so the conformance
// suite drives it on a fake clock.
type Tiny struct {
	clk clock.Clock

	mu      sync.Mutex
	wfs     map[string]workflow.Workflow
	runs    map[workflow.RunID]workflow.Status
	ctx     context.Context // the Start context; nil before Start
	lastRun int
}

var _ workflow.Client = (*Tiny)(nil)

// NewTiny returns an unstarted driver reading time from clk. It starts no
// goroutine: nothing runs until Start.
func NewTiny(clk clock.Clock) *Tiny {
	return &Tiny{clk: clk, wfs: map[string]workflow.Workflow{}, runs: map[workflow.RunID]workflow.Status{}}
}

// Capabilities never changes over the life of a client.
func (t *Tiny) Capabilities() workflow.Caps { return tinyCaps }

// snippet:end

// snippet:lifecycle
// Register validates the shape of wf (workflow.Validate wraps
// ErrInvalid) and stores it; registering a name again replaces it.
func (t *Tiny) Register(wf workflow.Workflow) error {
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx != nil {
		return workflow.ErrAlreadyStarted
	}
	t.wfs[wf.Name] = wf
	return nil
}

// Start refuses any registered workflow that needs a capability tiny
// lacks, then remembers ctx: cancelling it stops every run.
func (t *Tiny) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx != nil {
		return workflow.ErrAlreadyStarted
	}
	for _, wf := range t.wfs {
		if err := workflow.CheckCaps(tinyCaps, wf); err != nil {
			return err
		}
	}
	t.ctx = ctx
	return nil
}

// snippet:end

// snippet:enqueue
// Enqueue refuses a Job field tiny cannot honour, naming the field, then
// starts the run on its own goroutine.
func (t *Tiny) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	// The job's shape first (ErrInvalid), then what it asks of the driver:
	// an UnsupportedError naming the field (RunAt, a key, a queue, Retry).
	if err := workflow.ValidateJob(job); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.CheckJob(tinyCaps, job); err != nil {
		return workflow.RunRef{}, err
	}
	// Labels under a reserved prefix belong to the library; an app's
	// Enqueue may not set them (CheckJobLabels, first in every public entry).
	if err := workflow.CheckJobLabels(ctx, job); err != nil {
		return workflow.RunRef{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx == nil {
		return workflow.RunRef{}, workflow.ErrNotStarted
	}
	wf, ok := t.wfs[job.Workflow]
	if !ok {
		return workflow.RunRef{}, fmt.Errorf("%w: workflow %q", workflow.ErrNotFound, job.Workflow)
	}
	t.lastRun++
	id := workflow.RunID("tiny-" + strconv.Itoa(t.lastRun))
	t.runs[id] = workflow.StatusQueued
	go t.execute(t.ctx, wf, job, id)
	return workflow.RunRef{RunID: id}, nil
}

// snippet:end

// snippet:refuse
// Cancel, Schedule and Signal are where a driver without the capability
// says so. A Signal can match nothing, because Start refused every wait
// step, and a signal nobody waits for is not an error.
func (t *Tiny) Cancel(context.Context, workflow.RunRef) error {
	return &workflow.UnsupportedError{Field: "Cancel", Missing: workflow.CapCancel}
}

func (t *Tiny) Schedule(_ context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if err := workflow.ValidateSchedule(s); err != nil {
		return "", err
	}
	// Without CapCron this always refuses, before or after Start.
	return "", workflow.CheckScheduleCaps(tinyCaps, s)
}

func (t *Tiny) Unschedule(_ context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
}

func (t *Tiny) Signal(context.Context, workflow.Event) error { return t.started() }

// started is ErrNotStarted before Start, as the contract asks of Signal.
func (t *Tiny) started() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx == nil {
		return workflow.ErrNotStarted
	}
	return nil
}

// snippet:end

// Status reports where a run is; an unknown run is ErrNotFound.
func (t *Tiny) Status(_ context.Context, ref workflow.RunRef) (workflow.Status, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st, ok := t.runs[ref.RunID]
	if !ok {
		return "", fmt.Errorf("%w: run %q", workflow.ErrNotFound, ref.RunID)
	}
	return st, nil
}

func (t *Tiny) setStatus(id workflow.RunID, st workflow.Status) {
	t.mu.Lock()
	t.runs[id] = st
	t.mu.Unlock()
}

// snippet:execute
// execute runs the steps in DAG order: a step starts once all its After
// parents succeeded, with the input workflow.StepInput builds. The first
// step that fails for good fails the run, after OnFailure runs once with
// that step's input.
func (t *Tiny) execute(ctx context.Context, wf workflow.Workflow, job workflow.Job, id workflow.RunID) {
	t.setStatus(id, workflow.StatusRunning)
	outs := map[string][]byte{}
	for len(outs) < len(wf.Steps) {
		for _, s := range wf.Steps {
			if _, done := outs[s.Name]; done || !parentsDone(s, outs) {
				continue
			}
			in := workflow.StepInput(job.Payload, s.After, outs)
			out, err := t.runStep(ctx, wf, s, job, id, in)
			if err != nil {
				if ctx.Err() != nil {
					t.setStatus(id, workflow.StatusCancelled)
					return
				}
				if wf.OnFailure != nil {
					_, _ = wf.OnFailure(ctx, in)
				}
				t.setStatus(id, workflow.StatusFailed)
				return
			}
			outs[s.Name] = out
		}
	}
	t.setStatus(id, workflow.StatusSucceeded)
}

// parentsDone reports whether every step s runs After has an output, so s
// may start; a step with no After is always ready.
func parentsDone(s workflow.Step, outs map[string][]byte) bool {
	for _, p := range s.After {
		if _, ok := outs[p]; !ok {
			return false
		}
	}
	return true
}

// snippet:end

// snippet:attempts
// runStep tries a step until it succeeds or its effective retry policy
// gives up. Every attempt carries StepInfo on its ctx and its timeout,
// and every wait reads the injected clock, never time.Now.
func (t *Tiny) runStep(ctx context.Context, wf workflow.Workflow, s workflow.Step, job workflow.Job, id workflow.RunID, in []byte) ([]byte, error) {
	policy := workflow.EffectiveRetries(s, wf)
	timeout := s.Timeout
	if timeout == 0 {
		timeout = wf.Timeout
	}
	for attempt := 1; ; attempt++ {
		info := workflow.StepInfo{Workflow: wf.Name, Step: s.Name, RunID: id, Attempt: attempt, Labels: job.Labels}
		out, err := t.attempt(workflow.WithStepInfo(ctx, info), s.Run, in, timeout)
		if err == nil {
			return out, nil
		}
		if ctx.Err() != nil || !policy.Retryable(err, attempt) {
			return nil, err
		}
		if !t.sleep(ctx, policy.NextDelay(err, attempt, nil)) {
			return nil, ctx.Err()
		}
	}
}

// attempt runs h once, cancelling its ctx when timeout passes on the
// injected clock.
func (t *Tiny) attempt(ctx context.Context, h workflow.Handler, in []byte, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		return h(ctx, in)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timer := t.clk.NewTimer(timeout)
	defer timer.Stop()
	go func() {
		select {
		case <-timer.C():
			cancel(fmt.Errorf("step timed out after %s", timeout))
		case <-ctx.Done():
		}
	}()
	out, err := h(ctx, in)
	if err == nil && ctx.Err() != nil {
		err = context.Cause(ctx)
	}
	return out, err
}

// sleep waits d on the injected clock, or reports false when ctx ends.
func (t *Tiny) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := t.clk.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C():
		return true
	case <-ctx.Done():
		return false
	}
}

// snippet:end
