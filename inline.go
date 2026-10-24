package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// InlineRunID is the RunID a step sees in its StepInfo under RunInline.
const InlineRunID RunID = "inline"

// InlineResult is what RunInline returns, success or failure.
type InlineResult struct {
	// Outputs holds the output of every step that succeeded (a wait
	// step's event payload, a sleep step's input), by step name.
	Outputs map[string][]byte

	// Attempts holds how many attempts each Run step made.
	Attempts map[string]int
}

// inlineConfig is what InlineOptions set.
type inlineConfig struct {
	mw     []Middleware
	sleep  func(ctx context.Context, d time.Duration) error
	now    func() time.Time
	events []Event
}

// InlineOption configures RunInline.
type InlineOption func(*inlineConfig)

// InlineMiddleware wraps every step and OnFailure handler, first
// outermost, the way a driver's middleware option does. Pass the same
// middleware the app gives its driver to run steps exactly as they run
// there.
func InlineMiddleware(mw ...Middleware) InlineOption {
	return func(c *inlineConfig) { c.mw = append(c.mw, mw...) }
}

// InlineSleep replaces how RunInline waits (retry backoffs and sleep
// steps). The default is a real timer that ends early with ctx. A test
// passes a function that advances a fake clock, or one that returns at
// once. Nil is ignored.
func InlineSleep(sleep func(ctx context.Context, d time.Duration) error) InlineOption {
	return func(c *inlineConfig) {
		if sleep != nil {
			c.sleep = sleep
		}
	}
}

// InlineNow replaces RunInline's clock, read for RetryPolicy.Until and
// WaitSpec.Deadline. The default is time.Now. Nil is ignored.
func InlineNow(now func() time.Time) InlineOption {
	return func(c *inlineConfig) {
		if now != nil {
			c.now = now
		}
	}
}

// InlineEvents supplies the events wait steps take: each wait step takes
// the first event not yet taken whose name and resolved key match. A wait
// step with no matching event fails as if it timed out. Without this
// option a workflow with a wait step is refused.
func InlineEvents(events ...Event) InlineOption {
	return func(c *inlineConfig) { c.events = append(c.events, events...) }
}

// realSleep waits d on a real timer, or until ctx ends.
func realSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RunInline runs wf once, as job, in the caller's goroutine: no client, no Start, no goroutine of its own, for tests,
// command-line tools and one-off scripts.
//
// Steps run one at a time in a fixed order: of the steps whose parents
// have all succeeded, the first in declaration order goes next. Each gets
// its StepInput, a StepInfo (RunID InlineRunID), its timeout, its retry
// policy (EffectiveRetriesFor, decided by RetryPolicy.Next, waiting
// through the sleeper) and the InlineMiddleware. Sleep steps sleep. Wait
// steps take an InlineEvents event; a workflow with a wait step and no
// InlineEvents option is refused with an UnsupportedError before anything
// runs. When a step fails for good, OnFailure runs once with that step's
// input, and the error is returned wrapped with the step name. A step that
// returns Discard, or whose policy has DiscardOnExhaust and gives up,
// ends the call with an error matching ErrDiscarded and ErrCancelled,
// without OnFailure.
//
// Concurrency, rate limits, idempotency keys, cron, queues, priority and
// RunAt shape how a driver schedules many runs; a single inline call has
// nothing to schedule, so it ignores them. It validates wf and job first
// (ErrInvalid).
func RunInline(ctx context.Context, wf Workflow, job Job, opts ...InlineOption) (InlineResult, error) {
	cfg := inlineConfig{sleep: realSleep, now: time.Now}
	for _, o := range opts {
		o(&cfg)
	}
	res := InlineResult{Outputs: map[string][]byte{}, Attempts: map[string]int{}}
	if job.Workflow == "" {
		job.Workflow = wf.Name
	}
	if err := Validate(wf); err != nil {
		return res, err
	}
	if err := ValidateJob(job); err != nil {
		return res, err
	}
	if job.Workflow != wf.Name {
		return res, invalidf("RunInline: job names workflow %q, not %q", job.Workflow, wf.Name)
	}
	if cfg.events == nil {
		for _, s := range wf.Steps {
			if s.Kind() == StepWait {
				return res, Unsupported("WaitSpec", "RunInline has no Signal; pass InlineEvents for step "+s.Name)
			}
		}
	}
	wrap := func(h Handler) Handler {
		for i := len(cfg.mw) - 1; i >= 0; i-- {
			h = cfg.mw[i](h)
		}
		return h
	}
	in := &inlineRun{cfg: cfg, wf: wf, job: job, res: res, taken: make([]bool, len(cfg.events)), wrap: wrap}
	return in.res, in.run(ctx)
}

// inlineRun is the state of one RunInline call.
type inlineRun struct {
	cfg   inlineConfig
	wf    Workflow
	job   Job
	res   InlineResult
	taken []bool // which cfg.events a wait step has taken
	wrap  func(Handler) Handler
}

func (r *inlineRun) run(ctx context.Context) error {
	done := map[string]bool{}
	for len(done) < len(r.wf.Steps) {
		step, ok := r.nextReady(done)
		if !ok { // unreachable for a validated DAG; kept so a bug cannot loop
			return fmt.Errorf("workflow: RunInline: no step is ready in %q", r.wf.Name)
		}
		outs := make(map[string][]byte, len(step.After))
		for _, p := range step.After {
			outs[p] = r.res.Outputs[p]
		}
		input := StepInput(r.job.Payload, step.After, outs)
		out, err := r.step(ctx, step, input)
		if err != nil {
			if IsDiscarded(err) {
				return DiscardedError(step.Name, err)
			}
			if r.wf.OnFailure != nil {
				fctx := WithStepInfo(ctx, StepInfo{Workflow: r.wf.Name, Step: step.Name, RunID: InlineRunID, Attempt: r.res.Attempts[step.Name], Labels: r.job.Labels})
				_, _ = callInline(r.wrap(r.wf.OnFailure), fctx, input) // the step's error is the one returned
			}
			return fmt.Errorf("workflow: RunInline: step %q failed: %w", step.Name, err)
		}
		r.res.Outputs[step.Name] = out
		done[step.Name] = true
	}
	return nil
}

// nextReady is the first step in declaration order not yet done whose
// parents are all done.
func (r *inlineRun) nextReady(done map[string]bool) (Step, bool) {
	for _, s := range r.wf.Steps {
		if done[s.Name] {
			continue
		}
		ready := true
		for _, p := range s.After {
			if !done[p] {
				ready = false
				break
			}
		}
		if ready {
			return s, true
		}
	}
	return Step{}, false
}

// step runs one step of any kind.
func (r *inlineRun) step(ctx context.Context, s Step, in []byte) ([]byte, error) {
	switch s.Kind() {
	case StepSleep:
		if err := r.cfg.sleep(ctx, s.Sleep); err != nil {
			return nil, err
		}
		return in, nil
	case StepWait:
		return r.wait(s)
	}
	h := r.wrap(s.Run)
	policy := EffectiveRetriesFor(s, r.wf, r.job)
	timeout := s.Timeout
	if timeout == 0 {
		timeout = r.wf.Timeout
	}
	a := Attempts{First: r.cfg.now()}
	for {
		attempt := a.N + 1
		r.res.Attempts[s.Name] = attempt
		actx := WithStepInfo(ctx, StepInfo{Workflow: r.wf.Name, Step: s.Name, RunID: InlineRunID, Attempt: attempt, Labels: r.job.Labels, Queue: EffectiveQueue(r.wf, r.job), ReadyAt: r.cfg.now()})
		cancel := context.CancelFunc(func() {})
		if timeout > 0 {
			actx, cancel = context.WithTimeout(actx, timeout)
		}
		out, err := callInline(h, actx, in)
		timedOut := timeout > 0 && errors.Is(actx.Err(), context.DeadlineExceeded)
		cancel()
		if err == nil && !timedOut {
			return out, nil
		}
		if timedOut {
			err = fmt.Errorf("workflow: RunInline: step %q attempt %d timed out after %s: %w", s.Name, attempt, timeout, context.DeadlineExceeded)
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w: %w", ErrCancelled, err)
		}
		a = a.Count(err)
		delay, retry := policy.Next(err, a, r.cfg.now(), nil)
		if !retry {
			if dis := policy.ExhaustedError(attempt, err); dis != nil {
				return nil, dis // DiscardOnExhaust: run() ends the call discarded, without OnFailure, like a driver
			}
			return nil, err
		}
		if serr := r.cfg.sleep(ctx, delay); serr != nil {
			return nil, fmt.Errorf("%w: %w", ErrCancelled, serr)
		}
	}
}

// wait takes the first untaken event matching the wait step.
func (r *inlineRun) wait(s Step) ([]byte, error) {
	key, err := s.Wait.ResolveKey(r.job)
	if err != nil {
		return nil, err
	}
	if !s.Wait.Deadline.IsZero() && !r.cfg.now().Before(s.Wait.Deadline) {
		return nil, fmt.Errorf("workflow: RunInline: wait step %q is past its Deadline: %w", s.Name, context.DeadlineExceeded)
	}
	for i, ev := range r.cfg.events {
		if !r.taken[i] && ev.Name == s.Wait.Event && ev.Key == key {
			r.taken[i] = true
			return ev.Payload, nil
		}
	}
	return nil, fmt.Errorf("workflow: RunInline: no InlineEvents event %q with key %q for wait step %q: %w", s.Wait.Event, key, s.Name, context.DeadlineExceeded)
}

// callInline runs h, turning a panic into an error as every driver does.
func callInline(h Handler, ctx context.Context, in []byte) (out []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			out, err = nil, fmt.Errorf("workflow: RunInline: handler panicked: %v", p)
		}
	}()
	return h(ctx, in)
}
