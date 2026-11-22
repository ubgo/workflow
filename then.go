package workflow

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
)

// LabelThenFrom is the run that started a job through Then: the finished
// run's RunID (or "scheduled:" plus its ScheduledID for a delayed run
// cancelled before it started). Only Then sets it.
//
//ds:def id=labelthenfrom-8h5tpxrg owner=@khanakia stability=stable
const LabelThenFrom = LabelPrefixInternal + "then-from"

// ThenRule starts the next job when a run of When ends in one of On
// (EV-08).
//
//ds:def id=thenrule-rcv7hzmr owner=@khanakia stability=stable
type ThenRule struct {
	// Name identifies the rule in the next job's idempotency key and in
	// errors; stable across deploys. Empty means When + ">" + the next
	// job's Workflow, which is enough unless two rules for one When start
	// the same workflow.
	Name string

	// When is the workflow whose run ends trigger the rule. Required.
	When string

	// On is the terminal statuses that trigger it; nil means
	// StatusSucceeded only. A status that is not terminal is refused.
	On []Status

	// Start builds the next job from the finished run's event: the
	// output is not in the event, so a job that needs it reads it with
	// ResultReader or passes the run ref on. Returning false starts
	// nothing (a condition on the outcome or labels). Required.
	Start func(RunEvent) (Job, bool, error)
}

// ThenError is an error Then could not hand back to anyone: a rule's
// Start failed, its job carried a reserved label, or the enqueue failed.
// It goes to the ThenErrors function. The next job was not started; an
// app that must not lose it re-enqueues Job from here (Job is zero when
// Start failed).
//
//ds:def id=thenerror-9gk7jpsy owner=@khanakia stability=stable
type ThenError struct {
	Rule string
	// From is the finished run.
	From RunRef
	Job  Job
	Err  error
}

// Error names the rule and the finished run.
func (e *ThenError) Error() string {
	return fmt.Sprintf("workflow: then %q after run %s: %v", e.Rule, thenRunKey(e.From), e.Err)
}

// Unwrap returns the cause.
func (e *ThenError) Unwrap() error { return e.Err }

// ThenOption configures Then.
type ThenOption func(*thenPlugin)

// ThenErrors sends Then's errors (each a *ThenError) to fn. The default
// logs them with slog.Default at error level, because an observer has no
// caller and a next job that silently does not start is worse than a log
// line. fn runs on the driver's observer goroutine; it should return
// quickly.
//
//ds:def id=thenerrors-j2g8pm8g owner=@khanakia stability=stable
func ThenErrors(fn func(error)) ThenOption { return func(p *thenPlugin) { p.errs = fn } }

// ThenAfterRedo applies the rules to the ends of runs carrying LabelRedo
// too: a run reset after it had succeeded (ResetOptions.Succeeded) and a
// fork of a run that had succeeded (RunForker). By default Then skips
// them, because the first success already started each rule's next job
// and a second one would run that job again. Pass it when the next job
// must follow the corrected result, and make that job idempotent.
//
//ds:def id=thenafterredo-zh59sk4a owner=@khanakia stability=stable
func ThenAfterRedo() ThenOption { return func(p *thenPlugin) { p.afterRedo = true } }

// Then returns a plugin, named "then", that starts the next job when a
// run finishes (EV-08): a completion trigger, so "when import succeeds,
// start index" lives beside the workflows instead of inside import's last
// step. Pass it to Use before Register and Start.
//
// It observes run ends (RunFinishedProvider) and enqueues through the
// client Use returns, below the Then plugin: put Then first in Use so the
// other plugins' enqueue middleware (unique jobs, tracing) applies to the
// next job. Each next job gets the idempotency key
// "then/<rule>/<finished run>" (unless Start set one) and LabelThenFrom,
// so a run-finished event delivered twice starts the next job once while
// that job is unfinished; after it finishes, a late duplicate delivery
// would start it again, so on drivers that redeliver late, the next
// workflow should itself be idempotent.
//
// Drivers: it needs RunNotifier, so Use refuses a client without it.
// With CapRunFinished (memory, sqlstore, Redis) every run end is
// delivered, including after a restart on the durable drivers; without
// it (Hatchet) delivery is best effort and a next job can be missed.
//
// Then refuses, with ErrInvalid, no rules, a rule without When or Start,
// a status in On that is not terminal, and two rules with one Name. Rules
// are tried in order; every matching rule starts its own job.
//
//ds:def id=then-j2sz7cnw owner=@khanakia stability=stable
func Then(rules []ThenRule, opts ...ThenOption) (Plugin, error) {
	if len(rules) == 0 {
		return nil, invalidf("then: no rules")
	}
	names := map[string]bool{}
	for i, r := range rules {
		if r.When == "" || r.Start == nil {
			return nil, invalidf("then: rule %d needs When and Start", i)
		}
		for _, s := range r.On {
			if !s.IsTerminal() {
				return nil, invalidf("then: rule %d: status %q is not terminal", i, s)
			}
		}
		if r.Name != "" {
			if names[r.Name] {
				return nil, invalidf("then: two rules are named %q", r.Name)
			}
			names[r.Name] = true
		}
	}
	p := &thenPlugin{rules: slices.Clone(rules)}
	for _, o := range opts {
		o(p)
	}
	if p.errs == nil {
		p.errs = func(err error) { slog.Default().Error("workflow: then: next job not started", "error", err) }
	}
	return p, nil
}

// thenPlugin learns the enqueue it uses from Use itself: Use asks for its
// enqueue middleware, then for its observers, then builds the enqueue
// chain, calling the middleware with the next enqueue. Each Use gets a
// slot (made when the middleware is asked for, read by the observer made
// right after, filled when the chain is built), so one plugin value used
// with two clients enqueues each run's next job on its own client.
type thenPlugin struct {
	rules     []ThenRule
	errs      func(error)
	afterRedo bool

	mu   sync.Mutex
	last *thenSlot // the slot of the Use in progress
}

// thenSlot is the enqueue one Use call bound.
type thenSlot struct {
	mu  sync.Mutex
	enq EnqueueFunc
}

func (s *thenSlot) get() EnqueueFunc {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enq
}

// Name names the plugin in refusals.
func (p *thenPlugin) Name() string { return "then" }

// EnqueueMiddleware binds this Use call's enqueue and passes every job
// through unchanged.
//
//ds:def id=thenplugin-enqueuemiddleware-45pnxp9j owner=@khanakia stability=stable
func (p *thenPlugin) EnqueueMiddleware() []EnqueueMiddleware {
	slot := &thenSlot{}
	p.mu.Lock()
	p.last = slot
	p.mu.Unlock()
	return []EnqueueMiddleware{func(next EnqueueFunc) EnqueueFunc {
		slot.mu.Lock()
		slot.enq = next
		slot.mu.Unlock()
		return next
	}}
}

// RunFinishedObservers returns the observer that applies the rules.
func (p *thenPlugin) RunFinishedObservers() []RunFinishedFunc {
	p.mu.Lock()
	slot := p.last
	p.last = nil
	p.mu.Unlock()
	if slot == nil {
		slot = &thenSlot{} // never bound: every match reports an error
	}
	return []RunFinishedFunc{func(ctx context.Context, ev RunEvent) { p.observe(ctx, slot, ev) }}
}

// observe starts the next job of every rule ev matches.
//
//ds:def id=thenplugin-observe-fkwupcpn owner=@khanakia stability=stable
func (p *thenPlugin) observe(ctx context.Context, slot *thenSlot, ev RunEvent) {
	if ev.Labels[LabelRedo] == "true" && !p.afterRedo {
		return // a redo: the first success already started the next jobs
	}
	for _, r := range p.rules {
		on := r.On
		if len(on) == 0 {
			on = []Status{StatusSucceeded}
		}
		if r.When != ev.Workflow || !slices.Contains(on, ev.Status) {
			continue
		}
		job, ok, err := r.Start(ev)
		name := r.Name
		if name == "" {
			name = r.When + ">" + job.Workflow
		}
		if err != nil {
			p.errs(&ThenError{Rule: name, From: ev.Ref, Err: err})
			continue
		}
		if !ok {
			continue
		}
		if err := ValidateLabels(job.Labels); err != nil {
			p.errs(&ThenError{Rule: name, From: ev.Ref, Job: job, Err: err})
			continue
		}
		from := thenRunKey(ev.Ref)
		job.Labels = maps.Clone(job.Labels)
		if job.Labels == nil {
			job.Labels = map[string]string{}
		}
		job.Labels[LabelThenFrom] = from
		if job.IdempotencyKey == "" {
			job.IdempotencyKey = "then/" + name + "/" + from
		}
		enq := slot.get()
		if enq == nil {
			p.errs(&ThenError{Rule: name, From: ev.Ref, Job: job, Err: invalidf("then: the plugin was not applied with Use, so it has no client to enqueue on")})
			continue
		}
		if _, err := enq(AllowReservedLabels(ctx, LabelThenFrom), job); err != nil {
			p.errs(&ThenError{Rule: name, From: ev.Ref, Job: job, Err: err})
		}
	}
}

// thenRunKey is the finished run's ID, or its scheduled ID for a delayed run
// that never started.
func thenRunKey(ref RunRef) string {
	if !ref.RunID.IsZero() {
		return ref.RunID.String()
	}
	return "scheduled:" + ref.ScheduledID.String()
}
