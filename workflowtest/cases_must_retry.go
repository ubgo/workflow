package workflowtest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
)

// Retry precedence and discard-on-exhaust conformance.
//
// Precedence (job > step > workflow > default) is a matrix over what each
// level says: the job's Retry is unset, RetriesNone or a Max; the step's is
// unset, RetriesNone or a Max; the workflow's is unset or a Max. Every
// combination runs a step that always fails and the number of attempts is
// compared with the first set level, computed here independently of
// EffectiveRetriesFor (and then cross-checked against it).
//
// Discard-on-exhaust is a matrix over how the policy stops (Max exhausted,
// Permanent, NoRetryOn, MaxErrors, Until; plus the two non-stops: a retry
// that succeeds and an explicit Discard) x where the winning policy is
// declared (workflow, step, job) x DiscardOnExhaust on or off. At the two
// outer levels the loser is a decoy with the opposite flag and a different
// Max, so a driver that reads the wrong level or merges two levels is seen
// in both the status and the attempt count.

// Max values the precedence matrix gives each level; distinct, so the
// attempt count names the level that won.
const (
	precJobMax  = 1
	precStepMax = 2
	precWfMax   = 3
)

// retryLevel is what one level of the precedence chain says.
type retryLevel int

const (
	levelUnset retryLevel = iota
	levelNone             // RetriesNone(): an explicit "never retry"
	levelMax              // a policy with that level's Max
)

func (l retryLevel) String() string { return [...]string{"unset", "none", "max"}[l] }

// policyAt builds a level's policy; nil when unset.
func (l retryLevel) policyAt(max int) *workflow.RetryPolicy {
	switch l {
	case levelNone:
		return workflow.RetriesNone()
	case levelMax:
		return &workflow.RetryPolicy{Max: max}
	}
	return nil
}

// wantAttempts is the attempts a step makes when it always fails: the Max
// of the first set level, job over step over workflow, plus the first
// attempt; with nothing set, the default (no retries).
func wantAttempts(job, step retryLevel, wfSet bool) int {
	switch {
	case job == levelNone, job == levelUnset && step == levelNone:
		return 1
	case job == levelMax:
		return precJobMax + 1
	case step == levelMax:
		return precStepMax + 1
	case wfSet:
		return precWfMax + 1
	}
	return workflow.DefaultRetryMax + 1
}

var retryPrecedenceCases = []testCase{
	{"retries resolve job over step over workflow over default, whole policies, never merged", func(e *env) {
		hasJob := e.c.Capabilities().Has(workflow.CapJobRetry)
		type combo struct {
			job, step retryLevel
			wfSet     bool
			wf        workflow.Workflow
			c         *counter
		}
		var combos []*combo
		var wfs []workflow.Workflow
		for _, job := range []retryLevel{levelUnset, levelNone, levelMax} {
			if job != levelUnset && !hasJob {
				continue
			}
			for _, step := range []retryLevel{levelUnset, levelNone, levelMax} {
				for _, wfSet := range []bool{false, true} {
					c := &combo{job: job, step: step, wfSet: wfSet, c: &counter{}}
					c.wf = workflow.Workflow{
						Name:  e.name(fmt.Sprintf("j-%s-s-%s-w-%t", job, step, wfSet)),
						Steps: []workflow.Step{{Name: "only", Run: c.c.fail(errBoom), Retry: step.policyAt(precStepMax)}},
					}
					if wfSet {
						c.wf.Retry = workflow.RetryPolicy{Max: precWfMax}
					}
					combos = append(combos, c)
					wfs = append(wfs, c.wf)
				}
			}
		}
		e.up(wfs...)
		refs := make([]workflow.RunRef, len(combos))
		for i, c := range combos {
			refs[i] = e.enqueue(workflow.Job{Workflow: c.wf.Name, Retry: c.job.policyAt(precJobMax)})
		}
		for i, c := range combos {
			e.waitStatus(refs[i], workflow.StatusFailed, 0)
			want := wantAttempts(c.job, c.step, c.wfSet)
			jobPolicy := c.job.policyAt(precJobMax)
			job := workflow.Job{Retry: jobPolicy}
			if got := workflow.EffectiveRetriesFor(c.wf.Steps[0], c.wf, job).Max + 1; got != want {
				e.t.Errorf("job=%s step=%s wf=%t: EffectiveRetriesFor says %d attempts, the matrix says %d", c.job, c.step, c.wfSet, got, want)
			}
		}
		e.quiet()
		for _, c := range combos {
			if got, want := c.c.count(), wantAttempts(c.job, c.step, c.wfSet); got != want {
				e.t.Errorf("job=%s step=%s wf=%t: %d attempts, want %d", c.job, c.step, c.wfSet, got, want)
			}
		}
	}},
	{"a higher level's policy replaces the lower one whole: MaxErrors of the step is not inherited by the job", func(e *env) {
		e.need(workflow.CapJobRetry | workflow.CapRetryMaxErrors)
		var stepWins, jobWins counter
		// step: Max 5 but MaxErrors 1 stops after one error; the job's
		// plain Max 2 must replace it (3 attempts), not inherit MaxErrors.
		a := oneStep(e.name("a"), stepWins.fail(errBoom))
		a.Steps[0].Retry = &workflow.RetryPolicy{Max: 5, MaxErrors: 1}
		// And the other way round: the job's MaxErrors 2 stops a step whose own Max is 4.
		b := oneStep(e.name("b"), jobWins.fail(errBoom))
		b.Steps[0].Retry = &workflow.RetryPolicy{Max: 4}
		e.up(a, b)
		ra := e.enqueue(workflow.Job{Workflow: a.Name, Retry: &workflow.RetryPolicy{Max: 2}})
		rb := e.enqueue(workflow.Job{Workflow: b.Name, Retry: &workflow.RetryPolicy{Max: 6, MaxErrors: 2}})
		e.waitStatus(ra, workflow.StatusFailed, 0)
		e.waitStatus(rb, workflow.StatusFailed, 0)
		e.quiet()
		if got := stepWins.count(); got != 3 {
			e.t.Errorf("job Max 2 over step {Max 5, MaxErrors 1}: %d attempts, want 3", got)
		}
		if got := jobWins.count(); got != 2 {
			e.t.Errorf("job {Max 6, MaxErrors 2} over step Max 4: %d attempts, want 2", got)
		}
	}},
}

// exhaustKind is one way a policy stops retrying, or one of the two
// non-stops the option must leave alone.
type exhaustKind struct {
	name string
	// needs are the capabilities the stopping rule itself requires.
	needs workflow.Caps
	// handler builds the step body; every call to it bumps n first.
	handler func(n *atomic.Int64, unit time.Duration) workflow.Handler
	// policy is the winning policy for discard on or off.
	policy func(discard bool, unit time.Duration) workflow.RetryPolicy
	// attempts the winning policy makes; 0 means "see want".
	attempts int
	// want is the terminal status when DiscardOnExhaust is on and off.
	wantOn, wantOff workflow.Status
	// noJob is set when the policy holds functions, which Job.Retry refuses.
	noJob bool
	// boom is set when the run's error must still carry errBoom's text.
	boom bool
}

func failWith(err error) func(*atomic.Int64, time.Duration) workflow.Handler {
	return func(n *atomic.Int64, _ time.Duration) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) {
			n.Add(1)
			return nil, err
		}
	}
}

var exhaustKinds = []exhaustKind{
	{
		name: "Max exhausted", handler: failWith(errBoom), attempts: 3, boom: true,
		policy: func(d bool, _ time.Duration) workflow.RetryPolicy {
			return workflow.RetryPolicy{Max: 2, DiscardOnExhaust: d}
		},
		wantOn:  workflow.StatusDiscarded,
		wantOff: workflow.StatusFailed,
	},
	{
		name: "Permanent", handler: failWith(workflow.Permanent(errBoom)), attempts: 1, boom: true,
		policy: func(d bool, _ time.Duration) workflow.RetryPolicy {
			return workflow.RetryPolicy{Max: 5, DiscardOnExhaust: d}
		},
		wantOn:  workflow.StatusDiscarded,
		wantOff: workflow.StatusFailed,
	},
	{
		name: "NoRetryOn", handler: failWith(errBoom), attempts: 1, noJob: true, boom: true,
		policy: func(d bool, _ time.Duration) workflow.RetryPolicy {
			return workflow.RetryPolicy{Max: 5, DiscardOnExhaust: d, NoRetryOn: func(err error) bool { return errors.Is(err, errBoom) }}
		},
		wantOn:  workflow.StatusDiscarded,
		wantOff: workflow.StatusFailed,
	},
	{
		name: "MaxErrors", needs: workflow.CapRetryMaxErrors, handler: failWith(errBoom), attempts: 2, boom: true,
		policy: func(d bool, _ time.Duration) workflow.RetryPolicy {
			return workflow.RetryPolicy{Max: 10, MaxErrors: 2, DiscardOnExhaust: d}
		},
		wantOn:  workflow.StatusDiscarded,
		wantOff: workflow.StatusFailed,
	},
	{
		// The backoff is longer than Until, so the first failure already
		// has no retry that could start inside the deadline.
		name: "Until", needs: workflow.CapRetryUntil, handler: failWith(errBoom), attempts: 1, boom: true,
		policy: func(d bool, unit time.Duration) workflow.RetryPolicy {
			return workflow.RetryPolicy{Max: 10, Backoff: workflow.Backoff{Initial: unit}, Until: unit / 2, DiscardOnExhaust: d}
		},
		wantOn:  workflow.StatusDiscarded,
		wantOff: workflow.StatusFailed,
	},
	{
		// Not a stop: the retry succeeds, so the option must not touch it.
		name: "a retry that succeeds", attempts: 2,
		handler: func(n *atomic.Int64, _ time.Duration) workflow.Handler {
			return func(_ context.Context, in []byte) ([]byte, error) {
				if n.Add(1) == 1 {
					return nil, errBoom
				}
				return in, nil
			}
		},
		policy: func(d bool, _ time.Duration) workflow.RetryPolicy {
			return workflow.RetryPolicy{Max: 2, DiscardOnExhaust: d}
		},
		wantOn:  workflow.StatusSucceeded,
		wantOff: workflow.StatusSucceeded,
	},
	{
		// ErrCancelled ends the attempt for good but is not an exhaustion
		// the policy may relabel: with the option on or off the run fails
		// as before.
		name: "a handler returning ErrCancelled", handler: failWith(workflow.ErrCancelled), attempts: 1,
		policy: func(d bool, _ time.Duration) workflow.RetryPolicy {
			return workflow.RetryPolicy{Max: 3, DiscardOnExhaust: d}
		},
		wantOn:  workflow.StatusFailed,
		wantOff: workflow.StatusFailed,
	},
	{
		// Not a stop by policy: an explicit Discard is discarded whatever
		// the option says, and is never retried.
		name: "an explicit Discard", needs: workflow.CapDiscard, handler: failWith(workflow.Discard(errBoom)), attempts: 1, boom: true,
		policy: func(d bool, _ time.Duration) workflow.RetryPolicy {
			return workflow.RetryPolicy{Max: 3, DiscardOnExhaust: d}
		},
		wantOn:  workflow.StatusDiscarded,
		wantOff: workflow.StatusDiscarded,
	},
}

// declLevel is where the winning policy is declared.
type declLevel string

const (
	declWorkflow declLevel = "workflow"
	declStep     declLevel = "step"
	declJob      declLevel = "job"
)

// decoyMax is the Max of the losing policies of a precedence row: unlike
// every winner's, so the attempt count names the policy that applied.
const decoyMax = 9

// exhaustRun is one built workflow of the discard matrix with its counters.
type exhaustRun struct {
	kind    exhaustKind
	level   declLevel
	discard bool
	wf      workflow.Workflow
	job     workflow.Job
	attempt atomic.Int64
	onFail  counter
	ref     workflow.RunRef
}

func (r *exhaustRun) label() string {
	return fmt.Sprintf("%s / policy at %s / DiscardOnExhaust=%t", r.kind.name, r.level, r.discard)
}

func (r *exhaustRun) want() workflow.Status {
	if r.discard {
		return r.kind.wantOn
	}
	return r.kind.wantOff
}

// eventLog collects run-finished events by run.
type eventLog struct {
	mu sync.Mutex
	ev map[workflow.RunID]workflow.RunEvent
}

func (l *eventLog) add(ev workflow.RunEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ev == nil {
		l.ev = map[workflow.RunID]workflow.RunEvent{}
	}
	l.ev[ev.Ref.RunID] = ev
}

func (l *eventLog) get(id workflow.RunID) (workflow.RunEvent, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ev, ok := l.ev[id]
	return ev, ok
}

var discardExhaustCases = func() []testCase {
	var out []testCase
	for _, kind := range exhaustKinds {
		out = append(out, testCase{"DiscardOnExhaust matrix: " + kind.name, func(e *env) { e.runExhaustKind(kind) }})
	}
	return append(out,
		testCase{"a driver without CapDiscardExhausted refuses DiscardOnExhaust on a workflow", func(e *env) {
			e.lacks(workflow.CapDiscardExhausted)
			pol := workflow.RetryPolicy{Max: 1, DiscardOnExhaust: true}
			var c counter
			onWf := oneStep(e.name("wf"), c.echo())
			onWf.Retry = pol
			assertUnsupported(e.t, e.registerAndStart(onWf), "", workflow.CapDiscardExhausted)
		}},
		testCase{"a driver without CapDiscardExhausted refuses DiscardOnExhaust on a step", func(e *env) {
			e.lacks(workflow.CapDiscardExhausted)
			var c counter
			onStep := oneStep(e.name("step"), c.echo())
			onStep.Steps[0].Retry = &workflow.RetryPolicy{Max: 1, DiscardOnExhaust: true}
			assertUnsupported(e.t, e.registerAndStart(onStep), "", workflow.CapDiscardExhausted)
		}},
		testCase{"a driver with CapJobRetry but without CapDiscardExhausted refuses it on a Job", func(e *env) {
			e.need(workflow.CapJobRetry)
			e.lacks(workflow.CapDiscardExhausted)
			var c counter
			wf := oneStep(e.name("job"), c.echo())
			e.up(wf)
			ctx, cancel := e.callCtx()
			defer cancel()
			_, err := e.c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Retry: &workflow.RetryPolicy{Max: 1, DiscardOnExhaust: true}})
			assertUnsupported(e.t, err, "Job.Retry", workflow.CapDiscardExhausted)
		}},
		testCase{"observed statuses of an exhausting run follow the transition table", func(e *env) { e.runExhaustTransitions() }},
		testCase{"a run discarded by exhaustion is not re-opened by Retry", func(e *env) {
			e.need(workflow.CapDiscardExhausted | workflow.CapRunStates | workflow.CapRetryRun)
			var n atomic.Int64
			wf := oneStep(e.name("w"), exhaustKinds[0].handler(&n, e.tm.unit))
			wf.Retry = workflow.RetryPolicy{Max: 1, DiscardOnExhaust: true}
			e.up(wf)
			ref := e.enqueue(workflow.Job{Workflow: wf.Name})
			e.waitStatus(ref, workflow.StatusDiscarded, 0)
			ctx, cancel := e.callCtx()
			defer cancel()
			if _, err := as[workflow.RunRetrier](e, "workflow.RunRetrier").Retry(ctx, ref); !errors.Is(err, workflow.ErrWrongState) {
				e.t.Fatalf("Retry of a discarded run: %v, want ErrWrongState", err)
			}
			if st := e.status(ref); st != workflow.StatusDiscarded {
				e.t.Fatalf("status after the refused Retry = %s, want discarded", st)
			}
		}},
	)
}()

// build makes the workflow and job for one cell of the discard matrix.
func (e *env) buildExhaust(kind exhaustKind, level declLevel, discard bool) *exhaustRun {
	r := &exhaustRun{kind: kind, level: level, discard: discard}
	win := kind.policy(discard, e.tm.unit)
	decoy := workflow.RetryPolicy{Max: decoyMax, DiscardOnExhaust: !discard}
	step := workflow.Step{Name: "only", Run: kind.handler(&r.attempt, e.tm.unit)}
	r.wf = workflow.Workflow{Name: e.name(fmt.Sprintf("%s-%s-%t", strings.ReplaceAll(strings.ToLower(kind.name), " ", ""), level, discard)), OnFailure: r.onFail.echo()}
	r.job = workflow.Job{Workflow: r.wf.Name}
	switch level {
	case declWorkflow:
		r.wf.Retry = win
	case declStep:
		r.wf.Retry = decoy
		step.Retry = &win
	case declJob:
		r.wf.Retry = decoy
		d := decoy
		step.Retry = &d
		r.job.Retry = &win
	}
	r.wf.Steps = []workflow.Step{step}
	return r
}

func (e *env) runExhaustKind(kind exhaustKind) {
	e.t.Helper()
	e.need(workflow.CapDiscardExhausted | workflow.CapRunStates | kind.needs)
	var events eventLog
	haveEvents := e.c.Capabilities().Has(workflow.CapRunFinished)
	if haveEvents {
		if err := as[workflow.RunNotifier](e, "workflow.RunNotifier").OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { events.add(ev) }); err != nil {
			e.t.Fatalf("OnRunFinished: %v", err)
		}
	}
	levels := []declLevel{declWorkflow, declStep}
	if !kind.noJob && e.c.Capabilities().Has(workflow.CapJobRetry) {
		levels = append(levels, declJob)
	}
	var runs []*exhaustRun
	var wfs []workflow.Workflow
	for _, level := range levels {
		for _, discard := range []bool{true, false} {
			r := e.buildExhaust(kind, level, discard)
			runs = append(runs, r)
			wfs = append(wfs, r.wf)
		}
	}
	e.up(wfs...)
	for _, r := range runs {
		r.ref = e.enqueue(r.job)
	}
	for _, r := range runs {
		if got := e.waitTerminal(r.ref, 0); got != r.want() {
			e.t.Errorf("%s: ended %s, want %s", r.label(), got, r.want())
		}
	}
	// OnFailure is asynchronous: wait for the ones that must run, then watch
	// a quiet window for the ones that must not (and for a second run).
	e.eventually("OnFailure of every failed run", 0, func() bool {
		for _, r := range runs {
			if r.want() == workflow.StatusFailed && r.onFail.count() < 1 {
				return false
			}
		}
		return true
	})
	e.quiet()
	for _, r := range runs {
		wantFail := 0
		if r.want() == workflow.StatusFailed {
			wantFail = 1
		}
		if got := r.onFail.count(); got != wantFail {
			e.t.Errorf("%s: OnFailure ran %d times, want %d", r.label(), got, wantFail)
		}
		if got := int(r.attempt.Load()); got != kind.attempts {
			e.t.Errorf("%s: %d attempts, want %d (the winning policy's, never the decoy's Max %d)", r.label(), got, kind.attempts, decoyMax)
		}
		if !haveEvents {
			continue
		}
		e.eventually("the run-finished event", 0, func() bool { _, ok := events.get(r.ref.RunID); return ok })
		ev, _ := events.get(r.ref.RunID)
		if ev.Status != r.want() {
			e.t.Errorf("%s: RunEvent.Status = %s, want %s", r.label(), ev.Status, r.want())
		}
		switch r.want() {
		case workflow.StatusDiscarded:
			if !errors.Is(ev.Err, workflow.ErrDiscarded) || !errors.Is(ev.Err, workflow.ErrCancelled) {
				e.t.Errorf("%s: RunEvent.Err = %v, want it to match ErrDiscarded and ErrCancelled", r.label(), ev.Err)
			}
		case workflow.StatusFailed:
			if ev.Err == nil || errors.Is(ev.Err, workflow.ErrDiscarded) {
				e.t.Errorf("%s: RunEvent.Err = %v, want a plain failure", r.label(), ev.Err)
			}
		case workflow.StatusSucceeded:
			if ev.Err != nil {
				e.t.Errorf("%s: RunEvent.Err = %v, want nil", r.label(), ev.Err)
			}
		}
		if kind.boom && r.want() != workflow.StatusSucceeded && (ev.Err == nil || !strings.Contains(ev.Err.Error(), errBoom.Error())) {
			e.t.Errorf("%s: RunEvent.Err = %v, want the step's own error kept in it", r.label(), ev.Err)
		}
	}
}

// runExhaustTransitions polls a run through retrying to discarded and
// checks every observed move against workflow.Status.CanTransitionTo.
func (e *env) runExhaustTransitions() {
	e.t.Helper()
	e.need(workflow.CapDiscardExhausted | workflow.CapRunStates)
	var n atomic.Int64
	wf := oneStep(e.name("w"), exhaustKinds[0].handler(&n, e.tm.unit))
	wf.Retry = workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: e.tm.unit / 4}, DiscardOnExhaust: true}
	e.up(wf)
	ref := e.enqueue(workflow.Job{Workflow: wf.Name})
	var seen []workflow.Status
	e.eventually("a terminal status", e.tm.unit/4, func() bool {
		st := e.status(ref)
		if len(seen) == 0 || seen[len(seen)-1] != st {
			seen = append(seen, st)
		}
		return st.IsTerminal()
	})
	// Polling samples the run, so a move can hide an intermediate status
	// (retrying -> running -> discarded is seen as retrying -> discarded):
	// each observed pair must be reachable through the table, not adjacent.
	for i := 1; i < len(seen); i++ {
		if !reachable(seen[i-1], seen[i]) {
			e.t.Errorf("observed %s -> %s, which no path of the transition table allows (sequence %v)", seen[i-1], seen[i], seen)
		}
	}
	if last := seen[len(seen)-1]; last != workflow.StatusDiscarded {
		e.t.Fatalf("ended %s, want discarded (sequence %v)", last, seen)
	}
	e.quiet()
	if st := e.status(ref); st != workflow.StatusDiscarded {
		e.t.Fatalf("a terminal discarded run moved to %s", st)
	}
	if got := n.Load(); got != 3 {
		e.t.Fatalf("%d attempts under Max 2, want 3", got)
	}
}

// reachable reports whether to can be reached from from by one or more moves
// of the transition table.
func reachable(from, to workflow.Status) bool {
	seen := map[workflow.Status]bool{}
	queue := []workflow.Status{from}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, t := range cur.TransitionsFrom() {
			if t.To == to {
				return true
			}
			if !seen[t.To] {
				seen[t.To] = true
				queue = append(queue, t.To)
			}
		}
	}
	return false
}
