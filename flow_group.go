package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// GroupSeparator joins a group's name and a step's name ("import/fetch").
const GroupSeparator = "/"

// Group returns steps as one named group (WP-15): each step is renamed
// "<name>/<step>", its After inside the group renamed with it, the steps
// with no parent inside the group wait on after, and a join step named
// name waits on the group's last steps and carries their output (one last
// step: its output; several: a JSON object keyed by their names inside the
// group). Later steps put the group's name in After as they would a step.
// The group is ordinary steps, so it runs on every driver; retries and
// timeouts stay per step. A whole sub-workflow with its own retries and
// run is a child run instead (Spawn and AwaitChild).
//
// It returns an error wrapping ErrInvalid when name is empty or contains
// GroupSeparator, a step name repeats, or an After inside the group names
// no step of the group.
func Group(name string, after []string, steps []Step) ([]Step, error) {
	if name == "" || strings.Contains(name, GroupSeparator) || len(steps) == 0 {
		return nil, invalidf("group %q needs a name without %q and at least one step", name, GroupSeparator)
	}
	inner := map[string]bool{}
	for _, s := range steps {
		if s.Name == "" || inner[s.Name] {
			return nil, invalidf("group %q: step name %q is empty or repeats", name, s.Name)
		}
		inner[s.Name] = true
	}
	hasChild := map[string]bool{}
	out := make([]Step, 0, len(steps)+1)
	for _, s := range steps {
		s.Name = name + GroupSeparator + s.Name
		if len(s.After) == 0 {
			s.After = slices.Clone(after)
		} else {
			parents := make([]string, len(s.After))
			for i, p := range s.After {
				if !inner[p] {
					return nil, invalidf("group %q: step %q waits on %q, which is not in the group", name, s.Name, p)
				}
				hasChild[p] = true
				parents[i] = name + GroupSeparator + p
			}
			s.After = parents
		}
		out = append(out, s)
	}
	var last, lastInner []string
	for _, s := range steps {
		if !hasChild[s.Name] {
			last = append(last, name+GroupSeparator+s.Name)
			lastInner = append(lastInner, s.Name)
		}
	}
	join := func(_ context.Context, in []byte) ([]byte, error) {
		if len(last) == 1 {
			return in, nil
		}
		raw := map[string]json.RawMessage{}
		if err := json.Unmarshal(in, &raw); err != nil {
			return nil, Permanent(fmt.Errorf("workflow: group %q: join input does not decode: %w", name, err))
		}
		vals := make(map[string][]byte, len(raw))
		for i, l := range last {
			if v := raw[l]; string(v) != "null" {
				vals[lastInner[i]] = v
			}
		}
		return StepInput(nil, lastInner, vals), nil
	}
	return append(out, Step{Name: name, After: last, Run: join}), nil
}

// LabelDepth is the nesting depth RecursionLimit writes on a job enqueued
// from inside a step: one more than the depth of the run whose step
// enqueued it (a run enqueued from outside any step has depth 0).
const LabelDepth = LabelPrefixInternal + "depth"

// ErrDepth matches the refusal of RecursionLimit: the job would nest
// deeper than the limit.
var ErrDepth = errors.New("workflow: nesting depth limit reached")

// recursionPlugin is RecursionLimit.
type recursionPlugin struct{ max int }

// RecursionLimit returns a plugin that bounds recursion (WC-22): a step
// may enqueue its own workflow (or any other), directly or with Spawn, and
// each job enqueued from inside a step gets LabelDepth one deeper than its
// parent run's; a job past max is refused with an error matching ErrDepth
// and ErrInvalid, so a runaway recursion stops instead of filling the
// queue. A job enqueued outside a step (no StepInfo on the ctx) is depth
// 0. It is enqueue middleware, so it runs on every driver; a job enqueued
// through a client without the plugin carries no depth.
func RecursionLimit(max int) Plugin { return recursionPlugin{max: max} }

func (p recursionPlugin) Name() string { return "recursion-limit" }

func (p recursionPlugin) EnqueueMiddleware() []EnqueueMiddleware {
	return []EnqueueMiddleware{func(next EnqueueFunc) EnqueueFunc {
		return func(ctx context.Context, job Job) (RunRef, error) {
			info, ok := StepInfoFrom(ctx)
			if !ok {
				return next(ctx, job)
			}
			depth := 1
			if d, err := strconv.Atoi(info.Labels[LabelDepth]); err == nil {
				depth = d + 1
			}
			if depth > p.max {
				return RunRef{}, fmt.Errorf("%w: %w: job of %q would be at depth %d, over %d (enqueued by run %s, step %q)", ErrInvalid, ErrDepth, job.Workflow, depth, p.max, info.RunID, info.Step)
			}
			job.Labels = maps.Clone(job.Labels)
			if job.Labels == nil {
				job.Labels = map[string]string{}
			}
			job.Labels[LabelDepth] = strconv.Itoa(depth)
			return next(AllowReservedLabels(ctx, LabelDepth), job)
		}
	}}
}

// endRunData is the run data name EndRun sets.
const endRunData = "ubgo.workflow/ended"

// EndRun ends the run early and successfully (WC-43): every step that has
// not started yet is skipped (it returns a nil output without running),
// so the run finishes as soon as the steps already running return. Call it
// from a step, then return that step's output as usual. It needs the
// Journal plugin (it writes run data) and the EndRuns plugin, which does
// the skipping; both run on every driver. Steps already running are not
// cancelled: an explicit end is a decision, not an abort (Cancel is the
// abort). A step skipped this way gives its children empty input, which
// they skip too.
func EndRun(ctx context.Context) error { return SetRunData(ctx, endRunData, true) }

// endRunsPlugin is EndRuns.
type endRunsPlugin struct{}

// EndRuns returns the plugin that honours EndRun: step middleware that
// skips a step of a run that has ended. Pass it to Use after Journal, so
// the journal is on the ctx when it looks.
func EndRuns() Plugin { return endRunsPlugin{} }

func (endRunsPlugin) Name() string { return "end-runs" }

func (endRunsPlugin) StepMiddleware() []Middleware {
	return []Middleware{func(next Handler) Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if _, err := journalFrom(ctx); err != nil {
				return next(ctx, in)
			}
			ended, _, err := RunData[bool](ctx, endRunData)
			if err != nil {
				return nil, err
			}
			if ended {
				return nil, nil
			}
			return next(ctx, in)
		}
	}}
}

// Boundary returns step middleware for a non-interrupting timer on a step
// (WP-20): when the step is still running after d, onTimer is called once
// (in its own goroutine, with the step's ctx and StepInfo) while the step
// carries on: escalate, page someone, send a reminder. The interrupting
// form is Step.Timeout, with WithRules to send the timeout to a branch
// (RuleOneFailedOf(FailureTimeout)) on a driver with CapRunTimeout, which
// cancels the attempt with ErrStepTimeout for the tolerated step to record
// (ClaimTimeout); elsewhere return TimeoutError from a deadline inside the
// step. steps limits it to those step names; none means every step. The
// timer is the step's own: it starts per attempt and stops when the
// attempt returns.
func Boundary(d time.Duration, onTimer func(ctx context.Context, info StepInfo), steps ...string) Middleware {
	return func(next Handler) Handler {
		if d <= 0 || onTimer == nil {
			return next
		}
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := StepInfoFrom(ctx)
			if len(steps) > 0 && !slices.Contains(steps, info.Step) {
				return next(ctx, in)
			}
			t := time.AfterFunc(d, func() { onTimer(ctx, info) })
			defer t.Stop()
			return next(ctx, in)
		}
	}
}
