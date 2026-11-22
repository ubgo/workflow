package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// TriggerRule says when a step whose parents may have failed runs (WP-09,
// CM-13): the join of a graph whose failing steps are tolerated
// (RulesSpec.Tolerate) rather than fatal. A step whose rule does not hold
// does not call its handler; it ends with a skip record, and its own
// children see it as skipped.
type TriggerRule string

const (
	// RuleAllSucceeded runs the step only when every parent succeeded
	// (the default for a child of a tolerated or ruled step). A failed or
	// skipped parent skips it.
	//ds:def id=ruleallsucceeded-rb23vtac owner=@khanakia stability=stable
	RuleAllSucceeded TriggerRule = "all_succeeded"

	// RuleNoneFailed runs the step when no parent failed; skipped parents
	// count as fine (the continue policy).
	//ds:def id=rulenonefailed-sw2t9ffg owner=@khanakia stability=stable
	RuleNoneFailed TriggerRule = "none_failed"

	// RuleAllDone runs the step whatever its parents did (succeeded,
	// failed or skipped): the always rule.
	//ds:def id=rulealldone-apc5tzgu owner=@khanakia stability=stable
	RuleAllDone TriggerRule = "all_done"

	// RuleOneFailed runs the step when at least one parent failed, of any
	// kind: an error-handling branch.
	//ds:def id=ruleonefailed-epgevkfz owner=@khanakia stability=stable
	RuleOneFailed TriggerRule = "one_failed"

	// RuleAllFailed runs the step only when every parent failed.
	//ds:def id=ruleallfailed-rq28tyzz owner=@khanakia stability=stable
	RuleAllFailed TriggerRule = "all_failed"

	// RuleOneSucceeded runs the step when at least one parent succeeded.
	//ds:def id=ruleonesucceeded-7hffk2d3 owner=@khanakia stability=stable
	RuleOneSucceeded TriggerRule = "one_succeeded"
)

// ruleFailedPrefix starts a RuleOneFailedOf rule.
const ruleFailedPrefix = "one_failed:"

// RuleOneFailedOf runs the step when at least one parent failed with this
// kind (WP-01, error routing): the kind a CatchRoute gave the failure, or
// FailureTimeout, FailureError.
//
//ds:def id=ruleonefailedof-fsnzz3ss owner=@khanakia stability=stable
func RuleOneFailedOf(kind string) TriggerRule { return TriggerRule(ruleFailedPrefix + kind) }

// Failure kinds a tolerated step records when no CatchRoute matched.
const (
	// FailureTimeout is the kind of a failure matching ErrTimeout (a step,
	// wait or heartbeat timeout).
	//ds:def id=failuretimeout-tuhban6c owner=@khanakia stability=stable
	FailureTimeout = "timeout"
	// FailureError is the kind of every other failure.
	//ds:def id=failureerror-uhnjkhy8 owner=@khanakia stability=stable
	FailureError = "error"
)

// CatchRoute names a kind of failure of a tolerated step (WP-01): the
// first route whose Match returns true gives the failure its Kind, which
// RuleOneFailedOf routes on. Match runs in the failing attempt, on the
// real error, so errors.Is and errors.As work.
//
//ds:def id=catchroute-f563zdkd owner=@khanakia stability=stable
type CatchRoute struct {
	// Kind is the name the failure gets; not empty.
	Kind string
	// Match reports whether err is of this kind.
	Match func(err error) bool
}

// StepFailure is what a tolerated step recorded when its last attempt
// failed, as a step handler with a rule reads it (ParentFailures).
//
//ds:def id=stepfailure-xq2xnjvt owner=@khanakia stability=stable
type StepFailure struct {
	// Step is the step that failed.
	Step string `json:"step"`
	// Kind is the CatchRoute kind, FailureTimeout or FailureError.
	Kind string `json:"kind"`
	// Error is the failure's message (the error itself does not survive
	// the trip between steps).
	Error string `json:"error"`
}

// RulesSpec turns some steps of a workflow into tolerated steps and gives
// others a trigger rule; WithRules applies it.
//
//ds:def id=rulesspec-n7pbvchr owner=@khanakia stability=stable
type RulesSpec struct {
	// Tolerate lists Run steps whose final failure is recorded instead of
	// failing the run. A failure is final when the step's policy (the
	// step's Retry, else the workflow's Retry) would not retry it, or it
	// is Permanent. A cancelled, discarded or snoozed attempt is passed
	// through untouched.
	Tolerate []string

	// Catch gives the failures of a tolerated step their kinds, tried in
	// order; keyed by step name.
	Catch map[string][]CatchRoute

	// Rules gives a step its trigger rule. A child of a tolerated or ruled
	// step without an entry gets RuleAllSucceeded, so a failure or a skip
	// propagates as a skip instead of reaching a handler as data.
	Rules map[string]TriggerRule

	// Finally lists exit handlers (WP-11): steps that run once every
	// parent is done whatever happened (RuleAllDone), and then fail the
	// run when any parent, or any step a skipped parent stood for, failed.
	// A Finally step with no After is placed after every leaf of the
	// workflow, and every other Run step is tolerated so nothing can stop
	// the handler from running; the run still ends failed when a step
	// failed, so its status stays honest.
	Finally []string
}

// ruleRecord is the output a tolerated or ruled step leaves for its
// children when it did not produce a real output: a failure, or a skip
// carrying the failures upstream of it. The keys sit under the reserved
// internal prefix so no real output is mistaken for a record.
type ruleRecord struct {
	Failed   *StepFailure  `json:"ubgo.workflow/failed,omitempty"`
	Skipped  bool          `json:"ubgo.workflow/skipped,omitempty"`
	Upstream []StepFailure `json:"ubgo.workflow/upstream,omitempty"`
}

// parseRuleRecord reports whether out is a record and decodes it.
func parseRuleRecord(out []byte) (ruleRecord, bool) {
	if len(out) == 0 || out[0] != '{' || !strings.Contains(string(out), `"ubgo.workflow/`) {
		return ruleRecord{}, false
	}
	var r ruleRecord
	if err := json.Unmarshal(out, &r); err != nil || (r.Failed == nil && !r.Skipped) {
		return ruleRecord{}, false
	}
	return r, true
}

// parentFailuresKey carries the failures a ruled step's parents recorded.
type parentFailuresKey struct{}

// ParentFailures returns the failures the parents of a step with a
// trigger rule recorded, directly or through a skipped parent, in parent
// order; nil inside any other step.
//
//ds:def id=parentfailures-8u89n2hu owner=@khanakia stability=stable
func ParentFailures(ctx context.Context) []StepFailure {
	f, _ := ctx.Value(parentFailuresKey{}).([]StepFailure)
	return f
}

// WithRules returns a copy of wf with spec applied: tolerated steps record
// their final failure, ruled steps run by their rule, Finally steps run
// last. Everything is ordinary step code, so it runs on every driver with
// no capability. It returns an error wrapping ErrInvalid when a name is
// not a step of wf, a listed step is not a Run step, or a rule is unknown.
//
// What a ruled step receives: the usual StepInput (one parent's output, or
// an object keyed by parent), where a failed or skipped parent's output is
// null; ParentFailures lists what failed. The run's status: a run whose
// tolerated steps failed and that has no Finally ends succeeded, since
// the failure was handled; add a Finally step to keep it failed.
//
// Pitfall: the last attempt is judged from the step's or workflow's
// policy. A per-dispatch Job.Retry is not visible to the step, so a job
// that overrides the policy should not use tolerated steps.
//
//ds:def id=withrules-q3mcee45 owner=@khanakia stability=stable
func WithRules(wf Workflow, spec RulesSpec) (Workflow, error) {
	steps := slices.Clone(wf.Steps)
	index := map[string]int{}
	for i, s := range steps {
		index[s.Name] = i
	}
	tolerated := map[string]bool{}
	rules := map[string]TriggerRule{}
	finally := map[string]bool{}
	need := func(name, what string) (int, error) {
		i, ok := index[name]
		if !ok {
			return 0, invalidf("rules: %s %q is not a step of workflow %q", what, name, wf.Name)
		}
		if steps[i].Run == nil {
			return 0, invalidf("rules: %s %q is not a Run step", what, name)
		}
		return i, nil
	}
	for _, name := range spec.Tolerate {
		if _, err := need(name, "tolerated step"); err != nil {
			return wf, err
		}
		tolerated[name] = true
	}
	for name, routes := range spec.Catch {
		if !tolerated[name] && !slices.Contains(spec.Finally, name) {
			if _, err := need(name, "step with Catch routes"); err != nil {
				return wf, err
			}
			tolerated[name] = true
		}
		for _, r := range routes {
			if r.Kind == "" || r.Match == nil {
				return wf, invalidf("rules: a Catch route of step %q needs a Kind and a Match", name)
			}
		}
	}
	for name, r := range spec.Rules {
		if _, err := need(name, "ruled step"); err != nil {
			return wf, err
		}
		if !validRule(r) {
			return wf, invalidf("rules: step %q has unknown rule %q", name, r)
		}
		rules[name] = r
	}
	for _, name := range spec.Finally {
		i, err := need(name, "Finally step")
		if err != nil {
			return wf, err
		}
		finally[name] = true
		rules[name] = RuleAllDone
		if len(steps[i].After) == 0 {
			steps[i].After = leavesExcept(steps, name)
		}
	}
	if len(finally) > 0 {
		for _, s := range steps {
			if s.Run != nil && !finally[s.Name] {
				tolerated[s.Name] = true
			}
		}
	}
	// A child of a tolerated or ruled step gets RuleAllSucceeded unless it
	// has a rule, repeated until no new step joins, so a skip reaches every
	// step below it.
	for changed := true; changed; {
		changed = false
		for _, s := range steps {
			if _, ok := rules[s.Name]; ok || s.Run == nil {
				continue
			}
			for _, p := range s.After {
				if _, ruled := rules[p]; tolerated[p] || ruled {
					rules[s.Name] = RuleAllSucceeded
					changed = true
					break
				}
			}
		}
	}
	for _, s := range steps {
		for _, p := range s.After {
			if _, ruled := rules[p]; (tolerated[p] || ruled) && s.Run == nil {
				return wf, invalidf("rules: step %q waits on %q, which may fail or skip, and is not a Run step", s.Name, p)
			}
		}
	}
	for i, s := range steps {
		run := s.Run
		if r, ok := rules[s.Name]; ok {
			run = ruledHandler(s.Name, s.After, r, finally[s.Name], run)
		}
		if tolerated[s.Name] {
			policy := s.Retry
			if policy == nil {
				policy = &wf.Retry
			}
			run = toleratedHandler(s.Name, *policy, spec.Catch[s.Name], run)
		}
		steps[i].Run = run
	}
	wf.Steps = steps
	return wf, nil
}

// validRule reports whether r is one of the rules above.
func validRule(r TriggerRule) bool {
	switch r {
	case RuleAllSucceeded, RuleNoneFailed, RuleAllDone, RuleOneFailed, RuleAllFailed, RuleOneSucceeded:
		return true
	}
	return strings.HasPrefix(string(r), ruleFailedPrefix) && len(r) > len(ruleFailedPrefix)
}

// leavesExcept returns the steps no other step waits on, but skip.
//
//ds:def id=leavesexcept-2vzduexk owner=@khanakia stability=stable
func leavesExcept(steps []Step, skip string) []string {
	hasChild := map[string]bool{}
	for _, s := range steps {
		for _, p := range s.After {
			hasChild[p] = true
		}
	}
	var out []string
	for _, s := range steps {
		if s.Name != skip && !hasChild[s.Name] {
			out = append(out, s.Name)
		}
	}
	return out
}

// toleratedHandler runs next and turns its final failure into a record.
// An attempt cut off by its own Step.Timeout or HeartbeatTimeout fails
// with that timeout whatever next returned (the handler itself only sees
// a cancelled ctx), so it is recorded as a FailureTimeout, and the record
// claims the timeout (ClaimTimeout) so the driver keeps it instead of
// failing the attempt.
//
//ds:def id=toleratedhandler-xnm5kays owner=@khanakia stability=stable
func toleratedHandler(step string, policy RetryPolicy, routes []CatchRoute, next Handler) Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		out, err := next(ctx, in)
		if te := attemptTimeout(ctx); te != nil && !IsTimeout(err) {
			if err == nil {
				err = te
			} else {
				err = fmt.Errorf("%w: %w", te, err)
			}
		}
		if err == nil || IsDiscarded(err) || errors.Is(err, ErrCancelled) || errors.Is(err, ErrSnoozed) {
			return out, err
		}
		attempt := 1
		if info, ok := StepInfoFrom(ctx); ok && info.Attempt > 0 {
			attempt = info.Attempt
		}
		if !IsPermanent(err) && policy.Retryable(err, attempt) {
			return out, err
		}
		kind := FailureError
		if IsTimeout(err) {
			kind = FailureTimeout
		}
		for _, r := range routes {
			if r.Match(err) {
				kind = r.Kind
				break
			}
		}
		rec, rerr := encodeJSON("tolerated step "+step, ruleRecord{Failed: &StepFailure{Step: step, Kind: kind, Error: err.Error()}})
		if rerr == nil {
			ClaimTimeout(ctx) // a no-op unless the attempt timed out
		}
		return rec, rerr
	}
}

// ruledHandler applies rule to the parents' records, then runs next with
// failed and skipped parents nulled, or ends with a skip record.
//
//ds:def id=ruledhandler-sxsw3vmv owner=@khanakia stability=stable
func ruledHandler(step string, parents []string, rule TriggerRule, isFinally bool, next Handler) Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		outs := map[string][]byte{}
		switch len(parents) {
		case 0:
		case 1:
			outs[parents[0]] = in
		default:
			raw := map[string]json.RawMessage{}
			if err := json.Unmarshal(in, &raw); err != nil {
				return nil, Permanent(fmt.Errorf("workflow: rules: step %q: input does not decode: %w", step, err))
			}
			for k, v := range raw {
				if string(v) != "null" {
					outs[k] = v
				}
			}
		}
		var failures []StepFailure
		failed, skipped, succeeded := 0, 0, 0
		matchedKind := false
		for _, p := range parents {
			rec, ok := parseRuleRecord(outs[p])
			switch {
			case !ok:
				succeeded++
				continue
			case rec.Failed != nil:
				failed++
				failures = append(failures, *rec.Failed)
				if strings.HasPrefix(string(rule), ruleFailedPrefix) && rec.Failed.Kind == string(rule)[len(ruleFailedPrefix):] {
					matchedKind = true
				}
			default:
				skipped++
				failures = append(failures, rec.Upstream...)
			}
			delete(outs, p)
		}
		n := len(parents)
		var run bool
		switch rule {
		case RuleAllSucceeded:
			run = succeeded == n
		case RuleNoneFailed:
			run = failed == 0 && len(failures) == 0
		case RuleAllDone:
			run = true
		case RuleOneFailed:
			run = failed > 0
		case RuleAllFailed:
			run = n > 0 && failed == n
		case RuleOneSucceeded:
			run = succeeded > 0
		default:
			run = matchedKind
		}
		if !run {
			return encodeJSON("rules step "+step, ruleRecord{Skipped: true, Upstream: failures})
		}
		out, err := next(context.WithValue(ctx, parentFailuresKey{}, failures), StepInput(nil, parents, outs))
		if err != nil || !isFinally || len(failures) == 0 {
			return out, err
		}
		msgs := make([]string, len(failures))
		for i, f := range failures {
			msgs[i] = fmt.Sprintf("step %q: %s", f.Step, f.Error)
		}
		return out, Permanent(fmt.Errorf("%w: %s", ErrStepsFailed, strings.Join(msgs, "; ")))
	}
}

// ErrStepsFailed is the error a Finally step ends its run with after its
// handler ran, when a step before it failed.
//
//ds:def id=errstepsfailed-q4yb27zg owner=@khanakia stability=stable
var ErrStepsFailed = errors.New("workflow: steps failed before the exit handler")
