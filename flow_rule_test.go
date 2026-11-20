package workflow_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// ruleClient starts a memory client with wfs registered.
func ruleClient(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
	t.Helper()
	c := memory.New()
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

// seen records which steps ran and what each received.
type seen struct {
	mu   sync.Mutex
	in   map[string]string
	fail map[string][]workflow.StepFailure
}

func (s *seen) step(name string, out string, err error) workflow.Step {
	return workflow.Step{Name: name, Run: func(ctx context.Context, in []byte) ([]byte, error) {
		s.mu.Lock()
		if s.in == nil {
			s.in, s.fail = map[string]string{}, map[string][]workflow.StepFailure{}
		}
		s.in[name] = string(in)
		s.fail[name] = workflow.ParentFailures(ctx)
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return []byte(out), nil
	}}
}

func (s *seen) ran(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.in[name]
	return ok
}

func after(s workflow.Step, parents ...string) workflow.Step { s.After = parents; return s }

var errDeclined = errors.New("card declined")

func TestRulesToleratedFailureRoutesToHandlerBranch(t *testing.T) {
	var s seen
	wf, err := workflow.WithRules(workflow.Workflow{Name: "pay", Steps: []workflow.Step{
		s.step("charge", "", workflow.Permanent(errDeclined)),
		s.step("audit", `"audited"`, nil),
		after(s.step("ship", `"shipped"`, nil), "charge"),
		after(s.step("notify-declined", `"mailed"`, nil), "charge"),
		after(s.step("on-timeout", `"x"`, nil), "charge"),
		after(s.step("report", `"r"`, nil), "audit", "charge"),
		after(s.step("after-ship", `"y"`, nil), "ship"),
	}}, workflow.RulesSpec{
		Tolerate: []string{"charge"},
		Catch:    map[string][]workflow.CatchRoute{"charge": {{Kind: "declined", Match: func(err error) bool { return errors.Is(err, errDeclined) }}}},
		Rules: map[string]workflow.TriggerRule{
			"notify-declined": workflow.RuleOneFailedOf("declined"),
			"on-timeout":      workflow.RuleOneFailedOf(workflow.FailureTimeout),
			"report":          workflow.RuleAllDone,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := ruleClient(t, wf)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "pay"})); st != workflow.StatusSucceeded {
		t.Fatalf("status %s, want succeeded (the failure was handled)", st)
	}
	for name, want := range map[string]bool{"ship": false, "after-ship": false, "notify-declined": true, "on-timeout": false, "report": true} {
		if got := s.ran(name); got != want {
			t.Errorf("%s ran = %v, want %v", name, got, want)
		}
	}
	if f := s.fail["notify-declined"]; len(f) != 1 || f[0].Step != "charge" || f[0].Kind != "declined" || !strings.Contains(f[0].Error, "card declined") {
		t.Fatalf("ParentFailures = %+v", f)
	}
	// The ruled join sees the failed parent as null and the good one as is.
	if got := s.in["report"]; got != `{"audit":"audited","charge":null}` {
		t.Fatalf("report input %s", got)
	}
}

func TestRulesNoneFailedAndOneSucceeded(t *testing.T) {
	var s seen
	wf, err := workflow.WithRules(workflow.Workflow{Name: "fan", Steps: []workflow.Step{
		s.step("a", `1`, nil),
		s.step("b", "", workflow.Permanent(errors.New("b broke"))),
		after(s.step("none-failed", `"n"`, nil), "a", "b"),
		after(s.step("one-ok", `"o"`, nil), "a", "b"),
		after(s.step("all-failed", `"f"`, nil), "a", "b"),
	}}, workflow.RulesSpec{
		Tolerate: []string{"a", "b"},
		Rules: map[string]workflow.TriggerRule{
			"none-failed": workflow.RuleNoneFailed,
			"one-ok":      workflow.RuleOneSucceeded,
			"all-failed":  workflow.RuleAllFailed,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := ruleClient(t, wf)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "fan"})); st != workflow.StatusSucceeded {
		t.Fatalf("status %s", st)
	}
	if s.ran("none-failed") || !s.ran("one-ok") || s.ran("all-failed") {
		t.Fatalf("none-failed %v one-ok %v all-failed %v", s.ran("none-failed"), s.ran("one-ok"), s.ran("all-failed"))
	}
	if got := s.in["one-ok"]; got != `{"a":1,"b":null}` {
		t.Fatalf("one-ok input %s", got)
	}
}

func TestRulesNoneFailedSeesFailuresThroughSkips(t *testing.T) {
	var s seen
	wf, err := workflow.WithRules(workflow.Workflow{Name: "chain", Steps: []workflow.Step{
		s.step("a", "", workflow.Permanent(errors.New("a broke"))),
		after(s.step("b", `"b"`, nil), "a"),
		after(s.step("c", `"c"`, nil), "b"),
		after(s.step("d", `"d"`, nil), "b"),
	}}, workflow.RulesSpec{
		Tolerate: []string{"a"},
		Rules:    map[string]workflow.TriggerRule{"c": workflow.RuleNoneFailed, "d": workflow.RuleAllDone},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := ruleClient(t, wf)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "chain"})); st != workflow.StatusSucceeded {
		t.Fatalf("status %s", st)
	}
	if s.ran("b") || s.ran("c") || !s.ran("d") {
		t.Fatalf("b %v c %v d %v: a skip caused by a failure must count as failed for RuleNoneFailed", s.ran("b"), s.ran("c"), s.ran("d"))
	}
	if f := s.fail["d"]; len(f) != 1 || f[0].Step != "a" {
		t.Fatalf("d saw %+v", f)
	}
}

func TestRulesToleratedStepStillRetries(t *testing.T) {
	var calls atomic.Int32
	var s seen
	flaky := workflow.Step{Name: "flaky", Retry: &workflow.RetryPolicy{Max: 2}, Run: func(context.Context, []byte) ([]byte, error) {
		if calls.Add(1) < 3 {
			return nil, errors.New("try again")
		}
		return []byte(`"ok"`), nil
	}}
	wf, err := workflow.WithRules(workflow.Workflow{Name: "retry", Steps: []workflow.Step{
		flaky, after(s.step("next", `"n"`, nil), "flaky"),
	}}, workflow.RulesSpec{Tolerate: []string{"flaky"}})
	if err != nil {
		t.Fatal(err)
	}
	c := ruleClient(t, wf)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "retry"})); st != workflow.StatusSucceeded {
		t.Fatalf("status %s", st)
	}
	if calls.Load() != 3 || s.in["next"] != `"ok"` {
		t.Fatalf("calls %d, next got %q: a tolerated step must use its retries before recording a failure", calls.Load(), s.in["next"])
	}
}

func TestRulesFinallyRunsAndKeepsTheRunFailed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		loadErr error
		want    workflow.Status
	}{
		{"a step failed", errors.New("load broke"), workflow.StatusFailed},
		{"all succeeded", nil, workflow.StatusSucceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s seen
			wf, err := workflow.WithRules(workflow.Workflow{Name: "job", Steps: []workflow.Step{
				s.step("load", `"rows"`, tc.loadErr),
				after(s.step("transform", `"t"`, nil), "load"),
				after(s.step("save", `"s"`, nil), "transform"),
				s.step("cleanup", `"clean"`, nil),
			}}, workflow.RulesSpec{Finally: []string{"cleanup"}})
			if err != nil {
				t.Fatal(err)
			}
			c := ruleClient(t, wf)
			ref := enqueue(t, c, workflow.Job{Workflow: "job"})
			if st := waitRun(t, c, ref); st != tc.want {
				t.Fatalf("status %s, want %s", st, tc.want)
			}
			if !s.ran("cleanup") {
				t.Fatal("the exit handler did not run")
			}
			if tc.loadErr != nil {
				if s.ran("transform") || s.ran("save") {
					t.Fatal("steps after the failure ran")
				}
				if f := s.fail["cleanup"]; len(f) != 1 || f[0].Step != "load" {
					t.Fatalf("cleanup saw failures %+v, want load's through the skipped save", f)
				}
			}
		})
	}
}

func TestRulesRefusals(t *testing.T) {
	base := workflow.Workflow{Name: "w", Steps: []workflow.Step{
		{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
		{Name: "wait", After: []string{"a"}, Wait: &workflow.WaitSpec{Event: "e"}},
	}}
	for name, spec := range map[string]workflow.RulesSpec{
		"unknown step":        {Tolerate: []string{"nope"}},
		"not a run step":      {Tolerate: []string{"wait"}},
		"unknown rule":        {Rules: map[string]workflow.TriggerRule{"a": "sometimes"}},
		"wait below tolerate": {Tolerate: []string{"a"}},
		"empty catch route":   {Catch: map[string][]workflow.CatchRoute{"a": {{Kind: ""}}}},
	} {
		if _, err := workflow.WithRules(base, spec); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: err %v, want ErrInvalid", name, err)
		}
	}
}
