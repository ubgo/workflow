package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

// scoped is a one-step workflow named name whose only limit names scope.
func scoped(name, scope string, max int, hold workflow.ConcurrencyHold) workflow.Workflow {
	return workflow.Workflow{
		Name:  name,
		Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
		ConcurrencyLimits: []workflow.ConcurrencyLimit{{
			Key: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["account"] }), MaxRuns: max, Scope: scope, Hold: hold,
		}},
	}
}

func TestCheckConcurrencyScopes(t *testing.T) {
	sync := scoped("sync", "account", 1, "")
	for _, tc := range []struct {
		name  string
		wf    workflow.Workflow
		words []string // nil: accepted
	}{
		{"same settings", scoped("check", "account", 1, ""), nil},
		{"the zero Hold is HoldWholeRun", scoped("check", "account", 1, workflow.HoldWholeRun), nil},
		{"another scope", scoped("check", "region", 2, workflow.HoldWhileRunning), nil},
		{"no scope", scoped("check", "", 2, ""), nil},
		{"the same workflow again replaces it", scoped("sync", "account", 3, ""), nil},
		{"different MaxRuns", scoped("check", "account", 2, ""), []string{`"check"`, `"sync"`, `"account"`, "MaxRuns 2", "MaxRuns 1"}},
		{"different Hold", scoped("check", "account", 1, workflow.HoldWhileRunning), []string{`"check"`, `"sync"`, "Hold", "while_running", "whole_run"}},
	} {
		err := workflow.CheckConcurrencyScopes(tc.wf, []workflow.Workflow{sync})
		if tc.words == nil {
			if err != nil {
				t.Errorf("%s: %v, want nil", tc.name, err)
			}
			continue
		}
		if !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", tc.name, err)
			continue
		}
		for _, w := range tc.words {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: %v, want it to name %s", tc.name, err, w)
			}
		}
	}
}

func TestConcurrencyScopeRequiresAndValidate(t *testing.T) {
	wf := scoped("sync", "account", 1, "")
	if !workflow.ConcurrencyScoped(wf) {
		t.Error("ConcurrencyScoped = false for a limit with a Scope")
	}
	if !workflow.Requires(wf).Has(workflow.CapsOf(workflow.CapConcurrencyLimits, workflow.CapConcurrencyScope)) {
		t.Errorf("Requires = %s, want ConcurrencyLimits and ConcurrencyScope", workflow.Requires(wf))
	}
	plain := scoped("sync", "", 1, "")
	if workflow.ConcurrencyScoped(plain) || workflow.Requires(plain).Has(workflow.CapConcurrencyScope) {
		t.Errorf("a limit without a Scope: scoped %t, Requires %s; want neither", workflow.ConcurrencyScoped(plain), workflow.Requires(plain))
	}
	twice := scoped("sync", "account", 1, "")
	twice.ConcurrencyLimits = append(twice.ConcurrencyLimits, workflow.ConcurrencyLimit{Key: twice.ConcurrencyLimits[0].Key, MaxRuns: 1, Scope: "account"})
	if err := workflow.Validate(twice); !errors.Is(err, workflow.ErrInvalid) || !strings.Contains(err.Error(), `"account"`) {
		t.Errorf("Validate of a scope named twice = %v, want ErrInvalid naming it", err)
	}
	if got := workflow.CapConcurrencyScope.String(); got != "ConcurrencyScope" {
		t.Errorf("CapConcurrencyScope.String() = %q", got)
	}
}
