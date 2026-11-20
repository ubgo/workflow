package workflowtest_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestGroupNames: every group has a distinct, non-empty name, and the
// list holds groups from each part of the suite.
func TestGroupNames(t *testing.T) {
	names := workflowtest.GroupNames()
	seen := map[string]bool{}
	for _, n := range names {
		if n == "" || seen[n] {
			t.Fatalf("group name %q is empty or repeated in %v", n, names)
		}
		seen[n] = true
	}
	for _, want := range []string{"lifecycle", "queuelimit", "snooze", "payloadsearch"} {
		if !seen[want] {
			t.Errorf("GroupNames lacks %q: %v", want, names)
		}
	}
}

// TestHarnessGroupsPicksGroups: with Harness.Groups set, Run makes a
// subtest for exactly the groups it accepts, and asks it about every
// group once.
func TestHarnessGroupsPicksGroups(t *testing.T) {
	var asked []string
	var ran []string
	t.Run("suite", func(t *testing.T) {
		workflowtest.Run(t, workflowtest.Harness{
			Groups: func(name string) bool {
				asked = append(asked, name)
				return name == "lifecycle" || name == "idempotency"
			},
			New: func(t *testing.T) workflowtest.Instance {
				// t is a case's subtest: suite/<group>/<case>.
				group, _, _ := strings.Cut(strings.TrimPrefix(t.Name(), "TestHarnessGroupsPicksGroups/suite/"), "/")
				if len(ran) == 0 || ran[len(ran)-1] != group {
					ran = append(ran, group)
				}
				clk := clock.NewFake(time.Date(2026, 1, 1, 10, 0, 30, 0, time.UTC))
				return workflowtest.Instance{Client: memory.New(memory.WithClock(clk)), Clock: clk}
			},
		})
	})
	if !slices.Equal(asked, workflowtest.GroupNames()) {
		t.Errorf("Groups was asked about %v, want every group once: %v", asked, workflowtest.GroupNames())
	}
	if want := []string{"lifecycle", "idempotency"}; !slices.Equal(ran, want) {
		t.Errorf("cases ran in groups %v, want %v", ran, want)
	}
}
