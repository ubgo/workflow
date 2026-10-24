package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks its whole output: two snoozes that
// spend no attempt, a hung attempt failed by its heartbeat timeout and a
// retry that beats, and a run timeout that runs OnTimeout, not OnFailure.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `snooze:
  try at +0s: attempt 1
  snoozed, status waiting
  try at +1m0s: attempt 1
  snoozed, status waiting
  try at +2m0s: attempt 1
  run succeeded, attempts 1, errors recorded 0
heartbeat:
  attempt 1 started
  attempt 2 started at +31s
  attempt 1 failed at +30s: workflow: heartbeat timed out: memory: step "load" attempt 1 went 30s without a heartbeat
  run succeeded
run timeout:
  run failed: workflow: run timed out: after 5m0s
  ErrRunTimeout true, IsTimeout true, ErrStepTimeout false
  ran OnTimeout with payload "q3"
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
