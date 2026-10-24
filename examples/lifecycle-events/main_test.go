package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun checks every line the guides show: the lifecycle events of a
// run that retries, reports progress and succeeds and of one that fails,
// in order and on the fake clock; the failed run's RunError with its step
// and attempt; and Stuck reporting a hung attempt once while it keeps
// running to success.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- lifecycle
  09:00:00 inserted  export status=queued node=worker-1
  09:00:00 started   export step=rows attempt=1 status=running node=worker-1
  09:00:00 retrying  export step=rows attempt=1 status=retrying node=worker-1 err=database busy
  09:00:00 started   export step=rows attempt=2 status=running node=worker-1
  09:00:00 progress  export step=rows attempt=2 status=running node=worker-1 progress=50% "500 of 1000 rows"
  09:00:00 finished  export status=succeeded node=worker-1
  09:00:00 inserted  charge status=queued node=worker-1
  09:00:00 started   charge step=card attempt=1 status=running node=worker-1
  09:00:00 finished  charge status=failed node=worker-1 err=memory: step "card" failed after 1 attempt(s): card declined
-- run error
  failed: workflow "charge" run run-6 step "card" attempt 1: memory: step "card" failed after 1 attempt(s): card declined
  step=card attempt=1 declined=true
  Error(): memory: step "card" failed after 1 attempt(s): card declined
-- stuck
  report step=build attempt=1 node=worker-1 started=09:00 running=6m0s
  the attempt kept running and ended succeeded
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
