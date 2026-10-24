package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: Runs
// merges both backends newest first with refs naming their backend, each
// retry and the delete reach the backend holding the run, a prefixed Ref
// finds one run, CancelAll cancels the waiting run on each backend, and
// Pause holds the workflow on both.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- every run, from both backends, newest first
  o-4 on secondary succeeded
  o-3 on secondary failed memory: step "bill" failed after 1 attempt(s): billing API: 500
  o-2 on primary   failed memory: step "bill" failed after 1 attempt(s): billing API: 500
  o-1 on primary   succeeded
-- retry the failed runs, each on its own backend
  o-3 on secondary: retried, succeeded
  o-2 on primary: retried, succeeded
-- one run by its ref
  o-3 on secondary: succeeded
-- forget a finished run
  o-4 on secondary succeeded
  o-3 on secondary succeeded
  o-2 on primary   succeeded
-- cancel every waiting run on both backends
  cancelled: 2
-- pause on both backends
  paused: invoice
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
