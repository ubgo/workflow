package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: a new
// job lands on the new backend with a new: ref, a ref stored before the
// cutover still answers, Runs lists both backends, the old backend's
// schedule moves to the new one, and Remaining falls to zero once the old
// backend's run has finished.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- New work goes to the new backend
enqueued: new:run-2
status of run-2: running
status of new:run-2: running
runs: new:run-2, old:run-2
-- Move the schedules
schedule "nightly" on old:schedule-4
moved "nightly" from old:schedule-4 to new:schedule-4
schedule "nightly" on new:schedule-4
-- Retire the old backend
unfinished runs on the old backend: 1
after the export finished: 0 - the app can drop the old backend
-- Keep the schedules on the old backend for now
schedule on the old side: true (old:schedule-1)
MoveSchedules refused under SchedulesOn(SideOld): true
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
