package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// tick during an unfinished run is skipped with the overlap reason, a
// manual trigger is refused for the same reason and accepted once the run
// ended, the triggered run carries the schedule's labels and the manual
// mark, and the schedule list reports that trigger as its last fire.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- ticks
  09:00 fired, run run-3
  10:00 skipped (overlap)
-- run now while the 09:00 run is unfinished
  refused: workflow: schedule schedule-1: previous run run-3 is running: workflow: the schedule's previous run is unfinished
-- run now after it finished
  run-6 succeeded, labels team=sales ubgo.workflow/trigger=manual
-- schedules
  crm-hourly overlap=skip last fire 10:05 run run-6 (succeeded), next 11:00
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
