package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// helper's schedules carry the group label, both fire at 09:00 in Berlin
// and their runs are found by the label, and pausing the group leaves the
// schedule outside it running.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- the group in the schedule list
  nightly       -            next 02:00 CET
  digest-hourly office-hours next 09:00 CET
  orders-hourly office-hours next 09:00 CET
-- 09:00: both schedules of the group fire
runs of the group: 2
-- pause the group
  nightly       -            next 02:00 CET
  digest-hourly office-hours paused
  orders-hourly office-hours paused
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
