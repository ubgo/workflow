package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: by
// default the schedule lives on the primary alone and each tick fires
// once; with WithSchedulesOnBoth it is listed once per backend, each tick
// fires on both, and the ID Schedule returned pauses both copies.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- default: schedules on the primary only
Schedule returned an ID for: primary
  listed on primary
09:00 fired on: primary
  primary paused: true
-- WithSchedulesOnBoth: schedules on both backends
Schedule returned an ID for: both
  listed on primary
  listed on secondary
09:00 fired on: primary, secondary
  primary paused: true
  secondary paused: true
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
