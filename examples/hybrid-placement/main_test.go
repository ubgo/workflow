package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun checks every line the guide shows: the capability split, the
// 02:00 tick fired by the scheduler client running on the jobs client
// with its schedule label, the run kept by the jobs client, and the two
// refusals.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `cron: true (scheduler)  run query: true (jobs)  trigger: false
tick 02:00 ran on node "jobs", schedule cron:nightly-report:0
runs on the jobs client: 1
CronOverlap refused: true
TriggerSchedule refused: true
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
