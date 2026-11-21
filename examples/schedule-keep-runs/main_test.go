package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// TestRun runs the program and checks every line the guides show: each
// run reports its own tick and hour, also the three catch-up runs that
// start together at 13:30; the cursor hands each run the next three rows
// with none taken twice; and both schedules keep only their newest two
// finished runs.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- driver
  memory has ScheduleKeep|TickTime: true
-- hourly import, three rows a run
  tick 09:00, run at 09:00: orders 08:00-09:00, rows 1-3
  tick 10:00, run at 10:00: orders 09:00-10:00, rows 4-6
-- after a three-hour outage
  tick 11:00, run at 13:30: orders 10:00-11:00, rows 7-9
  tick 12:00, run at 13:30: orders 11:00-12:00, rows 10-12
  tick 13:00, run at 13:30: orders 12:00-13:00, rows 13-15
-- run history
  import-orders (KeepRuns 2): 2 runs kept, ticks 12:00 13:00
  rollup (CronKeepRuns 2): 2 runs kept, ticks 12:00 13:00
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}

// TestPeriodReadsTheTick tests the step's period on its own: the labels a
// schedule puts on a fired run are built with FiredLabelsAt, so a unit
// test needs no driver and no clock.
func TestPeriodReadsTheTick(t *testing.T) {
	// snippet:test
	tick := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	labels := workflow.FiredLabelsAt(nil, "sched-1", "hourly-import", tick) // what the driver sets
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Labels: labels})
	from, to, ok := period(ctx)
	if !ok || !from.Equal(tick.Add(-time.Hour)) || !to.Equal(tick) {
		t.Fatalf("period = %v, %v, %v; want the hour ending at %v", from, to, ok, tick)
	}
	// snippet:end
	if _, _, ok := period(context.Background()); ok {
		t.Fatal("a run no schedule fired has a period")
	}
}
