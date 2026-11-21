package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// on-time run works, the two stale catch-up runs are skipped with the tick
// that made them stale, the newest catch-up run works, and a run no
// schedule fired always works.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- on time
  tick 09:00, run at 09:00: refreshed the cache
-- after a three-hour outage
  tick 10:00, run at 12:30: skipped, the 11:00 tick is due
  tick 11:00, run at 12:30: skipped, the 12:00 tick is due
  tick 12:00, run at 12:30: refreshed the cache
-- not fired by a schedule
  tick none, run at 12:30: refreshed the cache
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
