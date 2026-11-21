package main

import (
	"bytes"
	"context"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `-- Decide what counts as an outage
  marked Unavailable default true  strict true  ours true
  connection refused default true  strict false ours true
  unknown error      default true  strict false ours false
  invalid job        default false strict false ours false
  permanent          default false strict false ours false
-- Watch failovers
approve a-1 -> primary
-- the primary goes down
  event failed_over workflow=report at=09:00:00 err="dial primary: connection refused"
report r-1 -> secondary
  event failed_over workflow=remind at=09:00:00 err="dial primary: connection refused"
  event cool_off_started workflow=remind at=09:00:00 until=09:01:00
remind m-1 -> secondary
  event primary_skipped workflow=approve at=09:00:00
approve a-2 -> secondary
-- the primary is back and the cool-off runs out
  event cool_off_ended workflow=approve at=09:01:00
approve a-3 -> primary
  count failed_over: 2
  count cool_off_started: 1
  count primary_skipped: 1
  count cool_off_ended: 1
-- Reset, edit and signal runs on either backend
report on the secondary is failed
  supports reset-run: true
  supports edit-run: true
  supports signal-waiting: true
reset in place: true succeeded
edited remind on the secondary sent to "new@example.com"
signal-waiting resumed 3 run(s)
  approve on the primary is succeeded
  approve on the secondary is succeeded
  approve on the primary is succeeded
  with a bare secondary, supports signal-waiting: false
  SignalWaiting refused: true
-- Schedules on both backends
schedule copies: primary, secondary
Schedule returned an ID for: both
`

// TestRun runs the program and checks every line it prints: the three
// classifiers' answers, the observer's events and counts, the reset, edit
// and signal on both backends, and the schedule on both.
func TestRun(t *testing.T) {
	var buf bytes.Buffer
	if err := run(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
