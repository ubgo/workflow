package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guides show: the
// signal wakes only the run waiting on the event's key, a replay reports
// a failed delivery and the same replay ID retries only it, a cancel
// subscription reports what it cancelled and refuses a message without a
// key, and only valid extension names cross into a CloudEvent.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `signal:
  signalled: true, subscriber runs started: 1
  order A-1 woke and shipped with {"order":"A-1","cents":4000}
  order A-2 is still waiting: the event's key was A-1
replay result:
  replay backfill-1: 2 messages, 4 deliveries, error: true
    inv_1 -> receipt existing=false failed=false
    inv_1 -> ledger  existing=false failed=true
    inv_2 -> receipt existing=false failed=false
    inv_2 -> ledger  existing=false failed=true
  the ledger is back; the same replay ID again:
    inv_1 -> receipt existing=true  failed=false
    inv_1 -> ledger  existing=false failed=false
    inv_2 -> receipt existing=true  failed=false
    inv_2 -> ledger  existing=false failed=false
cancellation:
  default statuses: scheduled, queued, running, waiting, retrying
  stop-reminders: cancelled 1, diverted false, err <nil>
  no key: cancelled 0, Cancellation.Err is ErrInvalid: true, Publish error: true
extension names:
  "tenant"     valid=true
  "region2"    valid=true
  "tenant-id"  valid=false
  "Tenant"     valid=false
  "subject"    valid=false
  ""           valid=false
  FromMessage kept: map[tenant:acme]
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
