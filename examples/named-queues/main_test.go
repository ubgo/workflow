package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `build-report hold on reports
send-email password-reset on urgent
send-email newsletter on mail
build-report monthly on reports
build-report weekly on reports
digest while mail is paused: queued
send-email digest on mail
enqueue to a queue this client does not serve: workflow: invalid spec: queue "sms" is not served by this client (WithQueues); the run would never start
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
