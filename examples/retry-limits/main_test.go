package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `send-sms with Job.Retry{Max: 4}: failed after 5 attempts
sync-crm with Until 2m: failed after 3 attempts, 2m30s after the first
call-partner-api with MaxErrors 2: failed after 5 attempts (3 releases, 2 errors)
ship-order discarded: discarded after 1 attempt, OnFailure ran 0 times, ErrDiscarded: true
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
