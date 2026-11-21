package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the run
// written by plain SQL ran with its payload, its label row finds it with
// the queue the worker filled in and its tenant, the delayed run ran, and
// a held key refuses a second insert and is returned by Enqueue.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `run_billing_0001 succeeded; its step saw {"month":"2026-11"}
label source=billing finds run_billing_0001: queue reports (filled in at admission), tenant acme
run_billing_0002 (scheduled) succeeded after its run_at
second insert of key report-2027-01: unique violation true
Go Enqueue with the same key returns the inserted run: true
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
