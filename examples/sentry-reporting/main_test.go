package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: one
// event per failed run at level error, the workflow tags and the opted-in
// tenant label, the default fingerprint, and neither the payload nor a
// label that was not opted in on the wire.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `events sent: 1
level: error
error: memory: step "capture" failed after 1 attempt(s): card declined
tag workflow.final=true
tag workflow.label.tenant=acme
tag workflow.name=charge
tag workflow.status=failed
tag workflow.step=capture
fingerprint: ubgo-workflow charge capture *errors.errorString
payload sent: false user label sent: false
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
