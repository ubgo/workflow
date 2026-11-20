package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, the one its README and guides show.
const want = `no source: workflow: invalid spec: job for "nightly-report" has no source; set one with workflow.WithSource
typo: workflow: invalid spec: job for "nightly-report" has source "webook", not one of [api webhook admin]
webhook: source=webhook ref=delivery-42
cron fire: source=schedule ref=cron:nightly-report:0
run now: source=schedule-manual ref=cron:nightly-report:0
validate: workflow: invalid spec: source "Slack" has the character 'S'; use lower-case letters, digits and . _ -
library sources: [schedule schedule-manual child batch event external]
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
