package main

import (
	"context"
	"strings"
	"testing"
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
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
