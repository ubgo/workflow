package main

import (
	"bytes"
	"context"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `memory declares CapSlotCost: true
webhooks that ran beside the render: 1 of 3
most slots held at once: 4 of 4
register a 5-slot step on a 4-slot pool: true
  workflow: invalid spec: workflow "transcode-8k" step "transcode" needs 5 worker slots and this client has 4
pool: 3 of 4 in use after one render
pool: a second render waits until its context ends: true
pool: 0 in use after Release
pool: asking for more than the pool: true
`

// TestRun runs the program and checks every line it writes: one webhook
// beside the three-slot render and never more than the pool, the refusal
// of a step bigger than the pool, and the SlotPool calls.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
