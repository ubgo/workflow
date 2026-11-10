package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, as the README shows it.
const want = `second enqueue returned the first run: true
after the run finished, a new run: true
payout without a key: true
ping with the same key twice, two runs: true
no CapIdempotencyKey: workflow: Job.IdempotencyKey unsupported by this driver (missing IdempotencyKey): job for workflow invoice
`

// TestRun runs the program and checks every line it prints, so a change
// in the library's behaviour fails here before the guide goes stale.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if got := out; got != want {
		t.Errorf("output differs from the README.\ngot:\n%s\nwant:\n%s", got, want)
	}
}
