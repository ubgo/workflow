package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, as the README shows it.
const want = `supports CompleteInTx: true
invoice-42: output "invoice-42", row written: true
invoice-declined: output "not invoiced", row written: false
`

// TestRun runs the program and checks every line it prints, so a change
// in the library's behaviour fails here before the guide goes stale.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if out != want {
		t.Errorf("output differs from the README.\ngot:\n%s\nwant:\n%s", out, want)
	}
}
