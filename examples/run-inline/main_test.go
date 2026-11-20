package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, the one its README and guide show.
const want = `step fetch attempt 1: err=CSV host timed out
step fetch attempt 2: err=<nil>
step store attempt 1: err=<nil>
store output: "stored ["ada","grace"]"
attempts: fetch 2, store 1; backoff waits [1s]
a wait step without InlineEvents is refused: true
wait output: "by ada"
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
