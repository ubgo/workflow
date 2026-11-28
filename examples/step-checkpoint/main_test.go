package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, as the README shows it.
const want = `attempt 1 starts at page 1
fetched page 1
fetched page 2
attempt 2 starts at page 3
fetched page 3
fetched page 4
the run: succeeded
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
