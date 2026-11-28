package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, as the README shows it.
const want = `second trigger returned the running run: true duplicate: true
a hand enqueue under the same key joined it: true
a refusing trigger while the key is held: true
the key of a daily digest fired at 09:00: digest:2026-11-28
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
