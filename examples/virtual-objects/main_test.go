package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: three
// calls of one cart applied in turn, another cart kept apart, and a clear.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `alice's cart holds 3 items
bob's cart holds 1 item
after clear, alice's cart holds 0 items
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
