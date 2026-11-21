package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: three
// calls of one cart applied in turn, another cart kept apart, a clear,
// the cart's four calls found by their key label, and a call of an
// unknown method failing its run.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `alice's cart holds 3 items
bob's cart holds 1 item
after clear, alice's cart holds 0 items
calls of alice's cart so far: 4
unknown method: the call failed, naming the methods: true
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
