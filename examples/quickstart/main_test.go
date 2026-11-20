package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks the line the guide shows.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if got, want := out, "hello Ada\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}
