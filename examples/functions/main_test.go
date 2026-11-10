package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks the lines the guide shows.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `send-welcome: succeeded after 2 attempts, step "func"
bad address: failed, catch ran for "eve@example.invalid"
unregistered name: ErrNotFound true
first process stopped with the run waiting
second process ran send-welcome: succeeded
`
	if got := out; got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
