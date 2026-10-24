package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks the lines the guide shows.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `send-welcome: succeeded after 2 attempts, step "func"
bad address: failed, catch ran for "eve@example.invalid"
unregistered name: ErrNotFound true
first process stopped with the run waiting
second process ran send-welcome: succeeded
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
