package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks the line the guide shows.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "hello Ada\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}
