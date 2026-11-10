package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, the one its README and guide show.
const want = `by depth: emails 1, imports 7
by wait:  emails 2, imports 6
bounded:  emails 3, imports 5
before: emails workers 4 running 0 waiting 0; imports workers 4 running 4 waiting 8
after one round: emails workers 1 running 0 waiting 0; imports workers 7 running 7 waiting 5
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
