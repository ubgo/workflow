package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: stage
// order, a conditional step passing its input on, a milestone holding a
// step until the run reaches its state, and a soft deadline letting a
// step stop before its hard timeout.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `stages:
  merge after both fetches: true
  publishing after merge: true
  run result: {"publish-web":"publish-web","publish-feed":"publish-feed"}
when:
  "icon.png" -> "icon.png"
  "poster-large.png" -> resized "poster-large.png"
milestone:
  before payment the run is: running
  after payment: {"pay":null,"ship":"shipped"}
soft timeout:
  step returned before its hard timeout: true
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
