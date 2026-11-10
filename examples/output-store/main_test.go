package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: both
// attempts' output is stored with the first attempt's error, read back
// oldest first, and served to the admin through OutputSourceFunc.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `run succeeded
fetch attempt 1 error="feed timed out" logged fetching feed: true
fetch attempt 2 error="" logged fetching feed: true
admin sees 2 attempts
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
