package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: both
// attempts' output is stored with the first attempt's error, read back
// oldest first, and served to the admin through OutputSourceFunc.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `run succeeded
fetch attempt 1 error="feed timed out" logged fetching feed: true
fetch attempt 2 error="" logged fetching feed: true
admin sees 2 attempts
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
