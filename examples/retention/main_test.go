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
	want := `now: deleted 3, held undelivered 0, idempotency 0, batch 0
a day later: deleted 2
a week later: deleted 1
archived: email failed "fail", email succeeded "a", email succeeded "b", report succeeded "r1", report succeeded "r2", report succeeded "r3"
no observer, Undelivered false: deleted 0, held undelivered 1
Undelivered true: deleted 1
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
