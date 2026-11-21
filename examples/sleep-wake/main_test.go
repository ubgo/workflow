package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// plain clock runs the reminder 30 minutes after the resume, the
// wake-checked clock within one check.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `reminder due 23:00, laptop sleeps 22:30 to 07:00
default check: 30s
plain clock:        ran at 07:30, 30m0s of running time after the resume
wake-checked clock: ran at 07:01, within one check (1m0s) of the resume
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
