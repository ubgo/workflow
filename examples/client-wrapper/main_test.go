package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// wrapper that embeds Forward still offers the run query and pause
// interfaces of the memory driver, the bare wrapper hides them, and
// Forward over the bare wrapper refuses with the missing capability.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `audit: enqueue email
audited: RunQuerier=true Pauser=true
runs through the wrapper: 1 succeeded
paused through the wrapper: email
bare: RunQuerier=false, although the driver has CapRunQuery=true
Forward over bare: ErrUnsupported=true missing=true
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
