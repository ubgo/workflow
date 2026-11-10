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
	want := `label prefix capability: true
acme, order A-*: A-1001 A-1002
has a coupon: A-1002 A-1003
IndexLabels: 4 runs, then 0
hatchet refuses: field RunQuery.LabelPrefixes, missing LabelPrefix
`
	if got := out; got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
