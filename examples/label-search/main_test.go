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
	want := `label prefix capability: true
acme, order A-*: A-1001 A-1002
has a coupon: A-1002 A-1003
IndexLabels: 4 runs, then 0
hatchet refuses: field RunQuery.LabelPrefixes, missing LabelPrefix
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
