package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks its deterministic output.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `memory: peak within limit 3: true
memory: peak reached the limit: true
sqlstore: queue limit is cluster-wide: true
sqlstore: peak within limit 3: true
sqlstore: peak reached the limit: true
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
