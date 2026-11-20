package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks its deterministic output.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `memory: peak within limit 3: true
memory: peak reached the limit: true
sqlstore: queue limit is cluster-wide: true
sqlstore: peak within limit 3: true
sqlstore: peak reached the limit: true
`
	if got := out; got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
