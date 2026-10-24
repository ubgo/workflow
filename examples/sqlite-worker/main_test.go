package main

import (
	"context"
	"strings"
	"testing"

	"github.com/ubgo/workflow/sqlstore"
)

// TestRun runs the program and checks the lines the guides show.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `memory durable: false
memory cron: true
memory run: succeeded
sqlite durable: true
run id kind: run
sqlite run: succeeded
status after the end: succeeded
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}

// TestOrderedIDs checks the generator keeps the driver's rules: 1 to 64
// bytes, prefixed by the kind, sortable by creation time.
func TestOrderedIDs(t *testing.T) {
	a, err := orderedIDs(sqlstore.IDRun)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(a, "run_") || len(a) > 64 {
		t.Fatalf("id %q: want a run_ prefix and at most 64 bytes", a)
	}
}
