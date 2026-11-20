package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, the one its README and guide show.
const want = `no source refused: true
run succeeded source=webhook/delivery-42 outcome=partial (3 of 120 rows rejected)
input as stored: {"card":{"number":"[REDACTED]"},"shop":"s-1"}
  step fetch attempt 1 succeeded output={"rows":120}
    event "fetched orders" [{rows 120} {shop s-1}]
  step save attempt 1 succeeded output={"saved":117}
webhook runs with a partial outcome: 1
admin reads the recorder: succeeded
reconcile: checked=0 finished=0 stale=0
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background(), filepath.Join(t.TempDir(), "runs.db")) })
	if err != nil {
		t.Fatal(err)
	}
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
