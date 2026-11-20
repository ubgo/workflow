package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, the one its README and guides show.
const want = `-- without a recorder
event sync/fetch INFO "fetched changes" [{rows 100}]
event sync/fetch WARN "rows rejected" [{bad 3} {first row-17}]
outcome kept: false unsupported: true
== 2026-01-05T02:00:00Z sync/fetch attempt 1 [run-2] ok
2026-01-05T02:00:00Z INFO fetched changes rows=100
2026-01-05T02:00:00Z WARN rows rejected bad=3 first=row-17
-- with a recorder
outcome kept: true unsupported: false
run succeeded outcome=partial (3 of 100 rows rejected)
  fetch INFO "fetched changes" [{rows 100}]
  fetch WARN "rows rejected" [{bad 3} {first row-17}]
cleanup succeeded outcome=success (set by a step: false)
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
