package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, the one its README and guide show.
const want = `process A started jobs 0 and 1, then stalled on job 2
finally: 5 jobs, 4 succeeded, 1 cancelled
job 0: succeeded
job 1: succeeded
job 2: cancelled
job 3: succeeded
job 4: succeeded
process A's dispatch ended with an error: true
`

// TestRun runs the program on a SQLite file in a temp dir and checks every
// line it prints: the stalled job cancelled, the jobs process A never
// started run by process B, and Finally once.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background(), filepath.Join(t.TempDir(), "wf.db")) })
	if err != nil {
		t.Fatal(err)
	}
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
