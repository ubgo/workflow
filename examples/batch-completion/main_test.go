package main

import (
	"bytes"
	"context"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `-- a poller, on a client without the run-finished hook
the job ran; before the poll: 0 of 1 processed
finally p1 succeeded=1 of 1
after one poll: 1 of 1 processed, finished=true
-- an app's own feed of run ends
finally f1 succeeded=2 of 2
then f1 succeeded=2 of 2
f1: 2 of 2 processed, finished=true
not a batch job, error: <nil>
unknown batch, error: <nil>
`

// TestRun runs the program and checks every line it prints: the polled
// batch hears of its job only on the poller's tick, the app's own Source
// carries the other batch to its callbacks, and RunFinished ignores runs
// that are not jobs of a known batch.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
