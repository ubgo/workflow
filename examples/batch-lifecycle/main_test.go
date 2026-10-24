package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guides show.
const want = `-- before and progress
before b1 total=3
finally b1 total=3 succeeded=3 failed=0
progress b1 total=3
progress b1 total=3
progress b1 total=3
-- a job adds jobs to its batch
finally b2 total=3 succeeded=3 failed=0
then b2 total=3 succeeded=3 failed=0
-- retry the failed jobs
catch b3 total=2 failed=1
finally b3 total=2 succeeded=1 failed=1
retried 1
finally b3 total=2 succeeded=2 failed=0
then b3 total=2 succeeded=2 failed=0
-- a workflow waits for a batch
import done: 4 of 4 succeeded
-- many jobs without a batch
started 3 runs
-- prune
pruned 4
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
