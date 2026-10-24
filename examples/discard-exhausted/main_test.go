package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output.
const want = `workflow policy, DiscardOnExhaust: discarded after 4 attempt(s), OnFailure ran 0, ErrDiscarded true
workflow policy, default ending:   failed    after 4 attempt(s), OnFailure ran 1, ErrDiscarded false
step policy beats workflow:        discarded after 2 attempt(s), OnFailure ran 0, ErrDiscarded true
Job.Retry beats step policy:       failed    after 1 attempt(s), OnFailure ran 0, ErrDiscarded false
Permanent with DiscardOnExhaust:   discarded after 1 attempt(s), OnFailure ran 0, ErrDiscarded true
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
