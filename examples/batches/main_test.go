package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `-- every job succeeds
dispatched thumbs-1
finally thumbs-1 tenant=acme succeeded=3 failed=0 cancelledJobs=0 cancelled=false
then thumbs-1 tenant=acme succeeded=3 failed=0 cancelledJobs=0 cancelled=false
-- a job fails
catch thumbs-2 tenant=acme succeeded=0 failed=1 cancelledJobs=0 cancelled=true
finally thumbs-2 tenant=acme succeeded=0 failed=1 cancelledJobs=1 cancelled=true
-- the app cancels
before cancel: 0 of 2 processed (0%)
finally thumbs-3 tenant=acme succeeded=0 failed=0 cancelledJobs=2 cancelled=true
-- a Finally that failed to enqueue
pending finally: attempts=1 last error: batch thumbs-4: enqueue finally callback "finally": engine unavailable
finally thumbs-4 tenant=globex succeeded=1 failed=0 cancelledJobs=0 cancelled=false
-- a reserved label
refused: true
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

// TestHatchet runs the Hatchet part against a live engine; it skips, with
// the reason, when HATCHET_CLIENT_TOKEN is not set.
func TestHatchet(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the Hatchet part needs a live engine (see README.md)", envToken)
	}
	var out strings.Builder
	if err := runHatchet(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := "hatchet: coordinated=true, one parent run=true\nhatchet: finally succeeded=50 of 50\nhatchet: coordinated batch without a LabelSource refused: true\n"
	if got := out.String(); got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}
