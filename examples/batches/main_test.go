package main

import (
	"context"
	"os"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, the one its README and guide show.
const want = `-- every job succeeds
dispatched thumbs-1
finally thumbs-1 tenant=acme succeeded=3 failed=0 cancelledJobs=0 cancelled=false
then thumbs-1 tenant=acme succeeded=3 failed=0 cancelledJobs=0 cancelled=false
progress thumbs-1: total=3 pending=0 succeeded=3 failed=0 processed=3 (100%) finished=true
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
-- an empty batch ID
refused: true
`

// wantGroups is the output of the group runner part (groups.go).
const wantGroups = `-- a client that starts a group in one call
group-1: grouped=true, RunGroup calls=1
group-1: finally succeeded=3 of 3
group-2: grouped=false, RunGroup calls=1
group-2: finally succeeded=3 of 3
-- Hatchet bulk-trigger chunks
chunks of at most 500 runs and 1048576 bytes
over the ceiling refused: true
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

// TestGroups runs the group runner part: an eligible batch starts in one
// RunGroup call, a batch with an idempotency key is enqueued job by job,
// and the Hatchet driver refuses a chunk limit over its ceiling before it
// connects, so this part needs no engine.
func TestGroups(t *testing.T) {
	out, err := capture.Stdout(func() error { return runGroups(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if got := out; got != wantGroups {
		t.Errorf("output:\n%s\nwant:\n%s", got, wantGroups)
	}
}

// TestHatchet runs the Hatchet part against a live engine; it skips, with
// the reason, when HATCHET_CLIENT_TOKEN is not set.
func TestHatchet(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the Hatchet part needs a live engine (see README.md)", envToken)
	}
	out, err := capture.Stdout(func() error { return runHatchet(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := "hatchet: coordinated=true, one parent run=true\nhatchet: finally succeeded=50 of 50\nhatchet: coordinated batch without a LabelSource refused: true\n"
	if got := out; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}
