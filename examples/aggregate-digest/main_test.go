package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: seven
// comments become three digests (a full chunk of three at once, the rest
// at the window's end), and a fan-out whose second page fails once resumes
// there and reaches every recipient exactly once.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- Aggregate comments into digests
7 comments became 3 mails:
  digest for alice: c1, c2, c3
  digest for alice: c4, c5
  digest for bob: c1, c2
-- Fan an announcement out in chunks
enqueued 25 jobs; 25 mails to 25 distinct recipients
pages read: offset 0 1x, offset 10 2x, offset 20 1x
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
