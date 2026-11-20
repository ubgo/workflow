package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: both
// steps receive a reference from the queue and the full bytes in the
// handler, the run's blobs are gone once it finishes, and a missing blob
// fails the step once, without the three retries its policy allows.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- Move big payloads out of the queue
load: queue carried a reference
load: handler got 511 bytes
summarise: queue carried a reference
summarise: handler got 200 bytes; blobs held: 2
run: succeeded token label set: true
-- Delete blobs when the run finishes
blobs held after the run: 0
use: queue carried a reference
late run: failed ErrBlobNotFound: true attempts: 1
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
