package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: the
// first mirror to answer wins and the others are cancelled, two of four
// mirrors make a quorum, and a paged map of ten products tolerates one
// failure.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `discriminator:
  first answer: "eu: 42"
  mirrors: 1 succeeded, 2 cancelled
partial join:
  two of four answered, winners: [0,2]
distributed map:
  started 10, succeeded 9, failed 1 (item 4)
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
