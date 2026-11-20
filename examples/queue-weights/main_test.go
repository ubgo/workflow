package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks its whole output: the order one worker takes two weighted queues in, from a pinned random source, and three runs of one key starting in enqueue order.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `weighted (E = export, I = import):
  E1 E2 E3 E4 I1 E5 I2 E6
  I3 E7 E8 I4 I5 I6 I7 I8
fifo per key:
  started sync-1
  started sync-2
  started sync-3
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
