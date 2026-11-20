package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks the lines the guide shows: two
// generations recycled at the job limit, a third that finds the queue
// empty, and a memory recycle.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `generation 1: recycled (max_jobs), jobs done: 2
generation 2: recycled (max_jobs), jobs done: 2
generation 3: queue empty, jobs done: 1, exit
done: 5 of 5
memory worker: recycled (max_memory) at 600 MiB, ErrRecycle true
`
	if got := out; got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
