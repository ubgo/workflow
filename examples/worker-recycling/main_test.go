package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks the lines the guide shows: two
// generations recycled at the job limit, a third that finds the queue
// empty, and a memory recycle.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `generation 1: recycled (max_jobs), jobs done: 2
generation 2: recycled (max_jobs), jobs done: 2
generation 3: queue empty, jobs done: 1, exit
done: 5 of 5
memory worker: recycled (max_memory) at 600 MiB, ErrRecycle true
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
