package main

import (
	"bytes"
	"context"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `photo p1: succeeded, result "label of p1"
photo p2: succeeded, result "label of p2"
photo p3: succeeded, result "label of p3"
resize on cpu-node: 3, resize on gpu-node: 0
model on gpu-node: 3, model on cpu-node: 0
model attempts at once on the GPU node: at most 1
`

// TestRun runs the program and checks every line it writes: each photo
// labelled by its child run, every resize on the CPU node, every model on
// the GPU node, and never two models on the one card at once.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
