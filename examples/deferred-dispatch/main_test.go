package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `POST /orders/o-1: status 201, panicked false
POST /orders/o-2?then=fail: status 402, panicked false
POST /orders/o-3?then=panic: status 500, panicked true
receipts after the requests: o-1
held before flush: 2
flushed: 2 runs
discarded: 1, enqueue after discard: ErrDeferredClosed true
receipts at the end: o-1,o-4,o-5
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
