package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `step fetch attempt 1: err=CSV host timed out
step fetch attempt 2: err=<nil>
step store attempt 1: err=<nil>
store output: "stored ["ada","grace"]"
attempts: fetch 2, store 1; backoff waits [1s]
a wait step without InlineEvents is refused: true
wait output: "by ada"
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
