package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guide show.
const want = `every run end is delivered: true
observer after Start refused: true
charge succeeded order=o-1 err=<nil> cancelled=false
charge failed order=o-2 err=memory: step "run" failed after 1 attempt(s): card declined cancelled=false
charge cancelled order=o-3 err=workflow: run cancelled cancelled=true
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

// TestHatchet runs the Hatchet part against a live engine; it skips, with
// the reason, when HATCHET_CLIENT_TOKEN is not set.
func TestHatchet(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the Hatchet part needs a live engine (see README.md)", envToken)
	}
	var out strings.Builder
	if err := runHatchet(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "hatchet: every run end is delivered: false\nhatchet: run succeeded\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}
