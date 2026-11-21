package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// boundary timer reminds about the slow review only and does not stop it,
// the fallback answers for a supplier that missed its deadline, and the
// teardown releases the environment after a green and a broken suite,
// with the broken run still failed.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `boundary timer:
  quick review: "approved"
    ran: file, review
  slow review: "approved"
    ran: file, reminder: claim-slow still in "review" after 20ms, review
interrupting deadline:
  "cached price 9.90 (price failed: timeout)"
setup and teardown:
  green suite: run succeeded
    ran: create env-42, test, delete env-42
  broken suite: run failed
    ran: create env-42, test, delete env-42
`
	if got := out.String(); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
}
