package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: spawned
// children with their parent labels, AwaitChildren's results in job order
// without spending parent attempts, and a failed child failing the parent.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `spawn:
  parent finished, children started: 2
  child of welcome, parent run matches: true
  child of welcome, parent run matches: true
  notifications sent: 2
await children:
  results in job order: ["a@2x.png","b@2x.png","c@2x.png"]
  parent step attempts spent: 1
  a failed child fails the parent: failed
  parent error names the child: true
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
