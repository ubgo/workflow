package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks its whole output: on the memory
// driver and on SQLite, the tenants take turns for one worker, and a
// tenant of weight 3 gets three turns for each turn of weight 1.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `memory:
  round robin: acme-1 globex-1 initech-1 acme-2 globex-2 acme-3 acme-4
  weights: free-1 gold-1 gold-2 gold-3 gold-4 free-2 gold-5 gold-6 free-3 free-4
sqlite:
  round robin: acme-1 globex-1 initech-1 acme-2 globex-2 acme-3 acme-4
  weights: free-1 gold-1 gold-2 gold-3 gold-4 free-2 gold-5 gold-6 free-3 free-4
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
