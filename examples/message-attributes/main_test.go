package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `http hop:
  step result: "tenant acme, source shop"
  step span in the producer's trace: true
sqs: traceparent ubgo-l-source ubgo-tenant ubgo-workflow
sqs, a label SQS cannot name: traceparent ubgo-labels ubgo-tenant ubgo-workflow
pub/sub, a 2000-byte key refused: true
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
