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
	want := `consumer:
  msg-1 ran: true
  msg-1 ran: false
  msg-1 ran: false
  receipts sent: 1
  msg-2 first attempt: smtp down
  msg-2 redelivered, ran: true <nil>
transactional, four concurrent deliveries:
  orders written: 1
webhook:
  status: 200
  status: 200
  status: 200
  runs started: 1
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
