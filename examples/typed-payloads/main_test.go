package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks its whole output: a typed job and its decoded result, a permanent decode error, the app's and the driver's size limits, and old and new payload versions read by one handler.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `typed:
  payload {"user_id":"u-7","plan":"pro"}
  receipt msg-u-7
  bad payload: permanent true
size limits:
  2002 bytes over the middleware.MaxPayload limit of 1024
  ErrPayloadTooLarge true, ErrInvalid true
  5002 bytes over the memory.WithPayloadLimit limit of 4096
  driver limit: 4096 bytes
versions:
  version 1 {"name":"Ann Lee"} -> "Hello Ann Lee"
  version 2 {"first":"Bo","last":"Chen"} -> "Hello Bo Chen"
  version 3 sent to release 2: ErrInvalid true
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
