package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// offline is what the program prints with no engine: the refusals.
const offline = `Retry.Until: ErrUnsupported=true
Job.Retry: ErrUnsupported=true
QueueConfig.Priority: ErrUnsupported=true
PauseWorkflow: false
DeleteRun: false
Discard: false
`

// TestRefusals runs the program without an engine: every refusal happens
// before anything connects.
func TestRefusals(t *testing.T) {
	t.Setenv(envToken, "")
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := offline + "HATCHET_CLIENT_TOKEN is not set: the live run is skipped\n"
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}

// TestLive runs the management calls on a real engine; it skips without
// HATCHET_CLIENT_TOKEN, as the driver's own live suite does.
func TestLive(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live run needs a Hatchet engine (see README.md)", envToken)
	}
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := offline + `failed run: queue=reports step=render error="renderer offline"
after retry: succeeded, render attempts=2
same name, same definition, same ID: true
schedule nightly: 0 2 * * *, next fire in UTC at 02:00
workflow listed as paused: true
after resume: succeeded
drained
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
