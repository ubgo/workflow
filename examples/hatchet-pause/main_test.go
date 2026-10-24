package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// offline is what the program prints with no engine: the refusals.
const offline = `without API: PauseWorkflow=false SchedulePause=false
Pause: ErrUnsupported=true
PauseSchedule: ErrUnsupported=true
NewAPI with a bad token: ErrInvalid=true
`

// TestRefusals runs the program without an engine: without Config.API
// every pause is refused before anything connects.
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

// TestLive pauses a workflow and a schedule on a real engine; it skips
// without HATCHET_CLIENT_TOKEN, as the driver's own live suite does.
func TestLive(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live run needs a Hatchet engine (see README.md)", envToken)
	}
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := offline + `with API: PauseWorkflow=true SchedulePause=true
workflow listed as paused: true
held run finished while paused: false
after resume: succeeded
PauseAll: ErrUnsupported=true
schedule nightly: paused=true
schedule nightly: paused=false
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
