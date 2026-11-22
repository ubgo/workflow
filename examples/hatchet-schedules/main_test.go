package main

import (
	"bytes"
	"context"
	"os"
	"testing"
)

// offlineOut is what the program prints with no engine: the defaults, the
// enqueue-only client, the overlap refusal and the quirk settings.
const offlineOut = `StepTimeout: 24h0m0s
ScheduleTimeout: 24h0m0s
RegisterTimeout: 30s
RunFinishedPoll: 1s
ScheduleHistoryWindow: 168h0m0s
PayloadLimit: 1572864 bytes
GroupChunkRuns: 1000 GroupChunkBytes: 2097152
BatchCoordinatorRetry: 10 retries, 2s doubling, capped at 2m0s
trigger now: true last fire: true
OnRunFinished on a web node: ErrUnsupported = true
Start on a web node starts nothing: true
OverlapSkip: ErrUnsupported = true
export registered with a 2m0s step timeout: true
a name ending in __wf_delayed: ErrInvalid = true
a step named wf__start: ErrInvalid = true
`

// TestOffline runs the program without an engine: nothing connects.
func TestOffline(t *testing.T) {
	t.Setenv(envToken, "")
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := offlineOut + "HATCHET_CLIENT_TOKEN is not set: the live run is skipped\n"
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}

// TestLive runs the schedule calls on a real engine; it skips without
// HATCHET_CLIENT_TOKEN, as the driver's own live suite does.
func TestLive(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live run needs a Hatchet engine (see README.md)", envToken)
	}
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := offlineOut + `schedule nightly: last run succeeded, next fire 02:00 UTC
enqueued from the web node: succeeded
keyed delayed start has a RunID: true same run for the key: true
Reschedule of a keyed start: ErrUnsupported = true
RunAt past IdempotencyMaxHold: ErrUnsupported = true
delayed start cancelled before it fired
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
