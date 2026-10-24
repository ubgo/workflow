package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// offline is what the program prints with no engine: the refusals.
const offline = `capabilities: Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|Queue|RunQuery|RetryRun|CancelAll|ScheduleList|Drain|ScheduleTrigger|ScheduleHistory|EnqueueOnly|Tenant|KeyFIFO|DueQuery|Result|CronSeconds|TickTime|DrainReport
berlin-report: ErrUnsupported=true
  workflow: Workflow.CronLocation unsupported by this driver (missing CronZone): workflow berlin-report: Hatchet evaluates crons in UTC only, not Europe/Berlin; shifting the expression would break across DST
per-tenant: ErrUnsupported=true
  workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow per-tenant: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression
jittered: ErrUnsupported=true
  workflow "jittered" step "only": workflow: Retry.Backoff.Jitter unsupported by this driver (missing RetryJitter): Hatchet waits exactly Factor^n seconds and has no jitter
GroupChunkRuns 5000: ErrInvalid=true
  workflow: invalid spec: hatchet driver: GroupChunkRuns 5000 is over the SDK's bulk limit of 1000
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

// TestLive runs one job on a real engine; it skips without
// HATCHET_CLIENT_TOKEN, as the driver's own live suite does.
func TestLive(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the live run needs a Hatchet engine (see README.md)", envToken)
	}
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := offline + "live run: succeeded\nworker listed through Native: true\n"
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
