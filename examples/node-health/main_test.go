package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// memoryOut is what the memory part prints: the probes before Start,
// after Start and after Stop, the node that ran each attempt, the one
// node the memory client lists, and CheckHealth's fallback.
const memoryOut = `-- before Start
/livez 200 workers ok=false not started
/readyz 503 workers ok=false not started
-- started
/readyz 200 workers ok=true 0 of 8 busy
run succeeded, attempted by [worker-1]
  attempt 1 on worker-1: upstream timeout on worker-1
node worker-1 mode=full workers=8 running=0
no HealthChecker: live=true ready=true check="the driver reports no health"
-- stopped
/livez 503 workers ok=false stopped
`

// TestRun checks every line the guide shows. Without REDIS_URL only the
// memory part runs; with it, a tick fired by the scheduler-only node runs
// on the worker-only node and Nodes lists both with their modes.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := memoryOut + "REDIS_URL is not set: the scheduler and worker nodes are skipped\n"
	if os.Getenv(envURL) != "" {
		want = memoryOut + `tick fired by sched-1, ran on worker-1
node sched-1 mode=scheduler workers=0
node worker-1 mode=worker workers=10
`
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
