package main

import (
	"bytes"
	"context"
	"os"
	"testing"
)

// offlineOut is what the program prints without a server: what the
// driver declares, and the two refusals at Start.
const offlineOut = `WaitForEvent: true
ConcurrencyKey: true
RateLimit: true
Queue: true
PauseWorkflow: true
PauseQueue: true
RunFinished: true
Chain: true
ScheduleWhen: true
ShutdownRequeue: true
Closure: false
QueueLimitGlobal: false
KeyExpr at Start: ErrUnsupported = true
QueueConfig.Limit at Start: ErrUnsupported = true
`

// liveOut is what the live part adds with REDIS_URL set.
const liveOut = `approval: the step after the wait got "by ops"
run-finished observer saw approval: succeeded
invoice: 3 runs of customer acme on two clients, at most 1 at a time
sms: 2 succeeded, the next one is queued (2 per hour)
render while paused: queued, paused targets 1
render after resume: succeeded on queue reports by [reports-node]
chain: extract then load
heartbeat: a tick skipped (reason "when"), a later one fired
long: cut short on deploy-old, finished on deploy-new, attempts [1 1]
data: plan="pro" version 1
promise: first settle true, second settle false, value "paid"
journal: fetch=42 found true
`

// TestRun checks every line the guide shows. Without REDIS_URL only the
// offline part runs.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := offlineOut + "REDIS_URL is not set: the live run is skipped\n"
	if os.Getenv(envURL) != "" {
		want = offlineOut + liveOut
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
