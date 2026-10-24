package hatchet_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// scheduleTimeoutWait bounds how long the engine takes to fail or start
// the held run: the rate-limit window (a minute), the engine's late
// refill (up to about 30s, see doc.go) and the poll in between.
const scheduleTimeoutWait = 3 * time.Minute

// heldRun starts a client with cfg and a fresh workflow limited to one
// start a minute, runs its first run and returns a second run the limit
// holds. A held task waits outside the scheduler until its bucket's next
// window and is then requeued, and the scheduler fails it on that pass
// if its schedule timeout (counted from creation) has passed
// (pkg/scheduling/v1/scheduler.go isTimedOut, queue.sql
// RequeueRateLimitedQueueItems in v0.107.1).
func heldRun(t *testing.T, cfg hatchet.Config) (*hatchet.Client, workflow.RunRef) {
	t.Helper()
	prefix := "wfst_" + uuid.NewString()[:8]
	cfg.WorkerName = prefix
	wf := workflow.Workflow{
		Name:       prefix,
		RateLimits: []workflow.RateLimit{{Key: workflow.KeyExpr(`"all"`), Limit: 1, Per: time.Minute}},
		Steps:      []workflow.Step{{Name: "a", Run: okStep}},
	}
	c, _ := startLive(t, cfg, wf)
	ctx := context.Background()
	first, err := c.Enqueue(ctx, workflow.Job{Workflow: prefix})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, first, workflow.StatusSucceeded)
	held, err := c.Enqueue(ctx, workflow.Job{Workflow: prefix})
	if err != nil {
		t.Fatal(err)
	}
	return c, held
}

// waitEnd polls ref until it is terminal, within scheduleTimeoutWait.
func waitEnd(t *testing.T, c workflow.Client, ref workflow.RunRef) workflow.Status {
	t.Helper()
	deadline := time.Now().Add(scheduleTimeoutWait)
	for time.Now().Before(deadline) {
		st, err := c.Status(context.Background(), ref)
		if err == nil && st.IsTerminal() {
			return st
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("run %+v did not end within %s", ref, scheduleTimeoutWait)
	return ""
}

// TestScheduleTimeoutLive shows the engine's schedule timeout and that
// Config.ScheduleTimeout sets it: a run a rate limit holds past its
// window is failed, without an attempt, when the timeout is shorter
// than the wait (5s here; the engine's five-minute default failed the
// conformance ratelimit case's later runs), and starts when it is not
// (the driver's day). The ratelimit conformance group is the
// minutes-long proof: 1 of 5 runs passed with the engine's default, 5 of
// 5 with the driver's.
func TestScheduleTimeoutLive(t *testing.T) {
	skipWithoutEngine(t)
	t.Run("a held run outliving its timeout fails", func(t *testing.T) {
		c, held := heldRun(t, hatchet.Config{ScheduleTimeout: 5 * time.Second})
		if st := waitEnd(t, c, held); st != workflow.StatusFailed {
			t.Fatalf("held run ended %s, want failed by the engine", st)
		}
	})
	t.Run("under the default a held run starts in the next window", func(t *testing.T) {
		c, held := heldRun(t, hatchet.Config{})
		if st := waitEnd(t, c, held); st != workflow.StatusSucceeded {
			t.Fatalf("held run ended %s, want succeeded", st)
		}
	})
}
