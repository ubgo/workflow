package memory_test

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestScheduleHeapOneGoroutineOneTimer is EM-04: a thousand schedules on
// WithScheduleHeap add no goroutine each and hold one fake-clock timer
// between them, and every one of them still fires on its tick. The same
// thousand without the option start a goroutine each, which is what the
// option removes.
func TestScheduleHeapOneGoroutineOneTimer(t *testing.T) {
	const n = 1000
	for _, heap := range []bool{false, true} {
		t.Run(fmt.Sprintf("heap=%v", heap), func(t *testing.T) {
			var fired atomic.Int64
			var opts []memory.Option
			if heap {
				opts = append(opts, memory.WithScheduleHeap())
			}
			c, fc := newClient(t, []workflow.Workflow{one("tick", func(context.Context, []byte) ([]byte, error) {
				fired.Add(1)
				return nil, nil
			})}, opts...)
			ctx := context.Background()
			before := runtime.NumGoroutine()
			for i := range n {
				if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "tick", Cron: "* * * * *", Name: fmt.Sprint("s", i), Location: time.UTC}); err != nil {
					t.Fatalf("Schedule %d: %v", i, err)
				}
			}
			grew := runtime.NumGoroutine() - before
			if heap {
				blockUntil(t, fc, 1)
				if grew > 10 || fc.Pending() != 1 {
					t.Fatalf("with the heap, %d schedules added %d goroutines and hold %d timers, want about none and 1", n, grew, fc.Pending())
				}
			} else {
				blockUntil(t, fc, n)
				if grew < n-10 { // other goroutines (a finished worker's) may end meanwhile
					t.Fatalf("without the heap, %d schedules added %d goroutines, want one each", n, grew)
				}
			}
			fc.Advance(time.Minute)
			deadline := time.Now().Add(4 * pollFor)
			for fired.Load() < n && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if got := fired.Load(); got != n {
				t.Fatalf("after one minute %d of %d schedules fired", got, n)
			}
		})
	}
}

// TestScheduleHeapRearms checks the heap's head bookkeeping on the real
// clock (a fake clock cannot show a re-arm: the old timer already counts as
// pending): with an hourly schedule armed, an every-second schedule added
// later fires within a second or two, a named upsert keeps one entry (it
// fires about once a second, not twice), and Unschedule stops it.
func TestScheduleHeapRearms(t *testing.T) {
	var secondly atomic.Int64
	c := memory.New(memory.WithScheduleHeap())
	for _, wf := range []workflow.Workflow{
		one("hourly", ok),
		one("secondly", func(context.Context, []byte) ([]byte, error) { secondly.Add(1); return nil, nil }),
	} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "hourly", Cron: "0 0 * * * *", Seconds: true, Location: time.UTC}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the scheduler arms for the hour
	sched := workflow.Schedule{Workflow: "secondly", Cron: "* * * * * *", Seconds: true, Name: "s", Location: time.UTC}
	id, err := c.Schedule(ctx, sched)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the every-second schedule added after the hourly one fired", func() bool { return secondly.Load() >= 1 })
	if id2, err := c.Schedule(ctx, sched); err != nil || id2 != id {
		t.Fatalf("upsert = %q, %v; want %q", id2, err, id)
	}
	from := secondly.Load()
	time.Sleep(3 * time.Second)
	if got := secondly.Load() - from; got < 2 || got > 4 {
		t.Fatalf("in 3s after the upsert the schedule fired %d times, want about 3 (one entry)", got)
	}
	if err := c.Unschedule(ctx, id); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // a fire already under way may finish
	from = secondly.Load()
	time.Sleep(1500 * time.Millisecond)
	if got := secondly.Load() - from; got != 0 {
		t.Fatalf("the unscheduled schedule fired %d more times", got)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(pollFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
