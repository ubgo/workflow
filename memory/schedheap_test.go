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
				if grew < n {
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

// TestScheduleHeapRearms checks the heap's head bookkeeping: a schedule
// added with an earlier tick than the armed one fires on that tick, an
// unscheduled head stops firing and leaves no timer behind, and a named
// upsert replaces its entry rather than adding a second.
func TestScheduleHeapRearms(t *testing.T) {
	var hourly, minutely atomic.Int64
	c, fc := newClient(t, []workflow.Workflow{
		one("hourly", func(context.Context, []byte) ([]byte, error) { hourly.Add(1); return nil, nil }),
		one("minutely", func(context.Context, []byte) ([]byte, error) { minutely.Add(1); return nil, nil }),
	}, memory.WithScheduleHeap())
	ctx := context.Background()
	if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "hourly", Cron: "0 * * * *", Location: time.UTC}); err != nil {
		t.Fatal(err)
	}
	blockUntil(t, fc, 1)
	id, err := c.Schedule(ctx, workflow.Schedule{Workflow: "minutely", Cron: "* * * * *", Name: "m", Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	blockUntil(t, fc, 1)
	fc.Advance(time.Minute)
	waitFor(t, "the minutely schedule fired once", func() bool { return minutely.Load() == 1 })
	// Upsert the same name: still one entry for "minutely", so the next
	// minute fires it once, not twice.
	if id2, err := c.Schedule(ctx, workflow.Schedule{Workflow: "minutely", Cron: "* * * * *", Name: "m", Location: time.UTC}); err != nil || id2 != id {
		t.Fatalf("upsert = %q, %v; want %q", id2, err, id)
	}
	blockUntil(t, fc, 1)
	fc.Advance(time.Minute)
	waitFor(t, "the upserted schedule fired", func() bool { return minutely.Load() >= 2 })
	time.Sleep(50 * time.Millisecond)
	if got := minutely.Load(); got != 2 {
		t.Fatalf("after the upsert one minute fired %d runs, want 1", got-1)
	}
	if err := c.Unschedule(ctx, id); err != nil {
		t.Fatal(err)
	}
	blockUntil(t, fc, 1)
	fc.Advance(58 * time.Minute) // 11:00: the hourly tick
	waitFor(t, "the hourly schedule fired", func() bool { return hourly.Load() == 1 })
	time.Sleep(50 * time.Millisecond)
	if got := minutely.Load(); got != 2 {
		t.Fatalf("the unscheduled minutely schedule fired %d times, want 2", got)
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
