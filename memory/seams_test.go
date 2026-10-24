package memory_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/memory"
)

// hourlyAlias is a custom dialect: it knows one word, "every-hour", and
// refuses everything else, including the standard syntax, so a test can
// tell which parser ran.
func hourlyAlias(expr string) (cron.Schedule, error) {
	if expr == "every-hour" {
		return cron.Parse("0 * * * *")
	}
	return nil, errors.New("hourlyAlias: only every-hour")
}

func TestCronParserCustomAllows(t *testing.T) {
	var declared, scheduled atomic.Int32
	wf := one("declared", func(context.Context, []byte) ([]byte, error) { declared.Add(1); return nil, nil })
	wf.Cron = []string{"every-hour"}
	c, fc := newClient(t, []workflow.Workflow{wf, one("sched", func(context.Context, []byte) ([]byte, error) {
		scheduled.Add(1)
		return nil, nil
	})}, memory.WithCronParser(hourlyAlias))
	if _, err := c.Schedule(context.Background(), workflow.Schedule{Workflow: "sched", Cron: "every-hour", Location: time.UTC}); err != nil {
		t.Fatalf("Schedule in the custom dialect: %v", err)
	}
	blockUntil(t, fc, 2)
	fc.Advance(time.Hour)
	waitCount(t, &declared, 1)
	waitCount(t, &scheduled, 1)
}

func TestCronParserCustomDenies(t *testing.T) {
	wf := one("std", ok)
	wf.Cron = []string{"0 * * * *"} // valid standard syntax the custom parser refuses
	c := memory.New(memory.WithCronParser(hourlyAlias))
	if err := c.Register(wf); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Register with a refused expression: %v, want ErrInvalid", err)
	}
	c2, _ := newClient(t, []workflow.Workflow{one("w", ok)}, memory.WithCronParser(hourlyAlias))
	_, err := c2.Schedule(context.Background(), workflow.Schedule{Workflow: "w", Cron: "0 * * * *", Location: time.UTC})
	if !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Schedule with a refused expression: %v, want ErrInvalid", err)
	}
}

func TestCronParserCustomFailsClosed(t *testing.T) {
	broken := func(string) (cron.Schedule, error) { return nil, nil } // breaks the Parser contract
	wf := one("w", ok)
	wf.Cron = []string{"0 * * * *"}
	if err := memory.New(memory.WithCronParser(broken)).Register(wf); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a parser returning (nil, nil): Register = %v, want ErrInvalid", err)
	}
	c, _ := newClient(t, []workflow.Workflow{one("w", ok)}, memory.WithCronParser(broken))
	if _, err := c.Schedule(context.Background(), workflow.Schedule{Workflow: "w", Cron: "0 * * * *", Location: time.UTC}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a parser returning (nil, nil): Schedule = %v, want ErrInvalid", err)
	}
}

func TestCronParserDefaultUnchanged(t *testing.T) {
	wf := one("w", ok)
	wf.Cron = []string{"every-hour"}
	if err := memory.New(memory.WithCronParser(nil)).Register(wf); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("the default parser accepted a custom word: %v", err)
	}
}

func TestJitterSourcePinsTheDelay(t *testing.T) {
	var attempts atomic.Int32
	wf := one("flaky", func(context.Context, []byte) ([]byte, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("transient")
		}
		return nil, nil
	})
	// Jitter 0.5 with u = 0.5 turns 40m into 30m.
	wf.Retry = workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: 40 * time.Minute, Jitter: 0.5}}
	c, fc := newClient(t, []workflow.Workflow{wf}, memory.WithJitterSource(func() float64 { return 0.5 }))
	ref := enqueue(t, c, workflow.Job{Workflow: "flaky"})
	blockUntil(t, fc, 1)
	fc.Advance(30*time.Minute - time.Second)
	time.Sleep(10 * time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatal("retried before the jittered 30m delay")
	}
	fc.Advance(time.Second)
	waitStatus(t, c, ref, workflow.StatusSucceeded)
}

func TestRetryAfterOverridesBackoff(t *testing.T) {
	var attempts atomic.Int32
	wf := one("limited", func(context.Context, []byte) ([]byte, error) {
		if attempts.Add(1) == 1 {
			return nil, workflow.RetryAfter(errors.New("429"), 2*time.Hour)
		}
		return nil, nil
	})
	wf.Retry = workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: time.Minute}, DelayFor: workflow.RetryAfterDelay}
	c, fc := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "limited"})
	blockUntil(t, fc, 1)
	fc.Advance(time.Hour) // the 1m backoff would have retried long ago
	time.Sleep(10 * time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatal("retried on the backoff instead of the 2h RetryAfter")
	}
	fc.Advance(time.Hour)
	waitStatus(t, c, ref, workflow.StatusSucceeded)
}

func TestDeclaredCronMisfireAndZone(t *testing.T) {
	var fired atomic.Int32
	wf := one("backfill", func(context.Context, []byte) ([]byte, error) { fired.Add(1); return nil, nil })
	wf.Cron = []string{"0 * * * *"}
	wf.CronLocation = time.FixedZone("UTC+0530", 5*3600+30*60) // hourly ticks fall at :30 UTC
	wf.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 24}
	_, fc := newClient(t, []workflow.Workflow{wf})
	blockUntil(t, fc, 1)
	// start is 10:00 UTC; ticks at 10:30, 11:30, 12:30 UTC are all missed.
	fc.Advance(2*time.Hour + 45*time.Minute)
	waitCount(t, &fired, 3)
}

func TestScheduleRefusesReservedLabels(t *testing.T) {
	c, _ := newClient(t, []workflow.Workflow{one("w", ok)})
	_, err := c.Schedule(context.Background(), workflow.Schedule{Workflow: "w", Cron: "0 * * * *", Location: time.UTC, Labels: map[string]string{workflow.LabelPrefixOTel + "x": "y"}})
	if !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Schedule with a reserved label: %v, want ErrInvalid", err)
	}
}
