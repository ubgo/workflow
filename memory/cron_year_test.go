package memory_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/memory"
)

// TestCronYearThroughParser runs a seven-field expression with a year
// through memory.WithCronParser(cron.ParseYear): it fires on its two days
// of 2026 at noon in Berlin, then the schedule has no next fire, its timer
// is gone and a year passing fires nothing more.
func TestCronYearThroughParser(t *testing.T) {
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	var fired atomic.Int32
	c, fc := newClient(t, []workflow.Workflow{one("campaign", func(context.Context, []byte) ([]byte, error) {
		fired.Add(1)
		return nil, nil
	})}, memory.WithCronParser(cron.ParseYear))
	_, err = c.Schedule(context.Background(), workflow.Schedule{
		Workflow: "campaign",
		Cron:     "0 0 12 5,6 JAN * 2026", // second minute hour day month weekday year
		Location: berlin,
	})
	if err != nil {
		t.Fatalf("Schedule with a year: %v", err)
	}
	infos, err := c.Schedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 1, 5, 12, 0, 0, 0, berlin); len(infos) != 1 || !infos[0].Next.Equal(want) {
		t.Fatalf("Schedules = %+v, want one with Next %v", infos, want)
	}

	blockUntil(t, fc, 1)
	fc.Advance(time.Hour) // start is 10:00 UTC; noon in Berlin is 11:00 UTC
	waitCount(t, &fired, 1)
	blockUntil(t, fc, 1)
	fc.Advance(24 * time.Hour)
	waitCount(t, &fired, 2)

	// The last fire of 2026 is past: the schedule's loop ends and leaves
	// no timer behind.
	deadline := time.Now().Add(pollFor)
	for fc.Pending() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := fc.Pending(); n != 0 {
		t.Fatalf("%d timers pending after the last year", n)
	}
	infos, err = c.Schedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || !infos[0].Next.IsZero() {
		t.Fatalf("Schedules after the last fire = %+v, want a zero Next", infos)
	}
	fc.Advance(366 * 24 * time.Hour)
	time.Sleep(10 * time.Millisecond)
	if n := fired.Load(); n != 2 {
		t.Fatalf("fired %d times, want 2", n)
	}
}
