// Command latest-only skips the work of scheduled runs that are no longer
// the newest, with middleware.LatestOnly, on the in-memory driver with a
// fake clock. An hourly cache refresh runs on time at 09:00; then the
// process is down from 09:30 to 12:30 and the misfire policy fires the
// three missed ticks at once. Only the newest of them (12:00) refreshes
// the cache: the 10:00 and 11:00 runs see that a newer tick is already due
// and return without calling the handler.
//
// It is the program behind docs/schedules/latest-only.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// start is Monday 5 January 2026, 08:30 UTC: the first hourly tick is
// 09:00.
var start = time.Date(2026, 1, 5, 8, 30, 0, 0, time.UTC)

// outage is how long the process is down after the 09:00 run: from 09:30
// to 12:30 it misses the 10:00, 11:00 and 12:00 ticks.
const outage = 3 * time.Hour

// catchUp is the misfire policy's bound on missed ticks: all three of the
// outage fire on return, so LatestOnly has stale runs to skip.
const catchUp = 3

// runWait bounds each wait for the fired runs to end; the memory driver
// takes well under a millisecond.
const runWait = 5 * time.Second

// toFirstTick moves the clock from start (08:30) past the 09:00 tick, to
// 09:30, when the outage begins.
const toFirstTick = 30 * time.Minute

// sinceTick is how long after the 09:00 tick the outage begins (09:30):
// added to outage it brings the clock back up at 12:30.
const sinceTick = 30 * time.Minute

// hourFormat prints ticks and clock readings as a wall time.
const hourFormat = "15:04"

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "latest-only:", err)
		os.Exit(1)
	}
}

// journal records what each run did, keyed by its tick, so the program
// prints them in tick order however the worker interleaves the runs.
type journal struct {
	mu    sync.Mutex
	lines map[time.Time]string
	ended chan struct{}
}

// add records what the run of tick did and signals that the run ended.
func (j *journal) add(tick time.Time, line string) {
	j.mu.Lock()
	j.lines[tick] = line
	j.mu.Unlock()
	j.ended <- struct{}{}
}

// flush waits for n runs to end, then writes their lines in tick order and
// forgets them.
func (j *journal) flush(w io.Writer, n int) error {
	for range n {
		select {
		case <-j.ended:
		case <-time.After(runWait):
			return errors.New("a fired run did not end in time")
		}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, tick := range slices.SortedFunc(func(yield func(time.Time) bool) {
		for t := range j.lines {
			if !yield(t) {
				return
			}
		}
	}, time.Time.Compare) {
		fmt.Fprintf(w, "  %s\n", j.lines[tick])
	}
	clear(j.lines)
	return nil
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))
	need := workflow.CapsOf(workflow.CapTickTime, workflow.CapScheduleList, workflow.CapMisfireFireAll)
	if missing := c.Capabilities().Missing(need); !missing.IsZero() {
		return fmt.Errorf("the driver lacks %s", missing)
	}
	j := &journal{lines: map[time.Time]string{}, ended: make(chan struct{}, catchUp+1)}

	// snippet:handler
	// refresh is the expensive work: rebuild the cache from the source.
	refresh := func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		label := "none"
		tick, ok := info.TickAt() // false for a run no schedule fired
		if ok {
			label = tick.Format(hourFormat)
		}
		j.add(tick, fmt.Sprintf("tick %s, run at %s: refreshed the cache", label, clk.Now().Format(hourFormat)))
		return in, nil
	}
	// snippet:end

	// snippet:latest
	latest := middleware.LatestOnly(middleware.LatestOnlyConfig{
		Schedules: c,   // the client lists its schedules (CapScheduleList)
		Clock:     clk, // the driver's clock, so a fake clock decides in tests
		OnSkip: func(ctx context.Context, info workflow.StepInfo, tick, newer time.Time) {
			j.add(tick, fmt.Sprintf("tick %s, run at %s: skipped, the %s tick is due", tick.Format(hourFormat), clk.Now().Format(hourFormat), newer.Format(hourFormat)))
		},
	})
	err := c.Register(workflow.Workflow{Name: "refresh-cache", Steps: []workflow.Step{
		{Name: "refresh", Run: latest(refresh)}, // only this step's work is skipped
	}})
	// snippet:end
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer c.Stop()

	// snippet:schedule
	_, err = c.Schedule(ctx, workflow.Schedule{
		Name:     "hourly",
		Workflow: "refresh-cache",
		Cron:     "0 * * * *",
		Location: time.UTC,
		// After an outage, fire every missed tick (at most three), oldest first.
		Misfire: workflow.Misfire{Policy: workflow.MisfireFireAll, Max: catchUp},
	})
	// snippet:end
	if err != nil {
		return err
	}

	fmt.Fprintln(w, "-- on time")
	if err := clk.BlockUntil(ctx, 1); err != nil { // the 09:00 timer is armed
		return err
	}
	clk.Advance(toFirstTick)
	if err := j.flush(w, 1); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- after a three-hour outage")
	if err := clk.BlockUntil(ctx, 1); err != nil { // the 10:00 timer is armed
		return err
	}
	clk.Advance(sinceTick + outage) // nothing fired from 09:30 to 12:30
	if err := j.flush(w, catchUp); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- not fired by a schedule")
	// snippet:manual
	// A run enqueued by hand has no tick, so LatestOnly always lets it work.
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "refresh-cache"}); err != nil {
		return err
	}
	// snippet:end
	return j.flush(w, 1)
}
