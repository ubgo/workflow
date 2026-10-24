// Command failover-schedules shows where a failover client keeps its
// schedules. By default a schedule lives on the primary alone, so each
// tick fires once (and none fire while the primary is down). With
// failover.WithSchedulesOnBoth it lives on both backends, so it keeps
// firing during an outage, at the price of every tick firing twice while
// both are up. Both backends are the in-memory driver on one fake clock.
//
// It is the program behind the "Schedules" section of
// docs/reliability/failover.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/memory"
)

// start is Monday 5 January 2026, 08:30 UTC: the next hourly tick is
// 09:00.
var start = time.Date(2026, 1, 5, 8, 30, 0, 0, time.UTC)

// tickWait bounds each wait for a tick; the memory driver takes well under
// a millisecond.
const tickWait = 5 * time.Second

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "failover-schedules:", err)
		os.Exit(1)
	}
}

var report = workflow.Workflow{Name: "report", Steps: []workflow.Step{{
	Name: "build",
	Run:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
}}}

// side is the backend a prefixed ID names: the part before the first colon.
func side(id string) string {
	s, _, _ := strings.Cut(id, ":")
	return s
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "-- default: schedules on the primary only")
	if err := demo(ctx, out); err != nil {
		return err
	}
	fmt.Fprintln(out, "-- WithSchedulesOnBoth: schedules on both backends")
	return demo(ctx, out, failover.WithSchedulesOnBoth())
}

// demo makes one hourly schedule through a failover client built with
// opts, lists it, lets one tick fall due, and pauses the schedule.
func demo(ctx context.Context, out io.Writer, opts ...failover.Option) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)
	primary := memory.New(memory.WithClock(clk))
	secondary := memory.New(memory.WithClock(clk))
	defer primary.Stop()
	defer secondary.Stop()

	// snippet:new
	c := failover.New(primary, secondary, append(opts, failover.WithClock(clk))...)
	// snippet:end
	if err := c.Register(report); err != nil {
		return err
	}
	fired := make(chan workflow.Tick, 4)
	// Tick observers follow the schedules: the primary's alone by default.
	if err := c.OnTick(func(_ context.Context, t workflow.Tick) {
		if t.Outcome == workflow.TickFired {
			fired <- t
		}
	}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// snippet:schedule
	id, err := c.Schedule(ctx, workflow.Schedule{Name: "hourly", Workflow: "report", Cron: "0 * * * *", Location: time.UTC})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "Schedule returned an ID for:", side(string(id)))
	infos, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range infos { // one entry per copy, its ID prefixed by the backend
		fmt.Fprintf(out, "  listed on %s\n", side(string(s.ID)))
	}
	// snippet:end

	copies := len(infos) // one timer per copy of the schedule
	if err := clk.BlockUntil(ctx, copies); err != nil {
		return err
	}
	clk.Advance(30 * time.Minute)
	var sides []string
	for range copies {
		select {
		case t := <-fired:
			sides = append(sides, side(string(t.ScheduleID)))
		case <-time.After(tickWait):
			return errors.New("a copy of the schedule did not fire")
		}
	}
	slices.Sort(sides)
	fmt.Fprintf(out, "09:00 fired on: %s\n", strings.Join(sides, ", "))

	// snippet:pause
	// The ID Schedule returned pauses every copy; a listed ID pauses one.
	if err := c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		return err
	}
	// snippet:end
	if infos, err = c.Schedules(ctx); err != nil {
		return err
	}
	for _, s := range infos {
		fmt.Fprintf(out, "  %s paused: %v\n", side(string(s.ID)), s.Paused)
	}
	return nil
}
