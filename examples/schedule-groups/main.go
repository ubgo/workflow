// Command schedule-groups gives several schedules the same options with a
// small Go helper instead of a group type: one zone, one time-of-day
// window, one misfire policy and one group label. The label then finds
// the group in the schedule list, filters the runs its ticks started, and
// pauses every schedule of the group at once. It runs on the in-memory
// driver with a fake clock.
//
// It is the program behind the "Group schedules" section of
// docs/schedules/manage.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// groupLabel is the label key that marks a schedule's group, and
// officeGroup the group this program builds.
const (
	groupLabel  = "group"
	officeGroup = "office-hours"
)

// tickWait bounds each wait for a tick or a run to land; the memory driver
// takes well under a millisecond.
const tickWait = 5 * time.Second

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "schedule-groups:", err)
		os.Exit(1)
	}
}

// snippet:helper
// officeHours builds a schedule of the office-hours group: every schedule
// it returns shares the zone, the window, the misfire policy and the group
// label, so changing one of them here changes the whole group.
func officeHours(berlin *time.Location, name, workflowName, expr string) workflow.Schedule {
	return workflow.Schedule{
		Name:     name,
		Workflow: workflowName,
		Cron:     expr,
		Location: berlin,
		Windows:  workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "18:00"}}},
		Misfire:  workflow.Misfire{Policy: workflow.MisfireSkip},
		Labels:   map[string]string{groupLabel: officeGroup},
	}
}

// snippet:end

// noop is the scheduled step.
func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		return err
	}
	// Monday 5 January 2026, 08:30 in Berlin: the next hourly tick is 09:00.
	clk := clock.NewFake(time.Date(2026, 1, 5, 8, 30, 0, 0, berlin))
	c := memory.New(memory.WithClock(clk))
	for _, name := range []string{"sync-orders", "send-digest", "backup"} {
		if err := c.Register(workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "run", Run: noop}}}); err != nil {
			return err
		}
	}
	fired := make(chan workflow.Tick, 8)
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
	defer c.Stop()

	// snippet:schedule
	for _, s := range []workflow.Schedule{
		officeHours(berlin, "orders-hourly", "sync-orders", "0 * * * *"),
		officeHours(berlin, "digest-hourly", "send-digest", "0 * * * *"),
	} {
		if _, err := c.Schedule(ctx, s); err != nil {
			return err
		}
	}
	// Not in the group: built without the helper.
	if _, err := c.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "backup", Cron: "0 2 * * *", Location: berlin}); err != nil {
		return err
	}
	// snippet:end

	fmt.Fprintln(out, "-- the group in the schedule list")
	if err := listGroup(ctx, c, out); err != nil {
		return err
	}

	fmt.Fprintln(out, "-- 09:00: both schedules of the group fire")
	if err := clk.BlockUntil(ctx, 3); err != nil { // one timer per schedule
		return err
	}
	clk.Advance(30 * time.Minute)
	for range 2 {
		select {
		case <-fired:
		case <-time.After(tickWait):
			return errors.New("the group did not fire twice")
		}
	}

	// snippet:runs
	// Fired runs carry their schedule's labels, so the group label finds them.
	page, err := c.Runs(ctx, workflow.RunQuery{Labels: map[string]string{groupLabel: officeGroup}})
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintln(out, "runs of the group:", len(page.Runs))

	fmt.Fprintln(out, "-- pause the group")
	// snippet:pause
	infos, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range infos {
		if s.Labels[groupLabel] != officeGroup {
			continue
		}
		if err := c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: s.ID}); err != nil {
			return err
		}
	}
	// snippet:end
	return listGroup(ctx, c, out)
}

// listGroup prints every schedule with its group and next fire time.
func listGroup(ctx context.Context, c workflow.ScheduleLister, out io.Writer) error {
	// snippet:list
	infos, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range infos {
		group := s.Labels[groupLabel]
		if group == "" {
			group = "-"
		}
		next := "paused"
		if !s.Paused {
			next = "next " + s.Next.In(s.Location).Format("15:04 MST")
		}
		fmt.Fprintf(out, "  %-13s %-12s %s\n", s.Name, group, next)
	}
	// snippet:end
	return nil
}
