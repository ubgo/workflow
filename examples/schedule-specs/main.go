// Command schedule-specs shows the ways to write and store a schedule
// beyond a five-field cron string: a structured calendar spec, the opt-in
// seconds dialect, zones loaded by name, the next runs shown in two
// zones, an active period, environment gating, a schedule stored as JSON,
// and a per-node LocalCron on a fake clock.
//
// It is the program behind the calendar, seconds, zone, next-runs, active,
// environment, JSON and local-cron sections of docs/schedules/: the guides
// copy its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "schedule-specs:", err)
		os.Exit(1)
	}
}

// monday is the reference instant every listing starts from: Monday 12
// October 2026, 00:00 UTC.
var monday = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	steps := []func(io.Writer) error{calendar, seconds, zones, nextRuns, active, environment, storeJSON}
	for _, step := range steps {
		if err := step(out); err != nil {
			return err
		}
	}
	return localCron(ctx, out)
}

// calendar compiles a structured spec to the expression a person would
// write, and shows the refusal of both day fields.
func calendar(out io.Writer) error {
	fmt.Fprintln(out, "calendar:")
	// snippet:calendar
	spec := cron.Calendar{
		Hour:      []int{9},
		Minute:    []int{0, 30},
		DayOfWeek: []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday},
	}
	expr, err := spec.Expr() // "0,30 9 * * 1-5", a plain cron string every driver takes
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "  %s\n", expr)
	_, err = cron.Calendar{DayOfMonth: []int{13}, DayOfWeek: []time.Weekday{time.Friday}}.Expr()
	fmt.Fprintf(out, "  ErrCalendar=%t\n", errors.Is(err, cron.ErrCalendar))
	return nil
}

// seconds parses the six-field dialect and lists its first fires.
func seconds(out io.Writer) error {
	fmt.Fprintln(out, "seconds:")
	// snippet:seconds
	s, err := cron.ParseSeconds("*/20 * * * * *") // second minute hour day month weekday
	if err != nil {
		return err
	}
	// snippet:end
	for _, t := range s.Between(monday, monday.Add(time.Minute), time.UTC, 3) {
		fmt.Fprintf(out, "  %s\n", t.Format("15:04:05"))
	}
	return nil
}

// zones loads zones by name and shows what LoadZone refuses.
func zones(out io.Writer) error {
	fmt.Fprintln(out, "zones:")
	// snippet:zones
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		return err // fails at boot, not as a schedule firing at the wrong hour
	}
	// snippet:end
	fmt.Fprintf(out, "  loaded %s\n", berlin)
	for _, name := range []string{"", "Local", "Europe/Berln"} {
		_, err := cron.LoadZone(name)
		fmt.Fprintf(out, "  %q ErrZone=%t\n", name, errors.Is(err, cron.ErrZone))
	}
	return nil
}

// nextRuns lists a Berlin schedule's next fires in its zone and in UTC.
func nextRuns(out io.Writer) error {
	fmt.Fprintln(out, "next runs:")
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		return err
	}
	// snippet:nextruns
	s, err := cron.Parse("0 9 * * *")
	if err != nil {
		return err
	}
	runs := workflow.NextRuns(workflow.NextRunsQuery{
		Schedule: s,
		Location: berlin,
		After:    time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC), // the caller's clock; Berlin leaves summer time on the 25th
		Count:    3,
	})
	for _, r := range runs {
		fmt.Fprintf(out, "  %s\n", r) // "2026-10-24 09:00:00 CEST (07:00:00 UTC)"
	}
	// snippet:end
	return nil
}

// active lists the fires of a schedule limited to one week.
func active(out io.Writer) error {
	fmt.Fprintln(out, "active:")
	s, err := cron.Parse("0 9 * * *")
	if err != nil {
		return err
	}
	// snippet:active
	campaign := workflow.Active{
		From:  time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC),
		Until: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), // exclusive
	}
	runs := workflow.NextRuns(workflow.NextRunsQuery{Schedule: s, Location: time.UTC, After: monday, Count: 10, Active: campaign})
	// snippet:end
	for _, r := range runs {
		fmt.Fprintf(out, "  %s\n", r.Local.Format("Mon 2 Jan 15:04"))
	}
	return nil
}

// environment builds a ScheduleWhen that fires only in production.
func environment(out io.Writer) error {
	fmt.Fprintln(out, "environment:")
	for _, env := range []string{"production", "staging"} {
		// snippet:environment
		when := workflow.AllWhen(
			workflow.InEnvironment(env, "production"), // env is the app's own setting
			func(_ context.Context, t workflow.Tick) bool { return t.At.Weekday() != time.Sunday },
		)
		// snippet:end
		fmt.Fprintf(out, "  %s fires=%t\n", env, when(context.Background(), workflow.Tick{At: monday}))
	}
	return nil
}

// storeJSON writes a schedule as JSON and reads it back.
func storeJSON(out io.Writer) error {
	fmt.Fprintln(out, "json:")
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		return err
	}
	// snippet:json
	data, err := json.Marshal(workflow.Schedule{
		Name:     "nightly-report",
		Workflow: "report",
		Cron:     "0 2 * * *",
		Location: berlin, // stored by its IANA name
		Misfire:  workflow.Misfire{Policy: workflow.MisfireSkip},
	})
	if err != nil {
		return err
	}
	var back workflow.Schedule
	if err := json.Unmarshal(data, &back); err != nil { // refuses another version, unknown fields and zones
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "  %s\n", data)
	fmt.Fprintf(out, "  back: %s %s %s\n", back.Name, back.Cron, back.Location)
	_, err = workflow.UnmarshalSchedule([]byte(`{"v":2,"name":"x"}`), nil)
	fmt.Fprintf(out, "  v2 ErrScheduleJSON=%t\n", errors.Is(err, workflow.ErrScheduleJSON))
	return nil
}

// localCron runs a per-node cron on a fake clock for three minutes.
func localCron(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "local cron:")
	clk := clock.NewFake(monday)
	ran := make(chan time.Time, 8)
	// snippet:localcron
	lc := workflow.NewLocalCron(
		workflow.WithLocalCronClock(clk),
		workflow.WithLocalCronErrors(func(name string, at time.Time, err error) {
			fmt.Fprintf(os.Stderr, "local cron %s at %s: %v\n", name, at, err)
		}),
	)
	err := lc.Add("warm-cache", "* * * * *", time.UTC, func(ctx context.Context, at time.Time) error {
		ran <- at // at is the tick, not the wall time the run started
		return nil
	})
	if err != nil {
		return err
	}
	// snippet:end
	cctx, cancel := context.WithCancel(ctx)
	if err := lc.Start(cctx); err != nil {
		cancel()
		return err
	}
	for range 3 {
		if err := clk.BlockUntil(ctx, 1); err != nil {
			cancel()
			return err
		}
		clk.Advance(time.Minute)
		select {
		case at := <-ran:
			fmt.Fprintf(out, "  ran for %s\n", at.Format("15:04"))
		case <-time.After(5 * time.Second):
			cancel()
			return errors.New("the local cron did not run")
		}
	}
	cancel()
	<-lc.Done()
	return nil
}
