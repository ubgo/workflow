// Command schedule-export writes one schedule as files other schedulers
// run: a Kubernetes CronJob, a systemd timer and service, a Windows Task
// Scheduler task, and the one-shot launchd handoff a desktop app leaves
// behind when it quits. It also shows what each format refuses, and the
// seven-field cron with a year. It writes to standard output only, so
// nothing needs to be installed.
//
// It is the program behind docs/schedules/export.md and the year field
// section of docs/schedules/cron.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/schedexport"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "schedule-export:", err)
		os.Exit(1)
	}
}

// monday is the fixed time the program's clock reads: Monday 5 January
// 2026, 10:00 UTC.
var monday = time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)

// run is main without the process exit, so the test drives the same code.
func run(context.Context) error {
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		return err
	}
	// snippet:schedule
	report := workflow.Schedule{
		Name:     "nightly-report",
		Workflow: "report",
		Cron:     "0 3 * * MON-FRI", // 03:00 on weekdays
		Location: berlin,
		Overlap:  workflow.OverlapSkip,                               // never two at once
		Misfire:  workflow.Misfire{Policy: workflow.MisfireFireOnce}, // catch up once
	}
	app := schedexport.Target{
		Command: []string{"/usr/local/bin/app", "run-schedule"},
		Args:    []string{"nightly-report"},
	}
	// snippet:end
	for _, part := range []func() error{
		func() error { return kubernetes(report, app) },
		func() error { return systemd(report, app) },
		func() error { return taskScheduler(report, app, berlin) },
		func() error { return launchd(report, app, berlin) },
		func() error { return handoff(report, app) },
		func() error { return refusals(report, app) },
		yearField,
	} {
		if err := part(); err != nil {
			return err
		}
	}
	return nil
}

// kubernetes prints the schedule as a Kubernetes CronJob manifest.
func kubernetes(report workflow.Schedule, app schedexport.Target) error {
	fmt.Println("kubernetes:")
	// snippet:cronjob
	f, err := schedexport.CronJob(report, app, schedexport.CronJobOptions{
		Image:     "ghcr.io/example/app:1.4.2",
		Namespace: "jobs",
	})
	if err != nil {
		return err
	}
	// The app writes f.Data to f.Name and runs kubectl apply -f on it.
	// snippet:end
	show(f)
	return nil
}

// systemd prints the schedule as a systemd timer and the service it starts.
func systemd(report workflow.Schedule, app schedexport.Target) error {
	fmt.Println("systemd:")
	// snippet:systemd
	files, err := schedexport.SystemdTimer(report, app) // the .timer, then the .service
	if err != nil {
		return err
	}
	// snippet:end
	for _, f := range files {
		show(f)
	}
	return nil
}

// taskScheduler prints an office-hours variant of the schedule as a Windows
// Task Scheduler task, to show a repeating trigger.
func taskScheduler(report workflow.Schedule, app schedexport.Target, berlin *time.Location) error {
	fmt.Println("task scheduler:")
	report.Cron = "*/15 9-17 * * MON-FRI" // every 15 minutes in office hours
	// snippet:tasks
	f, err := schedexport.TaskScheduler(report, app, schedexport.TaskSchedulerOptions{
		Zone: berlin, // the zone the Windows machine runs in
	})
	if err != nil {
		return err
	}
	// snippet:end
	show(f)
	return nil
}

// handoff prints the one-shot files a desktop app leaves behind when it
// quits (launchd, systemd, Task Scheduler) for the next fire on a fixed
// clock, and shows ErrNoNextFire once the schedule has ended.
func handoff(report workflow.Schedule, app schedexport.Target) error {
	fmt.Println("handoff:")
	report.Misfire = workflow.Misfire{} // launchd has its own catch-up rule
	clk := clock.NewFake(monday)        // the app passes its own clock
	// snippet:handoff
	mac, err := cron.LoadZone("America/New_York") // the Mac's zone
	if err != nil {
		return err
	}
	f, err := schedexport.LaunchdNextFire(report, app, schedexport.LaunchdOptions{Zone: mac}, clk)
	if err != nil {
		return err
	}
	// snippet:end
	show(f)

	// snippet:handoff-utc
	units, err := schedexport.SystemdTimerNextFire(report, app, clk) // a one-shot .timer, then its .service
	if err != nil {
		return err
	}
	task, err := schedexport.TaskSchedulerNextFire(report, app, clk) // one TimeTrigger
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Print(onCalendar(units[0]))
	fmt.Print(startBoundary(task))

	// snippet:no-next-fire
	ended := report
	ended.Active = workflow.Active{Until: monday} // the campaign ended this morning
	_, err = schedexport.SystemdTimerNextFire(ended, app, clk)
	if errors.Is(err, schedexport.ErrNoNextFire) {
		fmt.Println("  no next fire: uninstall the timer") // nothing to hand over
	}
	// snippet:end
	return nil
}

// launchd prints the StartCalendarInterval entries of the schedule as a
// launchd plist.
func launchd(report workflow.Schedule, app schedexport.Target, berlin *time.Location) error {
	fmt.Println("launchd:")
	report.Misfire = workflow.Misfire{} // launchd has its own catch-up rule
	// snippet:launchd
	f, err := schedexport.Launchd(report, app, schedexport.LaunchdOptions{
		Zone: berlin, // the zone the Mac runs in; it must be the schedule's
	})
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Print(calendarEntries(f))
	return nil
}

// refusals prints what each format cannot express: a fire-all misfire
// policy, overlapping runs and an @every interval.
func refusals(report workflow.Schedule, app schedexport.Target) error {
	fmt.Println("refusals:")
	// snippet:refusals
	backfill := report
	backfill.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 7} // replay up to a week of missed ticks
	_, err := schedexport.CronJob(backfill, app, schedexport.CronJobOptions{Image: "app"})
	var ne *schedexport.NotExpressibleError
	if errors.As(err, &ne) {
		fmt.Printf("  %s refuses %s: %v\n", ne.Format, ne.Field, errors.Is(err, schedexport.ErrNotExpressible))
	}
	// snippet:end
	allow := report
	allow.Overlap = workflow.OverlapAllow
	_, err = schedexport.SystemdTimer(allow, app)
	fmt.Println(" ", err)
	every := report
	every.Cron = "@every 90s"
	_, err = schedexport.TaskScheduler(every, app, schedexport.TaskSchedulerOptions{Zone: report.Location})
	fmt.Println(" ", err)
	return nil
}

// yearField walks a seven-field cron with a year until it ends, then exports
// it as a systemd timer through the year-aware parser.
func yearField() error {
	fmt.Println("year field:")
	// snippet:year
	s, err := cron.ParseYear("0 0 9 1 JAN,JUL * 2027-2028") // second minute hour day month weekday year
	if err != nil {
		return err
	}
	for t := s.Next(monday, time.UTC); !t.IsZero(); t = s.Next(t, time.UTC) {
		fmt.Println(" ", t.Format("Mon 2 Jan 2006 15:04"))
	}
	fmt.Println("  then never") // Next returns the zero time after the last year
	// snippet:end
	campaign := workflow.Schedule{Name: "campaign", Workflow: "mail", Cron: s.String(), Location: time.UTC, Overlap: workflow.OverlapSkip}
	// snippet:year-export
	files, err := schedexport.SystemdTimer(campaign, schedexport.Target{
		Command: []string{"/usr/local/bin/app", "run-schedule", "campaign"},
		Parser:  cron.ParseYear, // as the driver's WithCronParser
	})
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Print(onCalendar(files[0]))
	return nil
}

// show prints a file's name and its contents, indented.
func show(f schedexport.File) {
	fmt.Printf("  == %s\n", f.Name)
	for _, line := range strings.Split(strings.TrimSuffix(string(f.Data), "\n"), "\n") {
		if line == "" {
			fmt.Println()
			continue
		}
		fmt.Println("  " + line)
	}
}

// onCalendar returns the OnCalendar= lines of a timer, indented.
func onCalendar(f schedexport.File) string {
	var out strings.Builder
	for _, line := range strings.Split(string(f.Data), "\n") {
		if strings.HasPrefix(line, "OnCalendar=") {
			out.WriteString("  " + line + "\n")
		}
	}
	return out.String()
}

// startBoundary returns the StartBoundary line of a task, indented.
func startBoundary(f schedexport.File) string {
	for _, line := range strings.Split(string(f.Data), "\n") {
		if strings.Contains(line, "<StartBoundary>") {
			return "  " + strings.TrimSpace(line) + "\n"
		}
	}
	return ""
}

// calendarEntries summarises a plist's StartCalendarInterval entries, one
// line per entry, as key=value pairs.
func calendarEntries(f schedexport.File) string {
	var out strings.Builder
	var entry []string
	key := ""
	for _, line := range strings.Split(string(f.Data), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "<dict>":
			entry = nil
		case strings.HasPrefix(line, "<key>"):
			key = strings.TrimSuffix(strings.TrimPrefix(line, "<key>"), "</key>")
		case strings.HasPrefix(line, "<integer>"):
			entry = append(entry, key+"="+strings.TrimSuffix(strings.TrimPrefix(line, "<integer>"), "</integer>"))
		case line == "</dict>" && len(entry) > 0:
			out.WriteString("  " + strings.Join(entry, " ") + "\n")
			entry = nil
		}
	}
	return out.String()
}
