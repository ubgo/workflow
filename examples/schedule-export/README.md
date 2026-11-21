# schedule-export

One schedule, 03:00 on weekdays in Berlin, written as files other schedulers run: a Kubernetes CronJob, a systemd timer and its service, a Windows Task Scheduler task, a launchd plist, and the one-shot files (launchd, systemd, Task Scheduler) a desktop app leaves behind for its next fire. Then what the formats refuse, and a seven-field cron with a year that stops after its last year. Everything goes to standard output; nothing is installed.

```sh
cd examples
go run ./schedule-export
```

The output starts with the manifest:

```text
kubernetes:
  == nightly-report.yaml
  apiVersion: batch/v1
  kind: CronJob
  metadata:
    name: "nightly-report"
    namespace: "jobs"
  spec:
    schedule: "0 3 * * 1-5"
    timeZone: "Europe/Berlin"
    concurrencyPolicy: Forbid
    suspend: false
```

and ends with the refusals and the year field:

```text
refusals:
  kubernetes refuses Misfire: true
  schedexport: systemd cannot express Overlap: a running service is never started twice, so a tick during a run is skipped; set Overlap to OverlapSkip
  schedexport: taskscheduler cannot express Cron: "@every 90s" has no calendar fields (an @every interval counts from the Unix epoch, and a custom parser's schedule offers none); write it as a cron expression
year field:
  Fri 1 Jan 2027 09:00
  Thu 1 Jul 2027 09:00
  Sat 1 Jan 2028 09:00
  Sat 1 Jul 2028 09:00
  then never
  OnCalendar=2027..2028-01,07-01 09:00:00 UTC
```

## The code

The schedule and the command the files run:

<!-- snippet: examples/schedule-export/main.go#schedule -->
```go
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
```

A Kubernetes CronJob:

<!-- snippet: examples/schedule-export/main.go#cronjob -->
```go
f, err := schedexport.CronJob(report, app, schedexport.CronJobOptions{
	Image:     "ghcr.io/example/app:1.4.2",
	Namespace: "jobs",
})
if err != nil {
	return err
}
// The app writes f.Data to f.Name and runs kubectl apply -f on it.
```

A systemd timer and service:

<!-- snippet: examples/schedule-export/main.go#systemd -->
```go
files, err := schedexport.SystemdTimer(report, app) // the .timer, then the .service
if err != nil {
	return err
}
```

The repeating launchd plist, one entry per weekday:

<!-- snippet: examples/schedule-export/main.go#launchd -->
```go
f, err := schedexport.Launchd(report, app, schedexport.LaunchdOptions{
	Zone: berlin, // the zone the Mac runs in; it must be the schedule's
})
if err != nil {
	return err
}
```

The next fire handed to launchd, on the Mac's zone:

<!-- snippet: examples/schedule-export/main.go#handoff -->
```go
mac, err := cron.LoadZone("America/New_York") // the Mac's zone
if err != nil {
	return err
}
f, err := schedexport.LaunchdNextFire(report, app, schedexport.LaunchdOptions{Zone: mac}, clk)
if err != nil {
	return err
}
```

The same handoff for systemd and Task Scheduler, written as a UTC instant:

<!-- snippet: examples/schedule-export/main.go#handoff-utc -->
```go
units, err := schedexport.SystemdTimerNextFire(report, app, clk) // a one-shot .timer, then its .service
if err != nil {
	return err
}
task, err := schedexport.TaskSchedulerNextFire(report, app, clk) // one TimeTrigger
if err != nil {
	return err
}
```

A schedule that never fires again has nothing to hand over:

<!-- snippet: examples/schedule-export/main.go#no-next-fire -->
```go
ended := report
ended.Active = workflow.Active{Until: monday} // the campaign ended this morning
_, err = schedexport.SystemdTimerNextFire(ended, app, clk)
if errors.Is(err, schedexport.ErrNoNextFire) {
	fmt.Println("  no next fire: uninstall the timer") // nothing to hand over
}
```

A refusal names the format and the field:

<!-- snippet: examples/schedule-export/main.go#refusals -->
```go
backfill := report
backfill.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 7} // replay up to a week of missed ticks
_, err := schedexport.CronJob(backfill, app, schedexport.CronJobOptions{Image: "app"})
var ne *schedexport.NotExpressibleError
if errors.As(err, &ne) {
	fmt.Printf("  %s refuses %s: %v\n", ne.Format, ne.Field, errors.Is(err, schedexport.ErrNotExpressible))
}
```

The guide that copies this program is [export a schedule](../../docs/schedules/export.md). `go test ./schedule-export` checks every line.
