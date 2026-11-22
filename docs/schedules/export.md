# Export a schedule to other schedulers

Some schedules should fire where your app does not run all the time. A team that runs cron on Kubernetes wants the schedules declared in code as CronJob manifests; a desktop app that the user closes cannot fire anything, so before it quits it hands its next fire time to the operating system, which starts it again then. The `schedexport` package writes a `workflow.Schedule` as those files, one [`schedexport.Format`](ds:block?id=schedexport-format-n224t5n3) each: a Kubernetes `batch/v1` CronJob ([`FormatKubernetes`](ds:block?id=schedexport-formatkubernetes-w8hwdk4e)), a systemd timer and service ([`FormatSystemd`](ds:block?id=schedexport-formatsystemd-64bz5hh3)), a launchd property list ([`FormatLaunchd`](ds:block?id=schedexport-formatlaunchd-pbkgytm7)) and a Windows Task Scheduler task ([`FormatTaskScheduler`](ds:block?id=schedexport-formattaskscheduler-dfgxw7y7)), each OS format also as a one-shot file for the next fire. It writes bytes and never touches the system; installing the files stays the app's job. Every field is mapped honestly: what a format cannot express exactly is [refused with an error that names the field](ds:block?id=schedexport-notexpressibleerror-nwwc7ap8), never written with that part dropped.

## Before you start

- `schedexport` is in the core module and imports only the standard library and the core ([a guard test](ds:block?id=schedexport-testguard-stdlibandcoreonly-xkunw5qa) checks it), so it needs no new dependency and no driver.
- The export covers when a schedule fires: `Cron`, `Location`, `Seconds`, `Misfire`, `Overlap` and, where a format has them, `Active` and a [year field](cron.md#a-year-field). What runs is the command you pass, usually your own binary with arguments naming the schedule. `Workflow`, `Input`, `Labels`, `Tenant` and `KeepRuns` describe the run your app enqueues and are the command's to carry; they are [not written into the files](ds:block?id=schedexport-prepare-w5mrnvgs).
- The example is [`examples/schedule-export`](../../examples/schedule-export). Run it with `go run ./schedule-export` from `examples/`.

## The schedule and the command

A [`schedexport.Target`](ds:block?id=schedexport-target-htxk8djj) names what the files run: `Command` (the program and its leading arguments), `Args`, an optional `Name` (the job, unit, label or task name; the schedule's `Name` when empty) and `Description`:

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

`Target.Parser` reads the expression when it is not the default dialect, as a driver's `WithCronParser` does: pass `cron.ParseYear` for a seven-field expression with a year. A schedule from another parser exports when its `cron.Schedule` offers its fields (`cron.FieldsOf`).

Each format returns a [`schedexport.File`](ds:block?id=schedexport-file-utggyyut) (or two for systemd) with the file name the format expects and its contents. A refusal is a [`*schedexport.NotExpressibleError`](ds:block?id=schedexport-notexpressibleerror-nwwc7ap8) matching [`schedexport.ErrNotExpressible`](ds:block?id=schedexport-errnotexpressible-6gamr9aa), with the `Format`, the `Field` and the reason; input a format cannot take at all (no command, a name it refuses such as a [systemd](ds:block?id=schedexport-systemdprepare-qtrfbgra) unit name with a space, a control character) [matches `workflow.ErrInvalid`](ds:block?id=schedexport-prepare-w5mrnvgs).

## Kubernetes CronJob

[`schedexport.CronJob`](ds:block?id=schedexport-cronjob-haa7av5t) writes a `batch/v1` CronJob manifest. [`CronJobOptions`](ds:block?id=schedexport-cronjoboptions-hzyhec52) carries what a schedule does not: the `Image` (required), `Namespace`, the `Container` name, `RestartPolicy` (`OnFailure` or `Never`) and `Suspend`:

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

The expression is [rewritten from its parsed fields](ds:block?id=schedexport-fivefields-zajpjqpq) (names, macros and Sunday as 7 resolved, `MON-FRI` becomes `1-5`), so the controller reads the same days the library does. The controller's parser combines the two day fields differently from ours when one is a stepped `*/2`, so such an expression is [refused](ds:block?id=schedexport-dayfields-2vyg9nm3) rather than written with another meaning. `Location` becomes [`spec.timeZone`](ds:block?id=schedexport-cronjob-haa7av5t) (left out for UTC); `OverlapSkip` becomes `concurrencyPolicy: Forbid`, and the default `Allow`. A `Command` overrides the image's entrypoint; `Args` are the container's args.

## systemd timer

[`schedexport.SystemdTimer`](ds:block?id=schedexport-systemdtimer-527u7nzp) writes `<name>.timer` and the oneshot `<name>.service` it starts:

<!-- snippet: examples/schedule-export/main.go#systemd -->
```go
files, err := schedexport.SystemdTimer(report, app) // the .timer, then the .service
if err != nil {
	return err
}
```

[`OnCalendar=`](ds:block?id=schedexport-oncalendar-39zvsdns) carries the zone's IANA name, so the timer fires on the schedule's wall clock; seconds and a year field are expressed. A day rule where either day may match (`0 0 1 * MON`, the [Vixie rule](cron.md#day-of-month-and-day-of-week)) becomes two `OnCalendar=` lines, because systemd ANDs a weekday with a date. [`AccuracySec=1s`](ds:block?id=schedexport-job-systemdunits-5b3buk4u) is set, since systemd's default lets a timer fire up to a minute late. The command is [quoted for `ExecStart=`](ds:block?id=schedexport-job-argvquoted-9n89ub9y), with `%` and `$` doubled so systemd does not expand them.

## Windows Task Scheduler

[`schedexport.TaskScheduler`](ds:block?id=schedexport-taskscheduler-xtvrpdcn) writes a task XML (schema 1.2) with calendar triggers. A calendar trigger runs on the machine's own clock, so [`TaskSchedulerOptions.Zone`](ds:block?id=schedexport-taskscheduleroptions-dzg46fkr) names that machine's zone and the schedule's `Location` must be the same:

<!-- snippet: examples/schedule-export/main.go#tasks -->
```go
f, err := schedexport.TaskScheduler(report, app, schedexport.TaskSchedulerOptions{
	Zone: berlin, // the zone the Windows machine runs in
})
if err != nil {
	return err
}
```

[Each time of day is a trigger](ds:block?id=schedexport-tasktimes-fvefju5j); times an equal gap apart (every 15 minutes from 09:00 to 17:45) become one trigger with a repetition, and more than [48 triggers](ds:block?id=schedexport-maxtriggers-chw2bwff) are refused. The [day fields](ds:block?id=schedexport-taskdays-6jx7h8xt) become a daily, weekly, monthly or monthly-by-weekday schedule. `Active` and a contiguous year field set the trigger's [start and end boundaries](ds:block?id=schedexport-taskbounds-up2zj8uf). Register the file with `Register-ScheduledTask -Xml (Get-Content -Raw <file>) -TaskName <name>`.

## launchd

[`schedexport.Launchd`](ds:block?id=schedexport-launchd-354shg5d) writes a property list with one `StartCalendarInterval` entry per combination of the restricted fields. launchd has no zone setting, so [`LaunchdOptions.Zone`](ds:block?id=schedexport-launchdoptions-9rtf8g3q) names the Mac's zone and the schedule's `Location` must be the same:

<!-- snippet: examples/schedule-export/main.go#launchd -->
```go
f, err := schedexport.Launchd(report, app, schedexport.LaunchdOptions{
	Zone: berlin, // the zone the Mac runs in; it must be the schedule's
})
if err != nil {
	return err
}
```

The weekday schedule becomes five entries, Monday to Friday at 03:00:

```text
launchd:
  Weekday=1 Hour=3 Minute=0
  Weekday=2 Hour=3 Minute=0
  Weekday=3 Hour=3 Minute=0
  Weekday=4 Hour=3 Minute=0
  Weekday=5 Hour=3 Minute=0
```

The [misfire policy must be zero](ds:block?id=schedexport-launchdprepare-h3ujpqu4) (launchd starts one run on wake for the ticks a sleeping Mac missed, and none for a Mac that was off, which no policy describes) and `Overlap` must be `OverlapSkip`. Seconds, years, `Active` and windows have no place in a plist and are refused, and so is a schedule that needs more than [1000 entries](ds:block?id=schedexport-maxintervals-szcdhs5a). Load it with `launchctl bootstrap gui/<uid> <file>`.

## Hand the next fire to the OS

A desktop app that the user closes cannot fire its schedules. Before it quits, it writes the schedule's next fire as a one-shot file and installs it; when the OS starts the app at that time, the app runs the schedule and writes the next one. [`SystemdTimerNextFire`](ds:block?id=schedexport-systemdtimernextfire-bbkt6p5a), [`LaunchdNextFire`](ds:block?id=schedexport-launchdnextfire-nynthf6k) and [`TaskSchedulerNextFire`](ds:block?id=schedexport-taskschedulernextfire-9kprgebd) compute that time with the clock you pass (`clock.Real{}` in production), [inside the schedule's `Active` period, windows and year field](ds:block?id=schedexport-job-nextfire-vhxvmqtq):

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

The systemd and Task Scheduler forms write the instant in UTC, so they need no zone:

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

```text
  OnCalendar=2026-01-06 02:00:00 UTC
  <StartBoundary>2026-01-06T02:00:00Z</StartBoundary>
```

The clock reads Monday 10:00 UTC, so the next weekday 03:00 in Berlin is Tuesday 02:00 UTC; the launchd form above writes the same instant as 21:00 on Monday on a New York Mac. `SystemdTimerNextFire` maps the misfire policy to `Persistent=` as `SystemdTimer` does but checks no overlap, since one fire has no previous run of its own; `TaskSchedulerNextFire` maps both policies as `TaskScheduler` does.

launchd has no one-shot date: its entry fires again on the same date a year later, so the app replaces the plist each time it runs, which a handoff does anyway.

A schedule that never fires again (its `Active` period or year field is over, or no tick passes its windows) returns an error matching [`schedexport.ErrNoNextFire`](ds:block?id=schedexport-errnonextfire-2pu4m79k). There is nothing to hand over, so uninstall the file:

<!-- snippet: examples/schedule-export/main.go#no-next-fire -->
```go
ended := report
ended.Active = workflow.Active{Until: monday} // the campaign ended this morning
_, err = schedexport.SystemdTimerNextFire(ended, app, clk)
if errors.Is(err, schedexport.ErrNoNextFire) {
	fmt.Println("  no next fire: uninstall the timer") // nothing to hand over
}
```

## What each format refuses

Refusals are typed, so an app can fall back (another format, or a schedule written differently) and [a test can assert them](ds:block?id=schedexport-testrefusals-8mg3qn96):

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

| Schedule part | Kubernetes | systemd | launchd | Task Scheduler |
|---|---|---|---|---|
| Seconds field | only second 0 | yes | only second 0 | yes |
| Year field | refused | yes | refused | contiguous years |
| `@every` | refused | refused | refused | refused |
| Zone | [any IANA zone](ds:block?id=schedexport-zonename-ufzk25yh) | any IANA zone | the Mac's zone | the machine's zone |
| `OverlapAllow` | `Allow` | refused | refused | `Parallel` |
| `OverlapSkip` | `Forbid` | always | always | `IgnoreNew` |
| `MisfireSkip` | deadline 60s | `Persistent=false` | refused | `StartWhenAvailable` false |
| `MisfireFireOnce` | no deadline | `Persistent=true` | refused | `StartWhenAvailable` true |
| `MisfireFireOnce` with `Within` | `startingDeadlineSeconds` | refused | refused | refused |
| `MisfireFireAll` | [refused](ds:block?id=schedexport-classify-a9sc88gr) | refused | refused | refused |
| `Active` | refused | refused | refused | start and end boundary |
| `Windows` | [refused](ds:block?id=schedexport-job-nowindows-bmfak65q) | refused | refused | refused |

The zero misfire policy means the target's own default (the CronJob controller starts the newest missed tick, systemd and Task Scheduler skip missed ticks, launchd starts one run on wake). [systemd](ds:block?id=schedexport-systemdtimer-527u7nzp) and [launchd](ds:block?id=schedexport-launchd-354shg5d) never start a second copy of a running job, so they skip a tick during a run whatever the schedule says; the zero overlap policy means "allow", so set `Overlap: workflow.OverlapSkip` to export to them. Every one of those refusals says what to change.

## Try it

```sh
cd examples
go run ./schedule-export
```

The program prints every file in full, then the launchd entries and the three handoffs shown above; the refusals and the year field come last:

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

## Pitfalls

- **DST gaps may differ.** On a wall time that does not exist (02:30 on a spring-forward day), the library's drivers [fire once at the end of the gap](ds:block?id=cron-resolve-dt9n6h3q); the CronJob controller, systemd, launchd and Task Scheduler each follow their own rule and may skip that day. Avoid fixed times between 02:00 and 03:00 in zones with DST, or schedule in UTC.
- **A CronJob without a deadline gives up after 100 missed ticks.** The controller refuses to start a CronJob that missed more than 100 schedules; set `Misfire.Within` (it becomes [`startingDeadlineSeconds`](ds:block?id=schedexport-cronjob-haa7av5t)) for a frequent schedule that may be down for long. A deadline under 10 seconds can miss ticks, because the controller checks every 10 seconds.
- **launchd and Task Scheduler read the machine's zone.** Export with the zone of the machine the file is installed on ([`LaunchdOptions`](ds:block?id=schedexport-launchdoptions-9rtf8g3q), [`TaskSchedulerOptions`](ds:block?id=schedexport-taskscheduleroptions-dzg46fkr)); when the user changes the machine's zone, write the file again.
- **The one-shot launchd entry repeats a year later.** Replace or unload the plist when the app runs; the handoff does that by writing the next one.
- **The files run a command, not the workflow.** The [command](ds:block?id=schedexport-target-htxk8djj) decides what to enqueue with which input and labels; keep the schedule's `Name` in its arguments so it can look the schedule up.
- **Installing is yours.** `kubectl apply`, `systemctl enable --now <name>.timer`, `launchctl bootstrap gui/<uid> <file>` and `Register-ScheduledTask` all need permissions and paths only the app knows.

## Related

- [Cron schedules](cron.md) and its [year field](cron.md#a-year-field), [time zones and DST](time-zones.md)
- [Misfire policies](misfire.md), [overlap](overlap.md), [schedule conditions](conditions.md) (windows, active periods)
- Example: [examples/schedule-export](../../examples/schedule-export/)
