package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: the
// CronJob, the systemd units, the Task Scheduler task, the launchd
// handoff on a fixed clock, the refusals and the year field ending.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `kubernetes:
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
    jobTemplate:
      spec:
        template:
          spec:
            restartPolicy: OnFailure
            containers:
              - name: "job"
                image: "ghcr.io/example/app:1.4.2"
                command: ["/usr/local/bin/app", "run-schedule"]
                args: ["nightly-report"]
systemd:
  == nightly-report.timer
  [Unit]
  Description=Schedule nightly-report

  [Timer]
  OnCalendar=Mon..Fri *-*-* 03:00:00 Europe/Berlin
  AccuracySec=1s
  Persistent=true
  Unit=nightly-report.service

  [Install]
  WantedBy=timers.target
  == nightly-report.service
  [Unit]
  Description=Schedule nightly-report

  [Service]
  Type=oneshot
  ExecStart=/usr/local/bin/app run-schedule nightly-report
task scheduler:
  == nightly-report.xml
  <?xml version="1.0" encoding="UTF-8"?>
  <Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
    <RegistrationInfo>
      <Description>Schedule nightly-report</Description>
      <URI>\nightly-report</URI>
    </RegistrationInfo>
    <Triggers>
      <CalendarTrigger>
        <StartBoundary>2000-01-01T09:00:00</StartBoundary>
        <Repetition>
          <Interval>PT15M</Interval>
          <Duration>PT8H52M30S</Duration>
        </Repetition>
        <ScheduleByWeek>
          <WeeksInterval>1</WeeksInterval>
          <DaysOfWeek>
            <Monday />
            <Tuesday />
            <Wednesday />
            <Thursday />
            <Friday />
          </DaysOfWeek>
        </ScheduleByWeek>
      </CalendarTrigger>
    </Triggers>
    <Settings>
      <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
      <StartWhenAvailable>true</StartWhenAvailable>
      <Enabled>true</Enabled>
    </Settings>
    <Actions Context="Author">
      <Exec>
        <Command>/usr/local/bin/app</Command>
        <Arguments>run-schedule nightly-report</Arguments>
      </Exec>
    </Actions>
  </Task>
handoff:
  == nightly-report.plist
  <?xml version="1.0" encoding="UTF-8"?>
  <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
  <plist version="1.0">
  <dict>
  	<key>Label</key>
  	<string>nightly-report</string>
  	<key>ProgramArguments</key>
  	<array>
  		<string>/usr/local/bin/app</string>
  		<string>run-schedule</string>
  		<string>nightly-report</string>
  	</array>
  	<key>StartCalendarInterval</key>
  	<array>
  		<dict>
  			<key>Month</key>
  			<integer>1</integer>
  			<key>Day</key>
  			<integer>5</integer>
  			<key>Hour</key>
  			<integer>21</integer>
  			<key>Minute</key>
  			<integer>0</integer>
  		</dict>
  	</array>
  </dict>
  </plist>
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
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
