package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, as the README shows it. The fake clock makes the fire times exact.
const want = `syntax:
  Mon 5 Jan 09:00 EST
  Tue 6 Jan 09:00 EST
  Wed 7 Jan 09:00 EST
  1st of the month or a Monday: Mon 12 Jan, Mon 19 Jan, Mon 26 Jan, Sun 1 Feb, Mon 2 Feb
  field hour, value "25": cron: invalid hour "25" in "0 25 * * *": value 25 out of range 0-23
daylight saving time:
  "30 2 * * *" Mar 8 03:00 EDT, Mar 9 02:30 EDT
  "30 * * * *" Mar 8 01:30 EST, Mar 8 03:30 EDT, Mar 8 04:30 EDT
  "30 1 * * *" Nov 1 01:30 EDT, Nov 2 01:30 EST
  "30 * * * *" Nov 1 00:30 EDT, Nov 1 01:30 EDT, Nov 1 01:30 EST
declared cron:
  fired Mon 5 Jan 09:00 IST (03:30 UTC)
  fired Tue 6 Jan 09:00 IST (03:30 UTC)
schedule at run time:
  "weekly report" for acme at Mon 5 Jan 08:00 CET
  after Unschedule: two Mondays passed, no run
upsert by name:
  sqlstore, same name twice, same schedule: true
  memory, same name twice, same schedule: true
misfire after a five-hour pause:
  skip      0 run(s)
  fire-once 1 run(s)
  fire-all  3 run(s)
  Due skip      []
  Due fire-once [05:00]
  Due fire-all  [03:00 04:00 05:00]
another dialect:
  memory with the dialect: <nil>
  memory with cron.Parse: workflow: invalid spec: workflow "poll": Cron[0]: cron: invalid macro "@quarter-hour" in "@quarter-hour": unknown macro
refusals:
  no Location: workflow: invalid spec: schedule for "report" has no Location; use time.UTC for UTC
  time.Local: true
  hatchet zoned: workflow: Workflow.CronLocation unsupported by this driver (missing CronZone): workflow zoned: Hatchet evaluates crons in UTC only, not Asia/Kolkata; shifting the expression would break across DST
  hatchet catch-up: workflow: Workflow.Misfire unsupported by this driver (missing MisfireFireAll): workflow catch-up: Hatchet never catches up a missed cron tick (no backfill) and has no setting for how late a tick may still fire, so only the driver default is honoured: a tick missed while the engine was down is dropped
  hatchet macro: workflow "macro": Cron[0]: workflow: Cron unsupported by this driver: Hatchet takes plain five-field expressions, not "@daily"
  memory needs for a zoned cron: Cron|CronZone | has them: true
`

// TestRun runs the program and checks every line it prints, so a change
// in the library's behaviour fails here before the guide goes stale.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output differs from the README.\ngot:\n%s\nwant:\n%s", got, want)
	}
}
