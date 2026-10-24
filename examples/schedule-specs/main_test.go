package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guides show: the
// compiled calendar spec and its refusal, the seconds dialect, zone loads
// and refusals, next runs across a daylight saving change, an active
// period, environment gating, the JSON form and the local cron's ticks.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `calendar:
  0,30 9 * * 1-5
  ErrCalendar=true
seconds:
  00:00:20
  00:00:40
  00:01:00
zones:
  loaded Europe/Berlin
  "" ErrZone=true
  "Local" ErrZone=true
  "Europe/Berln" ErrZone=true
next runs:
  2026-10-24 09:00:00 CEST (07:00:00 UTC)
  2026-10-25 09:00:00 CET (08:00:00 UTC)
  2026-10-26 09:00:00 CET (08:00:00 UTC)
active:
  Wed 14 Oct 09:00
  Thu 15 Oct 09:00
  Fri 16 Oct 09:00
environment:
  production fires=true
  staging fires=false
json:
  {"v":1,"name":"nightly-report","workflow":"report","cron":"0 2 * * *","location":"Europe/Berlin","misfire":{"policy":"skip"}}
  back: nightly-report 0 2 * * * Europe/Berlin
  v2 ErrScheduleJSON=true
local cron:
  ran for 00:01
  ran for 00:02
  ran for 00:03
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
