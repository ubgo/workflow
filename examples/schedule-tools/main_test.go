package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guides show: the
// extended day fields and one firing on memory, hashed expressions,
// descriptions, a calendar skipping holidays and weekends, a backfill
// and its repeat, a limited and a one-shot schedule pruned, a startup
// run once per day, and the jittered start of a fired run.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `extended:
  0 9 L * *      [Sat Oct 31 09:00 Mon Nov 30 09:00 Thu Dec 31 09:00]
  0 9 15W * *    [Thu Oct 15 09:00 Mon Nov 16 09:00 Tue Dec 15 09:00]
  0 18 * * FRIL  [Fri Oct 30 18:00 Fri Nov 27 18:00 Fri Dec 25 18:00]
  0 9 * * MON#1  [Mon Nov 2 09:00 Mon Dec 7 09:00 Mon Jan 4 09:00]
  memory with cron.Extended fired for Oct 31 09:00; the default parser refuses L: true
hashed:
  acme     42 3 * * *
  globex   7 4 * * *
  initech  16 1 * * *
describe:
  0 9 * * MON-FRI  At 09:00, Monday through Friday
  */15 * * * *     Every 15 minutes
  5 * * * *        At minute 5 past every hour
  0 0 L * *        At 00:00, on the last day of the month
  30 8 * * MON#1   At 08:30, on the first Monday of the month
  @every 1h30m     Every 1 hour and 30 minutes
  */10 * * * * *   Every 10 seconds
calendars:
  Tue Dec 22 fired
  Wed Dec 23 fired
  Thu Dec 24 skipped (when)
  Fri Dec 25 skipped (when)
  Sat Dec 26 skipped (when)
  Sun Dec 27 skipped (when)
  Mon Dec 28 fired
backfill:
  plan: 3 ticks, first Oct 1 02:00 CEST
  run for Oct 1 02:00
  run for Oct 2 02:00
  run for Oct 3 02:00
  started 3; a repeat returns the same runs: true
limited:
  limited: */15 * * * *, active 10:15:00 to 10:45:01
  one-shot: 20 10 12 10 *
  poll-export ran 3 times (last 10:45), send-reminder 1 time
  pruned 2 schedules, 0 left
startup:
  three boots, once per day: 1 run
jitter:
  tick 09:00:00, step started 09:04:36
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
