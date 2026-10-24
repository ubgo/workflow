package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: each
// frequency's expression and next fire, each refusal, and the built
// expression firing on weekdays only.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `frequencies:
  @every 30s         next Mon 5 Jan 00:00:30
  */15 * * * 1-5     next Mon 5 Jan 00:15:00
  17 * * * *         next Mon 5 Jan 00:17:00
  0 */6 * * *        next Mon 5 Jan 06:00:00
  0 9 * * 1-5        next Mon 5 Jan 09:00:00
  0 1,13 * * *       next Mon 5 Jan 01:00:00
  30 17 * * 5        next Fri 9 Jan 17:30:00
  0 8 15 * *         next Thu 15 Jan 08:00:00
  0 0 1 1,4,7,10 *   next Wed 1 Apr 00:00:00
  0 12 1 6 *         next Mon 1 Jun 12:00:00
refusals:
  ErrBuilder=true cron: invalid builder: EveryMinutes(7): n must divide 60 (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)
  ErrBuilder=true cron: invalid builder: At("13:00"): only a daily or coarser frequency has a time of day to set
  ErrBuilder=true cron: invalid builder: Days(): the frequency also fixes the day of the month, and cron would fire when either day matches
  ErrBuilder=true cron: invalid builder: YearlyOn(February, 30): day must be 1-29
  ErrBuilder=true cron: invalid builder: At("9:5"): want "HH:MM" on a 24-hour clock, e.g. "13:00" or "9:05"
on a workflow:
  Mon 5 Jan 09:30, Tue 6 Jan 09:30, Wed 7 Jan 09:30, Thu 8 Jan 09:30, Fri 9 Jan 09:30, Mon 12 Jan 09:30
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
