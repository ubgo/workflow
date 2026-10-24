package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks its whole output: delayed runs due in the next hour, one moved by Reschedule, a finished run refused, and one step withdrawn with and without a fallback.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `due in the next hour:
  r-30m due at +30m0s, scheduled
rescheduled r-2h to +45m, same ref true
due in the next hour:
  r-2h due at +45m0s, scheduled
  r-30m due at +30m0s, scheduled
after an hour: r-2h succeeded, r-30m succeeded
reschedule a finished run: ErrWrongState true
cancel one step:
  store got "yesterday's prices"
  update-prices succeeded
  sync-feed failed, ErrStepCancelled true
  cancel it again: ErrWrongState true
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
