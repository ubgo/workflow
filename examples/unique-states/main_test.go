package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks its whole output: a lock that holds only while its run waits to start, a key from tagged payload fields, and duplicates answered with the existing run and reported by WithEnqueueOutcome.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `unique while waiting to start:
  s-1 in an hour: accepted
  s-1 now, other cursor, first one scheduled: refused (ErrDuplicate)
  s-2 now: accepted
  started {"shop_id":"s-2","cursor":""}
  started {"shop_id":"s-1","cursor":"a"}
  s-1 again, first one running: accepted
  started {"shop_id":"s-1","cursor":"c"}
return the existing run:
  first: same run true, duplicate false (reported true)
  second: same run true, duplicate true (reported true)
  key first: duplicate false
  key second: duplicate true
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
