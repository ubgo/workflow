package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guides show: a
// release waits its own delay and counts as an attempt, and a unique lock
// is held while the run waits and freed once its step starts.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- release while the API is in maintenance
attempt 1: released for 10m0s
attempt 2: released for 10m0s
maintenance over at 09:10
attempt 3: ran
run: succeeded at 09:20
attempt 1: released for 10m0s
attempt 2: released for 10m0s
attempt 3: released for 10m0s
attempt 4: released for 10m0s
run: failed at 09:50
-- unique until a step starts
first, runs in 5 minutes: accepted
second, while the first waits: refused (ErrDuplicate)
third, while the first runs: accepted
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
