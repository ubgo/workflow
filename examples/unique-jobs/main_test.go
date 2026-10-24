package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// second job is refused while the lock is held, in one process and across
// two pools on one SQLite file, and accepted once the lock is gone.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- Unique for a time
first: accepted
second, same hour: refused (ErrDuplicate)
third, an hour later: accepted
-- Unique until the run finishes
first: accepted
second, while the first runs: refused (ErrDuplicate)
export ran
run finished: succeeded key label: u-7
third, after it finished: accepted
export ran
-- Unique by workflow and arguments
a-1, ann: accepted
a-1, bob, keys reordered: refused (ErrDuplicate)
a-2: accepted
key: args:report:7dee8b416858...
-- A store shared by every process
process A: accepted
process B, same user: refused (ErrDuplicate)
expired locks pruned: 1
process B, after expiry: accepted
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
