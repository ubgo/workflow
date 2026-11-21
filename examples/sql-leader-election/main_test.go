package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// election hands over on Stop, a standalone elector's term grows on the
// handover, an Elector of the app's own gates the client, the database
// clock agrees with this machine's on SQLite, the stalled holder fences
// itself, the other node takes the step over, and WithMaxReclaims(0) fails
// the attempt as abandoned.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `node-a leads: true, node-b leads: false
after node-a stopped: node-a leads: false, node-b leads: true
node-1 won: true (term 1), node-2 won: false
after node-1 resigned: node-2 won: true (term 2)
client with a follower elector may fire cron: false
and once it leads: true
database clock: offset under 1s: true, uncertainty under 1s: true
client runs on the database clock: true
node-a stopped its attempt with ErrLeaseLost: true
node-b took the step over and ran it: run succeeded
WithMaxReclaims(0): run failed, abandoned: true
error: sqlstore: step "charge" failed after 1 attempt(s): sqlstore: step "charge" abandoned by 1 workers (its lease expired 1 times, over WithMaxReclaims 0): sqlstore: step abandoned by its workers
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
