package main

import (
	"bytes"
	"context"
	"testing"
)

// want is the program's whole output, as the README and the guide show it:
// a ticket driven through the JSON machine (a guard refusing "close" until
// two approvals, deep history returning a held ticket to investigating,
// the entry action noting every entry), the same hold and resume under
// all three history modes, the 48-hour timer closing a second ticket on
// the fake clock, the refusal of an unregistered guard with its JSON path,
// the Marshal and Parse round trip and ForgetMachine deleting an instance.
const want = `ticket T-1:
> assign
  states: [first-line]
> escalate
  states: [triage]
> investigate
  states: [investigating]
> close
  rejected: no transition takes it in the current state
> hold
  states: [held]
> resume
  states: [investigating]
> approve
  states: [investigating]
> approve
  states: [investigating]
> close
  states: [closed]
status of T-1: states [closed], done true, approvals 2, notes [escalated escalated closed]
   2 Mon 09:00 assign      [] -> [first-line]
   3 Mon 09:00 escalate    [first-line] -> [triage]
   4 Mon 09:00 investigate [triage] -> [investigating]
   5 Mon 09:00 hold        [investigating] -> [held]
   6 Mon 09:00 resume      [held] -> [investigating]
   7 Mon 09:00 approve     [investigating] -> [investigating]
   8 Mon 09:00 approve     [investigating] -> [investigating]
   9 Mon 09:00 close       [investigating] -> [closed]
history modes, after assign, escalate, investigate, hold:
  none    resume enters [first-line]
  shallow resume enters [triage]
  deep    resume enters [investigating]
ticket T-2:
> 48 hours pass while it is open
status of T-2: states [closed], done true, approvals 0, notes [closed]
   1 Mon 09:00 (start)     [] -> [open]
   2 Wed 09:00 expire      [open] -> [closed]
refused at transitions[5].guard: no guard is registered as "approved-once" (registered: approved-twice)
round trip equal: true
T-1 after ForgetMachine, found: false
`

// TestRun runs the program and checks every line it prints, so a change
// in the library's behaviour fails here before the guide goes stale.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
