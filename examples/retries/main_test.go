package main

import (
	"context"
	"strings"
	"testing"
)

// want is the program's whole output, as the README shows it. The fake clock and the pinned jitter source make it exact.
const want = `retry 1: delay 1s, with draw 0.5: 750ms
retry 2: delay 2s, with draw 0.5: 1.5s
retry 3: delay 4s, with draw 0.5: 3s
retry 4: delay 8s, with draw 0.5: 6s
retry 5: delay 10s, with draw 0.5: 7.5s

sync:
  call attempt 1 at +0s: upstream returned 503
  call attempt 2 at +750ms: upstream returned 503
  call attempt 3 at +2.25s: ok
  run succeeded

import:
  fetch attempt 1 at +0s: 429 too many requests
  fetch attempt 2 at +30s: ok
  run succeeded

charge:
  charge attempt 1 at +0s: card declined
  run failed (permanent: true): memory: step "charge" failed after 1 attempt(s): card declined

lookup:
  lookup attempt 1 at +0s: lookup: customer not found
  run failed (permanent: false): memory: step "lookup" failed after 1 attempt(s): lookup: customer not found

signup:
  create attempt 1 at +0s: upstream returned 503
  create attempt 2 at +1s: upstream returned 503
  create attempt 3 at +2s: ok
  welcome attempt 1 at +2s: smtp refused
  run failed (permanent: false): memory: step "welcome" failed after 1 attempt(s): smtp refused

refusals:
  needs: RetryJitter
  memory: <nil>
  hatchet: workflow "sync" step "call": workflow: Retry.Backoff.Jitter unsupported by this driver (missing RetryJitter): Hatchet waits exactly Factor^n seconds and has no jitter | unsupported: true
  hatchet: workflow "sync" step "call": workflow: Retry.DelayFor unsupported by this driver (missing RetryDelay): Hatchet sets a retry's delay from the backoff only, never from the error (RetryAfter)
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
