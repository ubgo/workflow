package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guides show: a late
// run discarded, waiting runs swept, the queue bound and tenant quota,
// quiet hours per zone, a refused payload, the retry ladder, constraint
// breaches failing at once, a pool of two, a critical section, an
// endpoint switch that trips after three failures, and the same switch
// shared by two workers through sqlstore.DisableStore.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `expiry:
  late run: discarded, code sent 0 time(s)
sweep:
  checked 2 waiting run(s), cancelled 2
bounds:
  acme: enqueued
  acme: enqueued
  acme: refused, quota exceeded
  globex: enqueued
  globex: refused, queue full
quiet hours:
  Asia/Tokyo: starts Tue 08:00 JST
  Europe/Berlin: starts now
validation:
  invalid: true workflow: invalid spec: payload of workflow "charge": order o-7: amount 0 is not positive
  valid order enqueued
ladder:
  4 retries after 5s, 5m0s, 30m0s, 2h0m0s
conditions:
  qty 4: stock left 6
  qty 0: failed after 1 attempt(s), constraint violated: true
  qty 12: failed after 1 attempt(s), constraint violated: true
pool:
  6 renders succeeded, at most 2 at once
critical section:
  transfer succeeded, at most 1 step(s) in the section at once
auto-disable:
  delivery 1: failed
  delivery 2: failed
  disabled https://hooks.example.com/a - tell its owner
  delivery 3: failed
  delivery 4: discarded
  delivery 5: discarded
  endpoint called 3 time(s)
  disabled after Enable: false
auto-disable across workers:
  delivery 1 on worker 1: failed
  delivery 2 on worker 2: failed
  worker 1 disabled https://hooks.example.com/b
  delivery 3 on worker 1: failed
  delivery 4 on worker 2: discarded
  delivery 5 on worker 1: discarded
  endpoint called 3 time(s)
`
	if out != want {
		t.Errorf("output:\n%s\nwant:\n%s", out, want)
	}
}
