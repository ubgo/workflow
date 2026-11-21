package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guides show: bus
// deliveries and their deduplication, the webhook handler's status codes,
// Standard Webhooks refusing a changed body and a replay, and an HMAC
// secret rotation where both secrets verify until the old one is removed.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `bus:
  receipt started a run: true
  crm started a run: true
  republish of receipt returned the first run: true
  republish of crm returned the first run: true
  a pos order matches 1 subscription(s)
  run labels: event=order.paid id=evt_1 subscription=crm channel=web
webhook:
  GET                    405 method not allowed
  no signature           401 unauthorized
  wrong signature        401 unauthorized
  too large              413 body too large
  signed                 200 {"id":"d-1"}
  redelivered            200 {"id":"d-1"}
  runs of github-push: 1
  backend down           503 unavailable
  no Verifier refused: true
standard webhooks:
  verified id=msg_1 err=<nil>
  changed body: ErrSignature=true
  replayed after 10m: ErrSignature=true
hmac rotation:
  new signature: g0kvyILC8kxerVjwwPqiYVmYAoBSuxlWZ1pbtLCkJS4=
  during rotation: old secret verifies true, new secret verifies true
  after rotation:  old secret verifies false, new secret verifies true
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
