package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guides show: a
// CloudEvent through HTTP binary mode onto the bus and back out as JSON, a
// v0 payload upcast to v2 and a v3 one refused, a replay that starts its
// runs once, a cancel on event, a divert that a republish does not repeat,
// and Then starting index after a success and alert after a failure.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `cloudevents:
  headers: ce-id=evt_1 ce-type=order.paid ce-subject=42 ce-tenant=acme
  message: name=order.paid key=42 labels=map[tenant:acme]
  receipt ran with {"order":42,"cents":1999,"currency":"EUR"}
  structured: {"specversion":"1.0","id":"evt_1","source":"https://shop.example/orders","type":"order.paid","subject":"42","tenant":"acme","data":{"order":42,"cents":1999,"currency":"EUR"}}
versions:
  v0 message reached the receipt run as {"order":7,"cents":500,"currency":"EUR"}
  v3 message refused: ErrInvalid=true
replay:
  log holds 3 messages; replayed 2 to receipt: ["inv_2" "inv_3"]
  same replay ID again: 2 of 2 already started, new receipt runs 0
cancel on event:
  stop-cart-reminders cancelled 1 run(s)
  cart c1 reminder: cancelled; cart c2 reminder: running
divert:
  cancelled 1 fulfilment run(s); refund ran with {"order":42}
  republish: cancelled 0, same refund run true, refund runs 1
then:
  import ok.csv succeeded -> index ran with ok.csv
  import bad.csv failed -> alert ran with bad.csv
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
