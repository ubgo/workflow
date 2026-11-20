package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: refusals
// naming where and which rule, a SecretRef accepted and resolved in the
// step, the detector's allow lists and an empty reference.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `refused:
  workflow: invalid spec: job carries a secret; pass a SecretRef instead: payload.api_key (key), payload.note (pem-private-key)
  ErrSecretInPayload: true ErrInvalid: true
  workflow: invalid spec: job carries a secret; pass a SecretRef instead: label auth_token (key)
accepted:
  payload {"shop":"acme","token":{"secret_ref":"shops/acme/token"}}
  result "synced acme with a 16-byte token"
rules:
  payload.password (key)
  empty SecretRef: ErrNoSecretRef=true permanent=true
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
