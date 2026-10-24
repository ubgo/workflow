package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: refusals
// naming where and which rule, a SecretRef accepted and resolved in the
// step, the detector's allow lists and an empty reference.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
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
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
