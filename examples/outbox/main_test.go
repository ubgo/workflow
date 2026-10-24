package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: a
// committed entry is relayed and a rolled-back one never exists; an entry
// the target keeps refusing goes dead after two attempts, is reported,
// listed and, once requeued, delivered; a target without idempotency keys
// is refused unless AllowDuplicates, and then gets no entry-ID key.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- Write jobs inside your transaction
-- Relay entries to any client
shipped {"order":1}
entries left after the rollback: 0
-- Failed and dead entries
accepted while down: 0
dead: ship after 2 attempts: engine unreachable
listed: {"order":4} attempts=2 last error="engine unreachable"
accepted after requeue: 1
shipped {"order":4}
-- Targets without idempotency keys
refused without AllowDuplicates: true
accepted: 1, idempotency key sent: ""
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
