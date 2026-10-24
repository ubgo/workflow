package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: a
// committed transaction runs its job, a rolled-back one leaves neither the
// order nor the run, and a repeated key in one transaction returns the
// first run without aborting the transaction.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- commit
shipped {"order":1}
run: succeeded
-- roll back
run after rollback is ErrNotFound: true
orders stored: 1
-- same key twice in one transaction
second call returned the first run: true
transaction still usable: true
shipped {"order":3}
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
