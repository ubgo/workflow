package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestRun runs the program and checks every line the guide shows: the
// generated trigger installs, an insert, an update and a delete made by
// plain SQL each become one outbox entry with the chosen columns (the old
// row on delete), and the relay delivers all three to the memory driver.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	want := `-- install the trigger
statements: 6
-- change rows with plain SQL
-- relay to the workers
relayed: 3
synced {"id":"a1","email":"ana@example.com","plan":"free","op":"insert"} true
synced {"id":"a1","email":"ana@example.com","plan":"pro","op":"update"} true
synced {"id":"a1","email":"ana@example.com","plan":"pro","op":"delete"} true
`
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
