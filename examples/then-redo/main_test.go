package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: by
// default a reset and a fork of a succeeded import carry the redo label
// and start no second index; with ThenAfterRedo each starts one; and a
// failing rule reaches ThenErrors as a *workflow.ThenError.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `default: redo runs are skipped
  first import: succeeded, redo label false, index runs so far 1
  reset import: succeeded, redo label true, index runs so far 1
  forked import: succeeded, redo label true, index runs so far 1
with ThenAfterRedo: every redo starts index
  first import: succeeded, redo label false, index runs so far 1
  reset import: succeeded, redo label true, index runs so far 2
  forked import: succeeded, redo label true, index runs so far 3
errors:
  rule "audit", job started: false, cause: audit log is read-only
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
