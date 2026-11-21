package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the
// panic fails its run on the first attempt with the stack kept apart from
// the message, the timeout stops a step that would wait an hour, FailOn
// fails a declined card at once while a busy gateway gets its retries,
// and Skip completes the quiet run without calling notify.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `-- recover
level=ERROR msg="workflow step panicked" panic="assignment to entry in nil map"
run: failed after 1 attempt(s): memory: step "parse" failed after 1 attempt(s): workflow/middleware: step panicked: assignment to entry in nil map
history: attempt 1 "workflow/middleware: step panicked: assignment to entry in nil map", stack kept: true
-- timeout
run: failed: memory: step "download" failed after 1 attempt(s): context deadline exceeded
stopped well before the hour: true
-- fail on
declined: failed after 1 attempt(s)
busy: failed after 3 attempt(s)
-- skip
{"order":1}: succeeded, notify called 1 time(s)
{"order":2,"quiet":true}: succeeded, notify called 0 time(s)
`
	if got := out.String(); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
}
