package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows:
// MemoryLineage keeps one record per fact and refuses a negative Limit,
// two processes sharing the SQL lineage store start the trigger once per
// version, and two processes with a MemoryLineage each start it twice.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `memory lineage:
  records: 2, newest: 2026-11-20T10, negative Limit is ErrInvalid: true
shared SQL lineage store:
  process A announces 2026-11-20T09: started audit
  process B announces 2026-11-20T09: started nothing
  process B announces 2026-11-20T10: started audit
  audit runs recorded in the shared store: 2
a MemoryLineage per process:
  process A announces 2026-11-20T11: started audit
  process B announces 2026-11-20T11: started audit
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
