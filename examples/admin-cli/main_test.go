package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun checks every line the guide shows: the bulk retry previewed and
// done, the health table, the nodes JSON, a drain of an empty queue, and
// exit 1 for a refused token and 2 for an unknown command.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `$ workflow retry --all --workflow send-email --dry-run
dry run: would retry 3 runs; nothing changed
  run-8
  run-5
  run-2
exit 0
$ workflow retry --all --workflow send-email
matched 3, retried 3, skipped 0 (changed meanwhile)
exit 0
$ workflow health
CHECK    OK   DETAIL
live     yes  
ready    yes  
workers  yes  0 of 4 busy
exit 0
$ workflow nodes --json
{
  "nodes": [
    {
      "id": "worker-1",
      "mode": "full",
      "queues": [],
      "workers": 4,
      "running": 0,
      "started_at": "2026-10-14T09:00:00Z",
      "last_seen": "2026-10-14T09:00:00Z"
    }
  ]
}
exit 0
$ workflow drain --wait 1m
drained after 0s
exit 0
$ workflow failures
workflow: admin API: HTTP 401 unauthorized: authentication required
exit 1
$ workflow retyr
workflow: unknown command "retyr"
run 'workflow help' for usage
exit 2
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
