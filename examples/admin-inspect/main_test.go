package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun checks every line the guide shows: the pages of runs, failures
// and discards, the count, the queue report and its bounded scan, the
// schedule's last fire, the node and health report and captured output.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// want is the program's whole output, as docs/admin/operations.md shows it.
const want = `-- runs, page by page
page 1: run-14 succeeded orders-5.csv
page 1: run-11 failed orders-4-bad.csv
page 2: run-8 succeeded orders-3.csv
page 2: run-5 failed orders-2-bad.csv
page 3: run-2 succeeded orders-1.csv
-- failed and discarded
failed run-11 at step load, 1 attempt(s): load: header row missing
failed run-5 at step load, 1 attempt(s): load: header row missing
discarded: 1 run of cleanup
Failures with statuses set: true
-- count for numbered pages
5 runs, truncated false, 3 pages
-- queues
default  depth 0, oldest wait 0s, workers 4, running 0
emails   depth 4, oldest wait 3m0s, workers 2, running 0
truncated false, worker counts true, at 09:03:00
with WithMaxScan(3): emails depth 3 (a lower bound)
-- schedules
schedule-31 "0 2 * * *" paused true, next fire zero true
schedule-31 next fire 2026-10-15 02:00:00
last fire 09:03:00 started run-33, now succeeded (trigger returned run-33)
-- nodes and health
node worker-1 (full) queues [default emails], 4 workers, 0 running
live true, ready true
  check workers ok=true 0 of 4 busy
-- captured output
step load attempt 1, error "load: header row missing", truncated true
== 2026-10-14T09:00:00Z import/load attempt 1 [run-11] error: load: header row missing
2026-10-14T09:00:00Z INFO loading file name=orders-4-
without WithOutputs: true
`
