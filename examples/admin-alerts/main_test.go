package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun checks every line the guide shows: both alert kinds, the
// refused thresholds, the failed poll reported to the error handler, the
// repeated alert and both resolutions.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// want is the program's whole output, as docs/admin/alerts.md shows it.
const want = `-- Check polls once
09:00:00 ALERT queue_depth emails: depth 3 > 2
09:11:00 ALERT queue_wait emails: oldest wait 11m0s > 10m0s
a threshold that checks nothing: true
two thresholds for one queue: true
-- Run polls every interval
09:11:30 monitor error: run store: connection refused
09:15:00 ALERT queue_depth emails: depth 3 > 2
09:16:00 resolved queue_depth emails: depth 0 <= 2
09:16:00 resolved queue_wait emails: oldest wait 0s <= 10m0s
Run returned: context canceled
`
