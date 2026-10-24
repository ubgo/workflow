package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun checks every line the guides show: the dry run changes nothing,
// the real retry retries every failed run, a delete without a filter is
// refused, Max bounds a call and says more match, the delete removes the
// succeeded runs, and MemoryMetrics reports the ends per five minutes.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `dry run: would retry 5, sample 5, done 0
retried 5 of 5, skipped 0, more to do: false
delete without a filter refused: true
dry run: would delete 3 (Max 3), more match: true
deleted 7 of 7
09:00 succeeded=2 failed=5 total=7
09:05 succeeded=5 failed=0 total=5
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
