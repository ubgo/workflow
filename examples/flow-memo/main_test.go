package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guides show: a memoised
// charge on a retried step, MapEach and Loop rerunning only what failed,
// chunked progress, run data, a Choice, and the journal forgotten at the end.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `memo:
  result ch_1, charge ran 1 time(s), step ran 2 time(s)
map:
  result ["a.png@2x","b.png@2x","c.png@2x"]
  calls: a.png 1, b.png 2, c.png 1
loop:
  final cursor 4
  fetches: page 0 1, page 1 1, page 2 2, page 3 1
chunks:
  rows 25; calls: offset 0 1, offset 10 2, offset 20 1
  progress 100% "25 of 25 items", done 25 of 25
run data:
  "welcome mail for the pro plan"
choice:
  steps: load, route.decide, route.auto, route.review, route, notify
  40: "refunded 40 automatically"
  250: "refund of 250 sent to review"
no journal:
  ErrNoJournal=true permanent=true
journal:
  runs holding entries after every run finished: 0
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
