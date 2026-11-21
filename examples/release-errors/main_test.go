package main

import (
	"bytes"
	"context"
	"testing"
)

// want is the program's whole output, as the README and the guide show it.
// The attempt sequence is fixed by the fake API's answers, so it is exact.
const want = `classify:
  429 is a release: true | wait: 10ms
  503 is a release: false
  RetryAfter(nil, d) is nil: true
  RetryAfter(Permanent(err)) retried: false
sync:
  attempt 1: 429 too many requests (release: true) -> attempts 1, errors 0
  attempt 2: 429 too many requests (release: true) -> attempts 2, errors 0
  attempt 3: 503 service unavailable (release: false) -> attempts 3, errors 1
  attempt 4: 429 too many requests (release: true) -> attempts 4, errors 1
  attempt 5: 503 service unavailable (release: false) -> attempts 5, errors 2
  run failed after 2 errors and 3 releases
`

// TestRun runs the program and checks every line: releases are told from
// failures, and MaxErrors stops the step on its second real error although
// Max allows more attempts.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
