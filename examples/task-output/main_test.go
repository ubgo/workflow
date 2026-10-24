package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks the lines the guide shows: the file
// gets both nights, the mail only the failed one, and the Debug line is
// below the capture level.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `night 1: succeeded
night 2: failed
--- nightly-report.log
== 2026-01-05T02:00:00Z nightly-report/export attempt 1 [run-2] ok
2026-01-05T02:00:00Z INFO exporting orders night=1
result: {"rows":42}
== 2026-01-06T02:00:00Z nightly-report/export attempt 1 [run-5] error: export refused
2026-01-06T02:00:00Z INFO exporting orders night=2
2026-01-06T02:00:00Z ERROR warehouse API refused the export status=503
--- mail
To: ops@example.com
Subject: nightly-report failed

== 2026-01-06T02:00:00Z nightly-report/export attempt 1 [run-5] error: export refused
2026-01-06T02:00:00Z INFO exporting orders night=2
2026-01-06T02:00:00Z ERROR warehouse API refused the export status=503
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
