package main

import (
	"context"
	"os"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

const want = `30 jobs, 30 ran exactly once
hourly-report fired at 10:00
fired once across 3 workers
stopped; Enqueue now fails with workflow: client not started
`

// TestRunSQLite runs the three workers over a SQLite file.
func TestRunSQLite(t *testing.T) {
	t.Setenv(envDSN, "")
	check(t)
}

// TestRunPostgres runs them over Postgres; it skips without PG_DSN, so
// `go test` works on a machine with no Postgres.
func TestRunPostgres(t *testing.T) {
	if os.Getenv(envDSN) == "" {
		t.Skipf("%s is not set: the Postgres run needs a database (see README.md)", envDSN)
	}
	check(t)
}

func check(t *testing.T) {
	t.Helper()
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if got := out; got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
