package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// want is the program's whole output, the one its README and guides show.
const want = `web node recorded: source=api/req-7 input={"image":"img-1"}
worker finished it: succeeded source=api/req-7 outcome=success
  step resize attempt 1 succeeded output="thumb-1.png" events=1
records for the run: 1
`

// TestRun runs the program on a temporary SQLite file and checks every
// line it prints.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out, filepath.Join(t.TempDir(), "app.db")); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
