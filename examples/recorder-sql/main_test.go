package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/ubgo/workflow/recorder/sqlrecorder"
)

// want is the program's whole output, the one its README and guide show.
const want = `dialects: [postgres mysql sqlite]
refused: history database "oracle": want one of [postgres mysql sqlite]
retried: sqlite busy true | proxy busy true | syntax error false
migration statements: 15, tables: hist_events hist_labels hist_runs hist_schema hist_steps
day 0: report succeeded steps=1, billing succeeded steps=1
prune after a week: steps cleared=1 runs deleted=0 archived=0
day 7: report succeeded steps=0, billing succeeded steps=1
prune after a month, archive down: deleted=0 error=sqlrecorder: archive: cold storage unreachable
day 31: report succeeded steps=0, billing succeeded steps=1
archived: report (steps kept: 0)
after PruneEvery: report gone, billing succeeded steps=1
archive store: migration statements: 19, tables: arch_archive arch_events arch_labels arch_runs arch_schema arch_steps
archive prune: runs deleted=3 moved=3
archived runs, pages of 2: 2+1 runs: exp-1 exp-2 exp-3
archived run exp-1: succeeded steps=1 events=1
still in the runs table: false
`

// TestRun runs the program and checks every line it prints: the dialect
// check, the retry test, the prefixed tables, stage one after a week,
// a refused archive that deletes nothing, and stage two through
// PruneEvery once the archive is back, with the billing run kept; then the
// archive table: its extra table and statements, a prune that moves every
// expired run once, the archive read in two pages with each run once (all
// three ended at the same instant), one run with its steps and events, and
// the run gone from the runs table.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// TestDialectFor accepts every dialect the store supports, by its name.
func TestDialectFor(t *testing.T) {
	for _, d := range sqlrecorder.Dialects() {
		got, err := dialectFor(string(d))
		if err != nil || got != d {
			t.Errorf("dialectFor(%q) = %q, %v", d, got, err)
		}
	}
}
