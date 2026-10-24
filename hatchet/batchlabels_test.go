package hatchet

import (
	"maps"
	"testing"

	"github.com/ubgo/workflow/batch"
)

// TestJobEndLabels pins that a job end is reported with the job's current
// attempt, so the Manager does not drop the end of a job RetryFailed
// restarted (it ignores reports whose attempt differs).
func TestJobEndLabels(t *testing.T) {
	st := batch.State{ID: "b1", Jobs: []batch.JobState{{}, {Attempt: 2}}}
	if got, want := jobEndLabels(st, 0), batch.JobLabels("b1", 0, 0); !maps.Equal(got, want) {
		t.Fatalf("first run: %v, want %v", got, want)
	}
	got := jobEndLabels(st, 1)
	if got[batch.LabelAttempt] != "2" || !maps.Equal(got, batch.JobLabels("b1", 1, 2)) {
		t.Fatalf("restarted job: %v", got)
	}
}
