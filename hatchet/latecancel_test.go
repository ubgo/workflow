package hatchet

import (
	"errors"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

// TestLateStartErr pins the error Cancel returns for a delayed start the
// engine fired despite the delete: it matches ErrCancelTooLate and
// workflow.ErrWrongState, names the fired run, and says whether Cancel
// cancelled it or it had already ended.
func TestLateStartErr(t *testing.T) {
	ref := workflow.RunRef{ScheduledID: "tok~1"}
	for _, tc := range []struct {
		ended workflow.Status
		want  string
	}{
		{"", "Cancel cancelled that run"},
		{workflow.StatusSucceeded, "had already ended succeeded"},
	} {
		err := (&lateStart{ref: ref, runID: "run-1", ended: tc.ended}).err()
		if !errors.Is(err, ErrCancelTooLate) || !errors.Is(err, workflow.ErrWrongState) {
			t.Fatalf("err = %v, want ErrCancelTooLate and ErrWrongState", err)
		}
		if msg := err.Error(); !strings.Contains(msg, "run-1") || !strings.Contains(msg, tc.want) {
			t.Fatalf("err = %q, want the run and %q", msg, tc.want)
		}
	}
}
