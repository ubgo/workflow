package batch

import (
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// TestFinishLatchesOnce pins the Finished latch directly. Through the
// public API a second finish is already stopped by per-job dedup, so this
// is the only test that fails if the latch goes; it guards a future caller
// of finish that has no dedup in front of it.
func TestFinishLatchesOnce(t *testing.T) {
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	st := State{Jobs: []JobState{{Status: workflow.StatusSucceeded}}}
	var first, second effects
	finish(&st, now, &first)
	finish(&st, now.Add(time.Hour), &second)
	if !first.then || !first.finally {
		t.Errorf("first finish = %+v, want then and finally", first)
	}
	if second.then || second.finally {
		t.Errorf("second finish = %+v, want nothing", second)
	}
	if !st.FinishedAt.Equal(now) {
		t.Errorf("FinishedAt = %v, want the first finish", st.FinishedAt)
	}
}

// TestDiscardedCountsAsDiscardedAs pins how a discarded job counts in
// every batch decision: a failure by default, else as DiscardedAs says.
func TestDiscardedCountsAsDiscardedAs(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		as                       workflow.Status
		catch, cancelRest, then  bool
		failed, cancelled, succd int
	}{
		{"", true, true, false, 1, 0, 0},
		{workflow.StatusFailed, true, true, false, 1, 0, 0},
		{workflow.StatusCancelled, false, false, false, 0, 1, 1},
		{workflow.StatusSucceeded, false, false, true, 0, 0, 2},
	} {
		st := State{DiscardedAs: tc.as, Jobs: []JobState{{}, {}}}
		eff := recordFinish(&st, 0, workflow.StatusDiscarded, now)
		if eff.catch != tc.catch || eff.cancelRest != tc.cancelRest {
			t.Errorf("DiscardedAs %q: effects %+v, want catch %v, cancelRest %v", tc.as, eff, tc.catch, tc.cancelRest)
		}
		if st.Jobs[0].Status != workflow.StatusDiscarded {
			t.Errorf("DiscardedAs %q: the raw status was not kept", tc.as)
		}
		if !st.Cancelled {
			eff = recordFinish(&st, 1, workflow.StatusSucceeded, now)
			if eff.then != tc.then {
				t.Errorf("DiscardedAs %q: then = %v, want %v", tc.as, eff.then, tc.then)
			}
		}
		p := progressOf(st)
		if p.Discarded != 1 || p.Failed != tc.failed || p.CancelledJobs != tc.cancelled || p.Succeeded != tc.succd {
			t.Errorf("DiscardedAs %q: progress %+v", tc.as, p)
		}
		wantRetry := tc.failed == 1
		if got := len(retryable(st)) > 0 && retryable(st)[0] == 0; got != wantRetry {
			t.Errorf("DiscardedAs %q: RetryFailed re-runs the discarded job = %v, want %v", tc.as, got, wantRetry)
		}
	}
}
