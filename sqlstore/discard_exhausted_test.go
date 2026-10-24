package sqlstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
)

// TestDiscardOnExhaust: a run whose policy has DiscardOnExhaust ends
// Discarded (ErrDiscarded, no OnFailure) once retries run out or a
// Permanent error stops them, whether the policy is the workflow's or a
// Job.Retry stored with the run; without the option the same failure ends
// Failed.
func TestDiscardOnExhaust(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix)
			boom := errors.New("boom")
			fail := func(err error) workflow.Handler {
				return func(context.Context, []byte) ([]byte, error) { return nil, err }
			}
			alerts := make(chan struct{}, 8)
			onFailure := func(context.Context, []byte) ([]byte, error) { alerts <- struct{}{}; return nil, nil }
			wfs := []workflow.Workflow{
				{Name: "exhaust", Retry: workflow.RetryPolicy{Max: 1, DiscardOnExhaust: true}, Steps: []workflow.Step{{Name: "a", Run: fail(boom)}}, OnFailure: onFailure},
				{Name: "plain", Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{{Name: "a", Run: fail(boom)}}, OnFailure: onFailure},
				{Name: "perm", Retry: workflow.RetryPolicy{Max: 5, DiscardOnExhaust: true}, Steps: []workflow.Step{{Name: "a", Run: fail(workflow.Permanent(boom))}}},
			}
			ctx := startWith(t, c, wfs...)
			cases := []struct {
				name string
				job  workflow.Job
				want workflow.Status
			}{
				{"workflow policy", workflow.Job{Workflow: "exhaust"}, workflow.StatusDiscarded},
				{"no option", workflow.Job{Workflow: "plain"}, workflow.StatusFailed},
				{"permanent", workflow.Job{Workflow: "perm"}, workflow.StatusDiscarded},
				{"Job.Retry turns it on", workflow.Job{Workflow: "plain", Retry: &workflow.RetryPolicy{Max: 0, DiscardOnExhaust: true}}, workflow.StatusDiscarded},
				{"Job.Retry turns it off", workflow.Job{Workflow: "exhaust", Retry: &workflow.RetryPolicy{Max: 0}}, workflow.StatusFailed},
			}
			for _, tc := range cases {
				ref, err := c.Enqueue(ctx, tc.job)
				if err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
				waitStatus(t, c, ref, tc.want)
				page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
				if err != nil || len(page.Runs) != 1 {
					t.Fatalf("%s: Runs = %v, %v", tc.name, page.Runs, err)
				}
				if tc.want == workflow.StatusDiscarded && page.Runs[0].Error == "" {
					t.Errorf("%s: discarded run has no reason", tc.name)
				}
			}
			// Only the two Failed runs ran OnFailure.
			if got := len(alerts); got != 2 {
				t.Errorf("OnFailure ran %d times, want 2 (the failed runs only)", got)
			}
		})
	}
}
