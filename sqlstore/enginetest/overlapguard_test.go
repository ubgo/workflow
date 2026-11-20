package enginetest

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// TestGuardOverlapReplaceOnSQLStore: workflow.GuardOverlap with
// OverlapReplace on a sqlstore client cancels the schedule's running run
// when the next tick fires (CR-05).
func TestGuardOverlapReplaceOnSQLStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix)
			release := make(chan struct{})
			if err := c.Register(workflow.Workflow{Name: "report", Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				select {
				case <-release:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}}}); err != nil {
				t.Fatal(err)
			}
			errs := make(chan error, 8)
			if err := workflow.GuardOverlap(c, func(err error) { errs <- err }, workflow.OverlapRule{Name: "nightly", Mode: workflow.OverlapReplace}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			id, err := c.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "report", Cron: "0 0 1 1 *", Location: time.UTC})
			if err != nil {
				t.Fatal(err)
			}
			fire := func() workflow.RunRef {
				ref, err := c.TriggerSchedule(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				return ref
			}
			wait := func(ref workflow.RunRef, want workflow.Status) {
				t.Helper()
				var st workflow.Status
				for i := 0; i < 1000; i++ {
					if st, err = c.Status(ctx, ref); err == nil && st == want {
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
				t.Fatalf("run %v is %s (%v), want %s", ref, st, err, want)
			}
			first := fire()
			wait(first, workflow.StatusRunning)
			second := fire()
			wait(first, workflow.StatusCancelled)
			wait(second, workflow.StatusRunning)
			close(release)
			wait(second, workflow.StatusSucceeded)
			select {
			case err := <-errs:
				t.Fatalf("guard error: %v", err)
			default:
			}
		})
	}
}
