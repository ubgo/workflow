package enginetest

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// TestLongPollSingleProcess proves IO-03 and the SQL half of EM-04: a
// single-process app (one SQLite file, every enqueue made by this client)
// can set a poll interval of an hour and still runs new work at once and
// delayed work at its due time, because the dispatcher sleeps until the
// next due time it knows and a local enqueue wakes it; the poll only looks
// for work other processes made due, and there are none.
func TestLongPollSingleProcess(t *testing.T) {
	for _, e := range engines(t) {
		if e.name != "sqlite" {
			continue
		}
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix, sqlstore.WithPollInterval(time.Hour))
			ran := make(chan string, 2)
			if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) {
				ran <- string(in)
				return nil, nil
			}}}}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			// Both runs must start long before the hour-long poll would
			// let them; the delayed one never before its RunAt. Their
			// order is not part of the proof: on a loaded host the
			// immediate run can take longer than 300 ms to start, and the
			// delayed one, due by then, may start first.
			runAt := time.Now().Add(300 * time.Millisecond)
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("delayed"), RunAt: runAt}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("now")}); err != nil {
				t.Fatal(err)
			}
			// bound is far below the poll interval, so a run that only the
			// poll would start fails it; it is generous for a loaded host.
			const bound = 30 * time.Second
			deadline := time.After(bound)
			seen := map[string]time.Time{}
			for len(seen) < 2 {
				select {
				case got := <-ran:
					if _, dup := seen[got]; dup {
						t.Fatalf("%q ran twice", got)
					}
					seen[got] = time.Now()
				case <-deadline:
					t.Fatalf("ran %v within %s, want both with an hour-long poll", seen, bound)
				}
			}
			if at := seen["delayed"]; at.Before(runAt) {
				t.Fatalf("the delayed run ran %s before its RunAt", runAt.Sub(at))
			}
		})
	}
}
