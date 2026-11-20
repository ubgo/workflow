package enginetest

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
)

// TestThenOnSQLStore: workflow.Then on a sqlstore client starts the next
// workflow when a run succeeds, with the payload Start built and
// LabelThenFrom naming the finished run (EV-08).
func TestThenOnSQLStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			then, err := workflow.Then([]workflow.ThenRule{{When: "import", Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
				return workflow.Job{Workflow: "index", Payload: []byte(`"` + ev.Labels["file"] + `"`)}, true, nil
			}}})
			if err != nil {
				t.Fatal(err)
			}
			c, err := workflow.Use(newClient(t, e, db, prefix), then)
			if err != nil {
				t.Fatal(err)
			}
			echo := []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}
			for _, wf := range []workflow.Workflow{{Name: "import", Steps: echo}, {Name: "index", Steps: echo}} {
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "import", Labels: map[string]string{"file": "a.csv"}})
			if err != nil {
				t.Fatal(err)
			}
			q := c.(workflow.RunQuerier)
			var next workflow.RunInfo
			for i := 0; ; i++ {
				page, err := q.Runs(ctx, workflow.RunQuery{Workflow: "index", Statuses: []workflow.Status{workflow.StatusSucceeded}, Limit: 10})
				if err != nil {
					t.Fatal(err)
				}
				if len(page.Runs) == 1 {
					next = page.Runs[0]
					break
				}
				if i > 1000 {
					t.Fatalf("no succeeded index run after import %v (%d runs)", ref, len(page.Runs))
				}
				time.Sleep(10 * time.Millisecond)
			}
			if next.Labels[workflow.LabelThenFrom] != ref.RunID.String() || string(next.Payload) != `"a.csv"` {
				t.Fatalf("index run labels %v payload %s", next.Labels, next.Payload)
			}
		})
	}
}

// TestEventsCancelDivertReplayOnSQLStore: on a sqlstore client a cancel
// subscription cancels the order's unfinished runs (a running one
// included) and diverts once, and a replay with one ID starts its runs
// once, even after they finished (EV-04, EV-05, WX-14).
func TestEventsCancelDivertReplayOnSQLStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix)
			release := make(chan struct{})
			defer close(release)
			hold := []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil, ctx.Err()
			}}}
			quick := []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}
			for _, wf := range []workflow.Workflow{{Name: "fulfil", Steps: hold}, {Name: "refund", Steps: quick}, {Name: "receipt", Steps: quick}} {
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			run, err := c.Enqueue(ctx, workflow.Job{Workflow: "fulfil", Labels: map[string]string{"order": "42"}})
			if err != nil {
				t.Fatal(err)
			}
			waitStatus(t, c, run, workflow.StatusRunning)

			b, err := events.New(c, events.WithLog(events.NewMemoryLog()))
			if err != nil {
				t.Fatal(err)
			}
			if err := b.SubscribeCancel(events.CancelSubscription{Name: "cancel-to-refund", Event: "order.cancelled", Label: "order", Divert: "refund"}); err != nil {
				t.Fatal(err)
			}
			if err := b.Subscribe(events.Subscription{Name: "receipt", Event: "order.paid", Workflow: "receipt"}); err != nil {
				t.Fatal(err)
			}
			msg := events.Message{ID: "c1", Name: "order.cancelled", Key: "42", Labels: map[string]string{"order": "42"}}
			p, err := b.Publish(ctx, msg)
			if err != nil || p.Cancellations[0].Cancelled != 1 || p.Cancellations[0].Diverted.IsZero() {
				t.Fatalf("publish = %+v, %v", p.Cancellations, err)
			}
			waitStatus(t, c, run, workflow.StatusCancelled)
			waitStatus(t, c, p.Cancellations[0].Diverted, workflow.StatusSucceeded)
			again, err := b.Publish(ctx, msg)
			if err != nil || again.Cancellations[0].Diverted != p.Cancellations[0].Diverted {
				t.Fatalf("republish = %+v, %v; want the first divert", again.Cancellations, err)
			}

			paid, err := b.Publish(ctx, events.Message{ID: "p1", Name: "order.paid"})
			if err != nil {
				t.Fatal(err)
			}
			waitStatus(t, c, paid.Deliveries[0].Ref, workflow.StatusSucceeded)
			r, err := b.Replay(ctx, events.ReplayOptions{ID: "fix"})
			if err != nil || len(r.Deliveries) != 1 || r.Deliveries[0].Existing || r.Deliveries[0].Ref == paid.Deliveries[0].Ref {
				t.Fatalf("replay = %+v, %v; want one new receipt run", r, err)
			}
			waitStatus(t, c, r.Deliveries[0].Ref, workflow.StatusSucceeded)
			r2, err := b.Replay(ctx, events.ReplayOptions{ID: "fix"})
			if err != nil || len(r2.Deliveries) != 1 || !r2.Deliveries[0].Existing || r2.Deliveries[0].Ref != r.Deliveries[0].Ref {
				t.Fatalf("repeated replay = %+v, %v; want the existing run", r2, err)
			}
		})
	}
}
