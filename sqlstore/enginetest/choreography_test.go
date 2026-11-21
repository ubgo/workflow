package enginetest

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
)

// TestChoreographyTrail proves events.WithCorrelation and events.Trail on
// every engine: the chain's labels are stored with the runs and found by
// the label query, and causation names the event that started the
// publishing run.
func TestChoreographyTrail(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			sc := newClient(t, e, db, prefix)
			var bus *events.Bus
			placed := workflow.Workflow{Name: "ch-charge", Steps: []workflow.Step{{Name: "charge", Run: func(ctx context.Context, in []byte) ([]byte, error) {
				_, err := bus.Publish(ctx, events.Message{ID: "paid-" + string(in), Name: "order.paid", Payload: in})
				return nil, err
			}}}}
			ship := workflow.Workflow{Name: "ch-ship", Steps: []workflow.Step{{Name: "ship", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
			startWith(t, sc, placed, ship)
			var err error
			if bus, err = events.New(sc, events.WithCorrelation()); err != nil {
				t.Fatal(err)
			}
			for _, s := range []events.Subscription{{Name: "charge", Event: "order.placed", Workflow: "ch-charge"}, {Name: "ship", Event: "order.paid", Workflow: "ch-ship"}} {
				if err := bus.Subscribe(s); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			if _, err := bus.Publish(ctx, events.Message{ID: "o1", Name: "order.placed", Payload: []byte("o1")}); err != nil {
				t.Fatal(err)
			}
			if _, err := bus.Publish(ctx, events.Message{ID: "o2", Name: "order.placed", Payload: []byte("o2")}); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(30 * time.Second)
			for {
				runs, err := events.Trail(ctx, sc, "o1", workflow.RunQuery{})
				if err != nil {
					t.Fatal(err)
				}
				done := 0
				var got []string
				for _, r := range runs {
					got = append(got, r.Workflow+"<"+r.Labels[events.LabelCausation])
					if r.Status == workflow.StatusSucceeded {
						done++
					}
				}
				if len(runs) == 2 && done == 2 {
					sort.Strings(got)
					if got[0] != "ch-charge<" || got[1] != "ch-ship<o1" {
						t.Fatalf("trail %v", got)
					}
					return
				}
				if len(runs) > 2 || time.Now().After(deadline) {
					t.Fatalf("trail of o1: %d runs, %d done: %v", len(runs), done, got)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}
