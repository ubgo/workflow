package memory_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestRoundRobinKeysTakeTurns proves FC-05 on memory: with one worker, a
// tenant that enqueued a backlog first does not starve the others; keys
// take turns.
func TestRoundRobinKeysTakeTurns(t *testing.T) {
	for _, tc := range []struct {
		strategy workflow.ConcurrencyStrategy
		want     string
	}{
		{workflow.ConcurrencyRoundRobin, "a1 b1 c1 a2 b2 a3"},
		{workflow.ConcurrencyQueueNewest, "a1 a2 a3 b1 b2 c1"}, // control: arrival order
	} {
		t.Run(string(tc.strategy), func(t *testing.T) {
			c := memory.New(memory.WithWorkers(1))
			var mu sync.Mutex
			var order []string
			gate := make(chan struct{})
			if err := c.Register(workflow.Workflow{
				Name:        "sync",
				Concurrency: &workflow.Concurrency{Key: workflow.KeyFunc(func(j workflow.Job) string { return string(j.Payload[:1]) }), MaxRuns: 10, Strategy: tc.strategy},
				Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) {
					<-gate
					mu.Lock()
					order = append(order, string(in))
					mu.Unlock()
					return nil, nil
				}}},
			}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			// "x" occupies the one worker while the backlog queues behind it.
			var refs []workflow.RunRef
			for _, p := range []string{"x0", "a1", "a2", "a3", "b1", "b2", "c1"} {
				ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync", Payload: []byte(p)})
				if err != nil {
					t.Fatal(err)
				}
				refs = append(refs, ref)
			}
			close(gate)
			for _, ref := range refs {
				if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
					t.Fatal(err)
				}
			}
			if got := strings.Join(order[1:], " "); got != tc.want {
				t.Fatalf("order %q, want %q", got, tc.want)
			}
		})
	}
}
