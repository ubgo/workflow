package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestDiscardOnExhaust: retries running out under DiscardOnExhaust end the
// run Discarded with an Err matching ErrDiscarded, ErrCancelled and the
// step's error; the same failure without the option ends Failed.
func TestDiscardOnExhaust(t *testing.T) {
	boom := errors.New("boom")
	for _, on := range []bool{true, false} {
		c := memory.New()
		wf := workflow.Workflow{Name: "w", Retry: workflow.RetryPolicy{Max: 2, DiscardOnExhaust: on},
			Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, boom }}}}
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
		ends := make(chan workflow.RunEvent, 1)
		if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"}); err != nil {
			t.Fatal(err)
		}
		select {
		case ev := <-ends:
			want := workflow.StatusFailed
			if on {
				want = workflow.StatusDiscarded
			}
			if ev.Status != want || errors.Is(ev.Err, workflow.ErrDiscarded) != on || !errors.Is(ev.Err, boom) {
				t.Errorf("on=%v: %s, err %v; want %s", on, ev.Status, ev.Err, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("on=%v: run did not end", on)
		}
		c.Stop()
	}
}
