package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestCallsRefusedOnceDone pins the order Done promises: by the time Done
// is closed, the client is marked stopped, so Enqueue, Signal and
// Schedule all refuse with ErrNotStarted, for every way a client stops.
func TestCallsRefusedOnceDone(t *testing.T) {
	wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
	for name, stop := range map[string]func(c *memory.Client, cancel context.CancelFunc){
		"start ctx": func(_ *memory.Client, cancel context.CancelFunc) { cancel() },
		"drain":     func(c *memory.Client, _ context.CancelFunc) { _ = c.Drain(context.Background()) },
	} {
		t.Run(name, func(t *testing.T) {
			for range 50 {
				c := memory.New()
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
				go stop(c, cancel)
				select {
				case <-c.Done():
				case <-time.After(10 * time.Second):
					t.Fatal("Done never closed")
				}
				bg := context.Background()
				if _, err := c.Enqueue(bg, workflow.Job{Workflow: "w"}); !errors.Is(err, workflow.ErrNotStarted) {
					t.Fatalf("Enqueue after Done = %v", err)
				}
				if err := c.Signal(bg, workflow.Event{Name: "e"}); !errors.Is(err, workflow.ErrNotStarted) {
					t.Fatalf("Signal after Done = %v", err)
				}
				if _, err := c.Schedule(bg, workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC}); !errors.Is(err, workflow.ErrNotStarted) {
					t.Fatalf("Schedule after Done = %v", err)
				}
				cancel()
			}
		})
	}
}
