package workflowtest

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// lateEvent holds back the run of one sent event, as a worker too loaded
// to pick it up for a while does: the event reaches its instance late.
// The memory client underneath keeps run results, which the case reads.
type lateEvent struct {
	*memory.Client
	event string
	by    time.Duration
}

func (l lateEvent) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	if job.RunAt.IsZero() && bytes.Contains(job.Payload, []byte(`"event":"`+l.event+`"`)) {
		time.Sleep(l.by)
	}
	return l.Client.Enqueue(ctx, job)
}

// TestMachineTimersLatePing runs the timer case with every ping 300 ms
// late, three quarters of the case's 400 ms timer: the first timer fires
// before the ping arrives. The case must tell that apart from a timer the
// re-entry failed to restart, and still prove the restart (it did not: a
// loaded gate failed it with "expired 219.8ms after the re-entry").
func TestMachineTimersLatePing(t *testing.T) {
	machineTimers(t, MachineHarness{
		Client: func(*testing.T) workflow.Client {
			return lateEvent{Client: memory.New(), event: "ping", by: 300 * time.Millisecond}
		},
		Start: func(t *testing.T, c workflow.Client, wfs ...workflow.Workflow) {
			for _, wf := range wfs {
				if err := c.Register(wf); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
		},
		Store: func(*testing.T, workflow.Client) workflow.JournalStore { return workflow.NewMemoryJournal() },
	})
}
