package workflowtest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// SlotHarness builds a started client whose worker pool has workers
// slots, with wfs registered, or returns the error Register gave.
//
//ds:def id=workflowtest-slotharness-zawhxbmb owner=@khanakia stability=stable
type SlotHarness struct {
	Start func(t *testing.T, workers int, wfs ...workflow.Workflow) (workflow.Client, error)
}

// RunSlotFlows proves Step.Slots on a driver with CapSlotCost: the slots
// held by running attempts never pass the pool, a heavy step still runs
// among a stream of light ones, and a step needing more slots than the
// pool is refused at Register.
//
//ds:def id=workflowtest-runslotflows-bndr8cke owner=@khanakia stability=stable
func RunSlotFlows(t *testing.T, h SlotHarness) {
	const pool = 4
	var mu sync.Mutex
	held, peak := 0, 0
	work := func(n int) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) {
			mu.Lock()
			held += n
			peak = max(peak, held)
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			held -= n
			mu.Unlock()
			return nil, nil
		}
	}
	heavy := workflow.Workflow{Name: "slot-heavy", Steps: []workflow.Step{{Name: "render", Slots: 3, Run: work(3)}}}
	light := workflow.Workflow{Name: "slot-light", Steps: []workflow.Step{{Name: "ping", Run: work(1)}}}
	c, err := h.Start(t, pool, heavy, light)
	if err != nil {
		t.Fatal(err)
	}
	needCaps(t, c, workflow.CapSlotCost)
	ctx := context.Background()
	var refs []workflow.RunRef
	for i := range 12 {
		wf := "slot-light"
		if i%4 == 1 {
			wf = "slot-heavy"
		}
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	for _, r := range refs {
		flowStatus(t, c, r, workflow.StatusSucceeded)
	}
	mu.Lock()
	defer mu.Unlock()
	if peak > pool {
		t.Fatalf("attempts held %d slots at once, pool is %d", peak, pool)
	}
	if peak < 3 {
		t.Fatalf("peak %d slots: the heavy step never ran", peak)
	}

	tooBig := workflow.Workflow{Name: "slot-too-big", Steps: []workflow.Step{{Name: "s", Slots: pool + 1, Run: work(1)}}}
	if _, err := h.Start(t, pool, tooBig); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a step needing %d of %d slots: %v, want ErrInvalid", pool+1, pool, err)
	}
}
