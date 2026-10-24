package admin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestDiscardedBesideFailures runs a discarding and a failing workflow
// on the memory driver and proves Failures and Discarded each list only
// their own run, and that QueueStats carries the driver's worker counts.
func TestDiscardedBesideFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := memory.New(memory.WithWorkers(4), memory.WithQueues(workflow.QueueConfig{Name: "mail", Workers: 2}, workflow.QueueConfig{Name: "sms"}))
	step := func(err error) []workflow.Step {
		return []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) { return nil, err }}}
	}
	for _, wf := range []workflow.Workflow{
		{Name: "drop", Queue: "mail", Steps: step(workflow.Discard(errors.New("bad address")))},
		{Name: "boom", Queue: "mail", Steps: step(workflow.Permanent(errors.New("smtp down")))},
	} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ended := make(chan workflow.RunEvent, 2)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ended <- ev }); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"drop", "boom"} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: name}); err != nil {
			t.Fatal(err)
		}
	}
	got := map[string]workflow.Status{}
	for range 2 {
		select {
		case ev := <-ended:
			got[ev.Workflow] = ev.Status
		case <-ctx.Done():
			t.Fatal("runs did not end")
		}
	}
	if got["drop"] != workflow.StatusDiscarded || got["boom"] != workflow.StatusFailed {
		t.Fatalf("run ends = %v", got)
	}

	a := New(c)
	fails, err := a.Failures(ctx, workflow.RunQuery{})
	if err != nil || len(fails.Runs) != 1 || fails.Runs[0].Workflow != "boom" {
		t.Fatalf("Failures = %+v, %v", fails.Runs, err)
	}
	dis, err := a.Discarded(ctx, workflow.RunQuery{})
	if err != nil || len(dis.Runs) != 1 || dis.Runs[0].Workflow != "drop" || dis.Runs[0].Error == "" {
		t.Fatalf("Discarded = %+v, %v", dis.Runs, err)
	}
	if _, err := a.Discarded(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Discarded with a status filter: %v", err)
	}

	rep, err := a.QueueStats(ctx)
	if err != nil || !rep.Workers {
		t.Fatalf("QueueStats = %+v, %v", rep, err)
	}
	want := map[string]int{"mail": 2, "sms": 4}
	if len(rep.Queues) != len(want) {
		t.Fatalf("queues = %+v", rep.Queues)
	}
	for _, s := range rep.Queues {
		if s.Workers != want[s.Queue] || s.Depth != 0 || s.Running != 0 {
			t.Errorf("queue %+v, want %d workers", s, want[s.Queue])
		}
	}

	h := NewHandler(a, bearerAuth())
	if rec := do(t, h, "GET", "/queues", ""); !strings.Contains(rec.Body.String(), `"queue":"mail","depth":0,`) || !strings.Contains(rec.Body.String(), `"workers":2,"running":0`) {
		t.Errorf("/queues = %s", rec.Body)
	}
	if rec := do(t, h, "GET", "/discarded", ""); !strings.Contains(rec.Body.String(), `"workflow":"drop"`) || strings.Contains(rec.Body.String(), `"boom"`) {
		t.Errorf("/discarded = %s", rec.Body)
	}
}

// TestDiscardedNeedsRunStates proves Discarded is refused, and reported
// unsupported, on a client without CapRunStates, and that QueueStats
// leaves worker counts out on a client that cannot scale.
func TestDiscardedNeedsRunStates(t *testing.T) {
	f := &fakeClient{caps: allCaps &^ workflow.CapRunStates}
	a := New(f)
	var ue *workflow.UnsupportedError
	if _, err := a.Discarded(context.Background(), workflow.RunQuery{}); !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapRunStates) {
		t.Fatalf("Discarded = %v", err)
	}
	if a.Supports(OpDiscarded) || !a.Supports(OpFailures) {
		t.Fatal("Supports: discarded must need CapRunStates, failures must not")
	}
	if len(f.Calls()) != 0 {
		t.Fatalf("a refused call reached the client: %v", f.Calls())
	}
	rep, err := a.QueueStats(context.Background())
	if err != nil || rep.Workers {
		t.Fatalf("QueueStats = %+v, %v", rep, err)
	}
}
