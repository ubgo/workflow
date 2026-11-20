package middleware_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// TestUniqueReplaceUpdatesTheScheduledHolder proves UQ-08: a duplicate of
// a scheduled job replaces its payload and start instead of being dropped,
// and once the holder ran, a duplicate is refused as before.
func TestUniqueReplaceUpdatesTheScheduledHolder(t *testing.T) {
	mc := memory.New()
	got := make(chan string, 4)
	if err := mc.Register(workflow.Workflow{Name: "digest", Steps: []workflow.Step{{Name: "send", Run: func(_ context.Context, in []byte) ([]byte, error) {
		got <- string(in)
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	mw, err := middleware.UniqueWith(middleware.UniqueConfig{
		By:      func(j workflow.Job) string { return "digest:" + j.Labels["user"] },
		Store:   middleware.NewMemoryUnique(clock.Real{}),
		TTL:     time.Hour,
		Client:  mc,
		Replace: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(mc, mw)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"user": "u1"}
	first, err := c.Enqueue(ctx, workflow.Job{Workflow: "digest", Payload: []byte(`["c1"]`), Labels: labels, RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Enqueue(ctx, workflow.Job{Workflow: "digest", Payload: []byte(`["c1","c2"]`), Labels: labels, RunAt: time.Now()})
	if err != nil {
		t.Fatalf("duplicate with Replace: %v", err)
	}
	if second.ScheduledID != first.ScheduledID {
		t.Fatalf("refs %+v and %+v: the duplicate must answer with the holder", first, second)
	}
	select {
	case in := <-got:
		if in != `["c1","c2"]` {
			t.Fatalf("ran with %s, want the replacing payload", in)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the edited holder did not run at its new time")
	}
	if _, err := workflow.Await(ctx, c, second, workflow.AwaitOptions{Poll: 2 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "digest", Payload: []byte(`["c3"]`), Labels: labels}); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("duplicate of a holder that ran = %v, want ErrDuplicate", err)
	}
	if _, err := middleware.UniqueWith(middleware.UniqueConfig{By: func(workflow.Job) string { return "k" }, Store: middleware.NewMemoryUnique(clock.Real{}), TTL: time.Hour, Replace: true}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Replace without a RunEditor client = %v, want ErrInvalid", err)
	}
}
