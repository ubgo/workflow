package dsl_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/dsl"
	"github.com/ubgo/workflow/memory"
)

// orderFlow is a three-step DAG whose middle step waits for the payment of
// the run's own order, read from the "order" label.
const orderFlow = `{
  "name": "order",
  "steps": [
    {"name": "reserve", "handler": "reserve"},
    {"name": "await-payment", "after": ["reserve"], "wait": {"event": "payment.succeeded", "key_label": "order", "timeout": "1h"}},
    {"name": "ship", "after": ["await-payment"], "handler": "ship"}
  ]
}`

// TestLoadedDefinitionRunsOnMemory loads a definition from JSON, registers
// it on the memory driver and runs it end to end: the wait holds the run
// until the matching signal, and the last step's output is the result.
func TestLoadedDefinitionRunsOnMemory(t *testing.T) {
	reg := dsl.NewRegistry()
	type payment struct{ Order, Paid string }
	if err := reg.Register("reserve", workflow.Define(func(_ context.Context, id string) (string, error) {
		return "reserved " + id, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register("ship", workflow.Define(func(_ context.Context, p payment) (string, error) {
		return fmt.Sprintf("shipped %s (paid %s)", p.Order, p.Paid), nil
	})); err != nil {
		t.Fatal(err)
	}
	wf, err := dsl.Load([]byte(orderFlow), reg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := memory.New()
	defer c.Stop()
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "order", Payload: []byte(`"A-1"`), Labels: map[string]string{"order": "A-1"}})
	if err != nil {
		t.Fatal(err)
	}
	for st, _ := c.Status(ctx, ref); st != workflow.StatusWaiting; st, _ = c.Status(ctx, ref) {
		if ctx.Err() != nil {
			t.Fatalf("run never reached waiting; last status %s", st)
		}
		time.Sleep(time.Millisecond)
	}
	// Another order's payment must not resume this run.
	if err := c.Signal(ctx, workflow.Event{Name: "payment.succeeded", Key: "B-2", Payload: []byte(`{"Order":"B-2","Paid":"9"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Signal(ctx, workflow.Event{Name: "payment.succeeded", Key: "A-1", Payload: []byte(`{"Order":"A-1","Paid":"40"}`)}); err != nil {
		t.Fatal(err)
	}
	out, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("result %s: %v", out, err)
	}
	if want := "shipped A-1 (paid 40)"; got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
}
