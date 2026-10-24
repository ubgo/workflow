package hatchet_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/hatchet"
)

// TestChoiceLive proves the engine accepts workflow.Choice's dotted step
// names ("<choice>.decide", "<choice>.<branch>") and runs the expansion:
// a step after the choice receives the chosen branch's output, for each
// branch, and the run's result is that step's output.
func TestChoiceLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfp5choice_" + uuid.NewString()[:8]
	branch := func(tag string) workflow.Handler {
		return func(_ context.Context, in []byte) ([]byte, error) {
			return []byte(`"` + tag + `:` + string(in[1:len(in)-1]) + `"`), nil
		}
	}
	steps, err := workflow.Choice(workflow.ChoiceSpec{
		Name: "route",
		Decide: func(_ context.Context, in []byte) (string, error) {
			if string(in) == `"big"` {
				return "large", nil
			}
			return "small", nil
		},
		Branches: []workflow.Branch{{Name: "small", Run: branch("s")}, {Name: "large", Run: branch("l")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	steps = append(steps, workflow.Step{Name: "after", After: []string{"route"}, Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }})
	wf := workflow.Workflow{Name: prefix, Steps: steps}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()
	for in, want := range map[string]string{`"big"`: `"l:big"`, `"tiny"`: `"s:tiny"`} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(in)})
		if err != nil {
			t.Fatal(err)
		}
		got, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{})
		if err != nil || string(got) != want {
			t.Fatalf("input %s: result %s, %v; want %s", in, got, err, want)
		}
	}
}

// TestEventBusLive proves events.Bus on the engine: Publish starts the
// subscription's run, publishing the same message ID again while that
// run is unfinished returns the same RunRef (the idempotency key
// "event/<sub>/<id>"), WithSignal resumes a run waiting on the event's
// name and key, and the reserved event labels pass the label check.
func TestEventBusLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfp5bus_" + uuid.NewString()[:8]
	release := make(chan struct{})
	sub := workflow.Workflow{Name: prefix + "_sub", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		if info.Labels[events.LabelEventID] == "" {
			return nil, workflow.Permanent(errors.New("no event ID label"))
		}
		<-release
		return in, nil
	}}}}
	waiter := workflow.Workflow{Name: prefix + "_wait", Steps: []workflow.Step{
		{Name: "w", Wait: &workflow.WaitSpec{Event: "order.paid", KeyFrom: workflow.KeyFunc(func(workflow.Job) string { return "o1" })}},
	}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, sub, waiter)
	ctx := context.Background()
	bus, err := events.New(c, events.WithSignal())
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.Subscribe(events.Subscription{Name: "fulfil", Event: "order.paid", Workflow: sub.Name}); err != nil {
		t.Fatal(err)
	}
	wref, err := c.Enqueue(ctx, workflow.Job{Workflow: waiter.Name})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, wref, workflow.StatusWaiting)

	m := events.Message{ID: "m-" + prefix, Name: "order.paid", Key: "o1", Payload: []byte(`{"order":"o1"}`)}
	first, err := bus.Publish(ctx, m)
	if err != nil || len(first.Deliveries) != 1 || first.Deliveries[0].Ref.RunID.IsZero() || !first.Signalled {
		t.Fatalf("first Publish = %+v, %v", first, err)
	}
	ref := first.Deliveries[0].Ref
	waitStatus(t, c, ref, workflow.StatusRunning)
	waitStatus(t, c, wref, workflow.StatusSucceeded)
	again, err := bus.Publish(ctx, m)
	if err != nil || len(again.Deliveries) != 1 || again.Deliveries[0].Ref != ref {
		t.Fatalf("republish = %+v, %v; want the same run %+v", again, err, ref)
	}
	close(release)
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	if res, err := c.Result(ctx, ref); err != nil || string(res) != string(m.Payload) {
		t.Fatalf("subscription run result = %s, %v", res, err)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: sub.Name})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("subscription runs = %+v, %v; want exactly one", page.Runs, err)
	}
}

// TestReservedLabelsLive checks the label gate on the engine path:
// Enqueue refuses an app label under a reserved prefix before anything
// reaches the engine, and accepts it when ctx allows that prefix.
func TestReservedLabelsLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfp5lbl_" + uuid.NewString()[:8]
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()
	job := workflow.Job{Workflow: wf.Name, Labels: map[string]string{workflow.LabelTickAt: "2026-01-01T00:00:00Z"}}
	if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("reserved label: %v, want ErrInvalid", err)
	}
	if _, err := c.RunGroup(ctx, "g", []workflow.Job{job}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("reserved label in RunGroup: %v, want ErrInvalid", err)
	}
	ref, err := c.Enqueue(workflow.AllowReservedPrefix(ctx, workflow.LabelPrefixInternal), job)
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
}
