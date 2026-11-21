package events_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/memory"
)

// shop is a three-service choreography with no orchestrator. The payment
// service charges on order.placed and publishes payment.failed or
// payment.captured; the order service compensates a failed payment by
// publishing order.cancelled, or confirms; the inventory service reserves
// on order.placed and releases on order.cancelled. Each service is a
// workflow whose step publishes through the bus it captured.
type shop struct {
	bus *events.Bus

	mu  sync.Mutex
	ran map[string][]workflow.StepInfo // workflow -> the steps that ran
}

func (s *shop) record(ctx context.Context) workflow.StepInfo {
	info, _ := workflow.StepInfoFrom(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ran == nil {
		s.ran = map[string][]workflow.StepInfo{}
	}
	s.ran[info.Workflow] = append(s.ran[info.Workflow], info)
	return info
}

func (s *shop) runs(wf string) []workflow.StepInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ran[wf])
}

// emit is a one-step workflow that records its run and publishes the
// event next returns, if any, with an ID derived from the event that
// started the run, so a retried step publishes the same ID again.
func (s *shop) emit(name string, next func(payload []byte) string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, p []byte) ([]byte, error) {
		info := s.record(ctx)
		if next == nil {
			return nil, nil
		}
		ev := next(p)
		if ev == "" {
			return nil, nil
		}
		_, err := s.bus.Publish(ctx, events.Message{ID: ev + "/" + info.Labels[events.LabelEventID], Name: ev, Payload: p})
		return nil, err
	}}}}
}

// newShop registers the services on a started memory client and wires
// the bus with opts.
func newShop(t *testing.T, opts ...events.Option) (*shop, *memory.Client) {
	t.Helper()
	s := &shop{}
	charge := func(p []byte) string {
		if strings.Contains(string(p), "declined") {
			return "payment.failed"
		}
		return "payment.captured"
	}
	c := started(t,
		s.emit("charge", charge),
		s.emit("reserve", nil),
		s.emit("cancel-order", func([]byte) string { return "order.cancelled" }),
		s.emit("confirm-order", nil),
		s.emit("release", nil),
	)
	bus, err := events.New(c, opts...)
	if err != nil {
		t.Fatal(err)
	}
	s.bus = bus
	for _, sub := range []events.Subscription{
		{Name: "payment-charge", Event: "order.placed", Workflow: "charge"},
		{Name: "inventory-reserve", Event: "order.placed", Workflow: "reserve"},
		{Name: "order-compensate", Event: "payment.failed", Workflow: "cancel-order"},
		{Name: "order-confirm", Event: "payment.captured", Workflow: "confirm-order"},
		{Name: "inventory-release", Event: "order.cancelled", Workflow: "release"},
	} {
		if err := bus.Subscribe(sub); err != nil {
			t.Fatal(err)
		}
	}
	return s, c
}

// place publishes order.placed and runs everything it leads to.
func place(t *testing.T, s *shop, c *memory.Client, id, payload string) {
	t.Helper()
	if _, err := s.bus.Publish(context.Background(), events.Message{ID: id, Name: "order.placed", Payload: []byte(payload)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RunAll(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// byWorkflow indexes a trail by workflow name; each workflow runs once
// per order here.
func byWorkflow(t *testing.T, runs []workflow.RunInfo) map[string]workflow.RunInfo {
	t.Helper()
	out := map[string]workflow.RunInfo{}
	for _, r := range runs {
		if _, dup := out[r.Workflow]; dup {
			t.Fatalf("workflow %s twice in the trail", r.Workflow)
		}
		out[r.Workflow] = r
	}
	return out
}

func TestChoreographyTrail(t *testing.T) {
	s, c := newShop(t, events.WithCorrelation())
	ctx := context.Background()
	place(t, s, c, "ord-1", `{"order":1,"card":"declined"}`)
	place(t, s, c, "ord-2", `{"order":2,"card":"ok"}`)

	// The declined order: payment failed, the order service compensated,
	// inventory released what it reserved; one trail holds all four runs.
	trail, err := events.Trail(ctx, c, "ord-1", workflow.RunQuery{})
	if err != nil {
		t.Fatal(err)
	}
	got := byWorkflow(t, trail)
	if len(got) != 4 || got["charge"].Ref.RunID.IsZero() || got["reserve"].Ref.RunID.IsZero() ||
		got["cancel-order"].Ref.RunID.IsZero() || got["release"].Ref.RunID.IsZero() {
		t.Fatalf("trail of ord-1 = %v", got)
	}
	for wf, r := range got {
		if r.Status != workflow.StatusSucceeded || r.Labels[events.LabelCorrelation] != "ord-1" {
			t.Errorf("%s: status %s, labels %v", wf, r.Status, r.Labels)
		}
	}
	// Causation chains each hop to the event that started the hop before.
	for wf, want := range map[string]string{
		"charge":       "",
		"reserve":      "",
		"cancel-order": "ord-1",
		"release":      "payment.failed/ord-1",
	} {
		cause, ok := got[wf].Labels[events.LabelCausation]
		if cause != want || ok != (want != "") {
			t.Errorf("%s causation = %q (present %v), want %q", wf, cause, ok, want)
		}
	}
	if got["release"].Labels[events.LabelEventID] != "order.cancelled/payment.failed/ord-1" {
		t.Errorf("release event = %v", got["release"].Labels)
	}
	if n := len(s.runs("confirm-order")); n != 1 {
		t.Errorf("confirm-order ran %d times, want 1 (ord-2 only)", n)
	}

	// The paid order has its own trail and never mixes with the first.
	trail2, err := events.Trail(ctx, c, "ord-2", workflow.RunQuery{})
	if err != nil {
		t.Fatal(err)
	}
	got2 := byWorkflow(t, trail2)
	if len(got2) != 3 || got2["confirm-order"].Labels[events.LabelCausation] != "ord-2" {
		t.Fatalf("trail of ord-2 = %v", got2)
	}

	// Paging follows Next; q narrows; q's labels are not changed.
	paged, err := events.Trail(ctx, c, "ord-1", workflow.RunQuery{Limit: 1})
	if err != nil || len(paged) != 4 {
		t.Fatalf("paged trail = %d runs, %v", len(paged), err)
	}
	labels := map[string]string{events.LabelCorrelation: "ord-2"}
	only, err := events.Trail(ctx, c, "ord-1", workflow.RunQuery{Workflow: "release", Labels: labels})
	if err != nil || len(only) != 1 || only[0].Workflow != "release" || labels[events.LabelCorrelation] != "ord-2" {
		t.Fatalf("narrowed trail = %v, %v; q labels %v", only, err, labels)
	}
	if none, err := events.Trail(ctx, c, "nobody", workflow.RunQuery{}); err != nil || len(none) != 0 {
		t.Fatalf("unknown correlation = %v, %v", none, err)
	}
}

func TestChoreographyExplicitCorrelationAndReplay(t *testing.T) {
	log := events.NewMemoryLog()
	s, c := newShop(t, events.WithCorrelation(), events.WithLog(log))
	ctx := context.Background()
	_, err := s.bus.Publish(ctx, events.Message{ID: "evt-9", Name: "order.placed", Correlation: "order-9", Payload: []byte(`{"card":"declined"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RunAll(ctx); err != nil {
		t.Fatal(err)
	}
	trail, err := events.Trail(ctx, c, "order-9", workflow.RunQuery{})
	if err != nil || len(trail) != 4 {
		t.Fatalf("explicit correlation trail = %d runs, %v", len(trail), err)
	}
	if got := byWorkflow(t, trail)["cancel-order"].Labels[events.LabelCausation]; got != "evt-9" {
		t.Fatalf("cancel-order causation = %q", got)
	}

	// The log kept the filled fields, so a replay of the compensation
	// stays in the chain.
	r, err := s.bus.Replay(ctx, events.ReplayOptions{ID: "r1", Subscriptions: []string{"order-compensate"}})
	if err != nil || len(r.Deliveries) != 1 {
		t.Fatalf("replay = %+v, %v", r, err)
	}
	if _, err := c.RunAll(ctx); err != nil {
		t.Fatal(err)
	}
	trail, err = events.Trail(ctx, c, "order-9", workflow.RunQuery{Workflow: "cancel-order"})
	if err != nil || len(trail) != 2 {
		t.Fatalf("trail after replay = %d cancel-order runs, %v", len(trail), err)
	}
	for _, run := range trail {
		if run.Labels[events.LabelCausation] != "evt-9" {
			t.Errorf("cancel-order run %v causation = %q", run.Ref.RunID, run.Labels[events.LabelCausation])
		}
	}
}

func TestChoreographyOffByDefault(t *testing.T) {
	s, c := newShop(t)
	ctx := context.Background()
	place(t, s, c, "ord-1", `{"card":"declined"}`)
	if n := len(s.runs("release")); n != 1 {
		t.Fatalf("release ran %d times without WithCorrelation, want 1", n)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{})
	if err != nil || len(page.Runs) != 4 {
		t.Fatalf("runs = %d, %v", len(page.Runs), err)
	}
	for _, r := range page.Runs {
		if _, ok := r.Labels[events.LabelCorrelation]; ok {
			t.Errorf("%s carries %s without WithCorrelation", r.Workflow, events.LabelCorrelation)
		}
		if _, ok := r.Labels[events.LabelCausation]; ok {
			t.Errorf("%s carries %s without WithCorrelation", r.Workflow, events.LabelCausation)
		}
	}
	if trail, err := events.Trail(ctx, c, "ord-1", workflow.RunQuery{}); err != nil || len(trail) != 0 {
		t.Fatalf("trail without WithCorrelation = %v, %v", trail, err)
	}
	// The fields need the option rather than being dropped silently.
	for _, m := range []events.Message{
		{Name: "order.placed", Correlation: "x"},
		{Name: "order.placed", Causation: "x"},
	} {
		if _, err := s.bus.Publish(ctx, m); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("publish %+v without WithCorrelation = %v, want ErrInvalid", m, err)
		}
	}
}

func TestChoreographyLabelsCannotBeForged(t *testing.T) {
	rec := &recorder{}
	c := started(t, rec.workflow("w"))
	bus, err := events.New(c, events.WithCorrelation())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, key := range []string{events.LabelCorrelation, events.LabelCausation} {
		if _, err := bus.Publish(ctx, events.Message{Name: "e", Labels: map[string]string{key: "forged"}}); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("message label %s = %v, want ErrInvalid", key, err)
		}
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Labels: map[string]string{key: "forged"}}); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("public Enqueue with %s = %v, want ErrInvalid", key, err)
		}
	}
	if err := bus.Subscribe(events.Subscription{Name: "forge", Event: "e", Job: func(events.Message) (workflow.Job, error) {
		return workflow.Job{Workflow: "w", Labels: map[string]string{events.LabelCorrelation: "forged"}}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	p, err := bus.Publish(ctx, events.Message{Name: "e"})
	if !errors.Is(err, workflow.ErrInvalid) || len(p.Deliveries) != 1 || p.Deliveries[0].Err == nil {
		t.Fatalf("Job with %s = %+v, %v; want ErrInvalid", events.LabelCorrelation, p, err)
	}
	if rec.count() != 0 {
		t.Fatalf("a forged run ran")
	}
}

// noQueryCap keeps Runs but does not declare CapRunQuery.
type noQueryCap struct{ *memory.Client }

func (n noQueryCap) Capabilities() workflow.Caps {
	return n.Client.Capabilities().Without(workflow.CapRunQuery)
}

func TestTrailRefusals(t *testing.T) {
	c := started(t)
	ctx := context.Background()
	for name, client := range map[string]workflow.Client{"no RunQuerier": noQuery{c}, "no CapRunQuery": noQueryCap{c}} {
		_, err := events.Trail(ctx, client, "x", workflow.RunQuery{})
		var ue *workflow.UnsupportedError
		if !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapRunQuery) || !errors.Is(err, workflow.ErrUnsupported) {
			t.Errorf("%s: Trail = %v, want UnsupportedError naming CapRunQuery", name, err)
		}
	}
	if _, err := events.Trail(ctx, c, "", workflow.RunQuery{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty correlation = %v, want ErrInvalid", err)
	}
	if _, err := events.Trail(ctx, nil, "x", workflow.RunQuery{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("nil client = %v, want ErrInvalid", err)
	}
	if _, err := events.Trail(ctx, c, "x", workflow.RunQuery{Limit: workflow.MaxRunsLimit + 1}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("invalid query = %v, want ErrInvalid", err)
	}
}
