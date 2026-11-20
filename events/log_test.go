package events_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/events"
)

var t0 = time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)

// TestMemoryLogOrderAndBounds: Range is [from, to) with open zero bounds,
// in time order then append order, even for an out-of-order append;
// Prune drops older entries; stored messages are copies.
func TestMemoryLogOrderAndBounds(t *testing.T) {
	ctx := context.Background()
	l := events.NewMemoryLog()
	add := func(id string, at time.Time) {
		if err := l.Append(ctx, events.Message{ID: id, Name: "e"}, at); err != nil {
			t.Fatal(err)
		}
	}
	add("b", t0.Add(time.Minute))
	add("c", t0.Add(2*time.Minute))
	add("a", t0) // late arrival
	add("b2", t0.Add(time.Minute))
	ids := func(from, to time.Time) []string {
		var out []string
		if err := l.Range(ctx, from, to, func(m events.Message, _ time.Time) error { out = append(out, m.ID); return nil }); err != nil {
			t.Fatal(err)
		}
		return out
	}
	cases := []struct {
		name     string
		from, to time.Time
		want     []string
	}{
		{"all", time.Time{}, time.Time{}, []string{"a", "b", "b2", "c"}},
		{"from inclusive", t0.Add(time.Minute), time.Time{}, []string{"b", "b2", "c"}},
		{"to exclusive", time.Time{}, t0.Add(2 * time.Minute), []string{"a", "b", "b2"}},
		{"window", t0.Add(time.Second), t0.Add(90 * time.Second), []string{"b", "b2"}},
	}
	for _, tc := range cases {
		if got := ids(tc.from, tc.to); !slices.Equal(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
	stop := errors.New("stop")
	n := 0
	if err := l.Range(ctx, time.Time{}, time.Time{}, func(events.Message, time.Time) error { n++; return stop }); !errors.Is(err, stop) || n != 1 {
		t.Fatalf("Range did not stop at fn's error: %v after %d", err, n)
	}
	if got := l.Prune(t0.Add(time.Minute)); got != 1 || l.Len() != 3 {
		t.Fatalf("Prune = %d, Len = %d", got, l.Len())
	}

	m := events.Message{ID: "x", Name: "e", Payload: []byte("p"), Labels: map[string]string{"k": "v"}}
	_ = l.Append(ctx, m, t0.Add(time.Hour))
	m.Payload[0], m.Labels["k"] = 'X', "changed"
	_ = l.Range(ctx, t0.Add(time.Hour), time.Time{}, func(got events.Message, _ time.Time) error {
		if string(got.Payload) != "p" || got.Labels["k"] != "v" {
			t.Fatalf("stored message changed with its publisher's copy: %+v", got)
		}
		return nil
	})
}

// holdWorkflow returns a workflow whose runs block until the test ends,
// so their idempotency keys stay held.
func holdWorkflow(t *testing.T, name string) workflow.Workflow {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	}}}}
}

// failingLog fails every Append.
type failingLog struct{ events.MemoryLog }

func (*failingLog) Append(context.Context, events.Message, time.Time) error {
	return errors.New("disk full")
}

// TestPublishLogs: every accepted message is logged at the clock's time,
// matched or not; refused ones are not; an Append error fails the publish
// before any delivery.
func TestPublishLogs(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(t0)
	l := events.NewMemoryLog()
	var rec recorder
	c := started(t, rec.workflow("w"))
	b, err := events.New(c, events.WithLog(l), events.WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Subscribe(events.Subscription{Name: "s", Event: "matched", Workflow: "w"})
	_, _ = b.Publish(ctx, events.Message{ID: "1", Name: "matched"})
	clk.Advance(time.Minute)
	_, _ = b.Publish(ctx, events.Message{ID: "2", Name: "nobody-listens"})
	_, _ = b.Publish(ctx, events.Message{Name: ""}) // refused
	var got []string
	_ = l.Range(ctx, time.Time{}, time.Time{}, func(m events.Message, at time.Time) error {
		got = append(got, m.ID+"@"+at.Sub(t0).String())
		return nil
	})
	if !slices.Equal(got, []string{"1@0s", "2@1m0s"}) {
		t.Fatalf("log = %v", got)
	}

	fb, _ := events.New(c, events.WithLog(&failingLog{}))
	_ = fb.Subscribe(events.Subscription{Name: "s", Event: "matched", Workflow: "w"})
	p, err := fb.Publish(ctx, events.Message{ID: "3", Name: "matched"})
	if err == nil || len(p.Deliveries) != 0 || p.ID != "3" {
		t.Fatalf("Publish with a failing log = %+v, %v; want an error and no delivery", p, err)
	}
}

// replayEnv is a bus with a log over a client whose runs hold, with one
// message published to two subscriptions.
func replayEnv(t *testing.T, c workflow.Client) (*events.Bus, events.Published) {
	t.Helper()
	b, err := events.New(c, events.WithLog(events.NewMemoryLog()))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []events.Subscription{
		{Name: "receipt", Event: "order.paid", Workflow: "hold"},
		{Name: "crm", Event: "order.paid", Workflow: "hold"},
		{Name: "big-only", Event: "order.paid", Workflow: "hold", Filter: func(m events.Message) bool { return string(m.Payload) == "big" }},
	} {
		if err := b.Subscribe(s); err != nil {
			t.Fatal(err)
		}
	}
	p, err := b.Publish(context.Background(), events.Message{ID: "evt_1", Name: "order.paid", Payload: []byte("small")})
	if err != nil || len(p.Deliveries) != 2 {
		t.Fatalf("publish = %+v, %v", p, err)
	}
	return b, p
}

// TestReplayIsNotSwallowedByOriginalDedup: while the original runs are
// unfinished (their keys held), a replay still starts new runs, labelled
// with the replay ID; a repeated replay with the same ID starts nothing;
// a new ID starts again.
func TestReplayIsNotSwallowedByOriginalDedup(t *testing.T) {
	ctx := context.Background()
	c := started(t, holdWorkflow(t, "hold"))
	b, orig := replayEnv(t, c)

	r, err := b.Replay(ctx, events.ReplayOptions{ID: "fix-1"})
	if err != nil {
		t.Fatal(err)
	}
	if r.ID != "fix-1" || r.Messages != 1 || len(r.Deliveries) != 2 {
		t.Fatalf("replay = %+v", r)
	}
	for i, d := range r.Deliveries {
		if d.Existing || d.Ref == orig.Deliveries[i].Ref || d.Ref.IsZero() {
			t.Fatalf("delivery %d = %+v: want a new run, not the original %v", i, d, orig.Deliveries[i].Ref)
		}
		info := runInfo(t, c, d.Ref)
		if info.Labels[events.LabelReplay] != "fix-1" || info.Labels[events.LabelEventID] != "evt_1" {
			t.Fatalf("replayed run labels %v", info.Labels)
		}
		// The documented key: enqueuing under it returns the replayed run.
		same, err := c.Enqueue(ctx, workflow.Job{Workflow: "hold", IdempotencyKey: "event/" + d.Subscription + "/evt_1/replay/fix-1"})
		if err != nil || same != d.Ref {
			t.Fatalf("key event/%s/evt_1/replay/fix-1 gave %v, %v; want the replayed run %v", d.Subscription, same, err, d.Ref)
		}
	}

	again, err := b.Replay(ctx, events.ReplayOptions{ID: "fix-1"})
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range again.Deliveries {
		if !d.Existing || d.Ref != r.Deliveries[i].Ref {
			t.Fatalf("repeated replay delivery %d = %+v; want the existing run %v", i, d, r.Deliveries[i].Ref)
		}
	}

	fresh, err := b.Replay(ctx, events.ReplayOptions{ID: "fix-2", Subscriptions: []string{"crm"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(fresh.Deliveries) != 1 || fresh.Deliveries[0].Subscription != "crm" || fresh.Deliveries[0].Existing {
		t.Fatalf("replay fix-2 = %+v", fresh)
	}
	if n := countRuns(t, c); n != 5 {
		t.Fatalf("%d runs, want 5 (2 original, 2 fix-1, 1 fix-2)", n)
	}
}

// TestReplaySameIDAfterRunsFinished: on a client with run queries, a
// replay repeated after its runs finished (keys released) still starts
// nothing.
func TestReplaySameIDAfterRunsFinished(t *testing.T) {
	ctx := context.Background()
	var rec recorder
	c := started(t, rec.workflow("hold"))
	b, orig := replayEnv(t, c)
	waitAll(t, c, orig)
	r, err := b.Replay(ctx, events.ReplayOptions{ID: "fix"})
	if err != nil {
		t.Fatal(err)
	}
	waitAll(t, c, events.Published{Deliveries: []events.Delivery{{Ref: r.Deliveries[0].Ref}, {Ref: r.Deliveries[1].Ref}}})
	again, err := b.Replay(ctx, events.ReplayOptions{ID: "fix"})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range again.Deliveries {
		if !d.Existing {
			t.Fatalf("repeated replay after finish started a run: %+v", d)
		}
	}
	if rec.count() != 4 {
		t.Fatalf("%d runs, want 4", rec.count())
	}
}

// noQuery hides the client's RunQuerier, as a driver without run queries.
type noQuery struct{ workflow.Client }

// TestReplayWithoutRunQueryDedupsByKey: without RunQuerier the repeated
// replay is deduplicated by its idempotency key while the run is
// unfinished.
func TestReplayWithoutRunQueryDedupsByKey(t *testing.T) {
	ctx := context.Background()
	c := started(t, holdWorkflow(t, "hold"))
	b, _ := replayEnv(t, noQuery{c})
	r, err := b.Replay(ctx, events.ReplayOptions{ID: "fix"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := b.Replay(ctx, events.ReplayOptions{ID: "fix"})
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range again.Deliveries {
		if d.Existing || d.Ref != r.Deliveries[i].Ref {
			t.Fatalf("delivery %d = %+v; want the key to return run %v", i, d, r.Deliveries[i].Ref)
		}
	}
}

// TestReplaySelection: the time range, Filter, subscription Filter and a
// message logged twice are honoured; bad requests are refused.
func TestReplaySelection(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(t0)
	var rec recorder
	c := started(t, rec.workflow("w"))
	b, err := events.New(c, events.WithLog(events.NewMemoryLog()), events.WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Subscribe(events.Subscription{Name: "all", Event: "e", Workflow: "w"})
	_ = b.Subscribe(events.Subscription{Name: "vip", Event: "e", Workflow: "w", Filter: func(m events.Message) bool { return m.Key == "vip" }})
	_ = b.SubscribeCancel(events.CancelSubscription{Name: "stop", Event: "e", Label: "order"})
	for i, key := range []string{"a", "vip", "b"} {
		_, _ = b.Publish(ctx, events.Message{ID: key, Name: "e", Key: key})
		clk.Advance(time.Minute)
		if i == 1 {
			_, _ = b.Publish(ctx, events.Message{ID: key, Name: "e", Key: key}) // a retried publish: logged twice
		}
	}
	got := func(o events.ReplayOptions) []string {
		r, err := b.Replay(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, d := range r.Deliveries {
			out = append(out, d.MessageID+">"+d.Subscription)
		}
		return out
	}
	cases := []struct {
		name string
		o    events.ReplayOptions
		want []string
	}{
		{"everything once", events.ReplayOptions{}, []string{"a>all", "vip>all", "vip>vip", "b>all"}},
		{"window", events.ReplayOptions{From: t0.Add(time.Minute), To: t0.Add(2 * time.Minute)}, []string{"vip>all", "vip>vip"}},
		{"filter", events.ReplayOptions{Filter: func(m events.Message) bool { return m.Key != "a" }}, []string{"vip>all", "vip>vip", "b>all"}},
		{"one subscription", events.ReplayOptions{Subscriptions: []string{"vip"}}, []string{"vip>vip"}},
	}
	for _, tc := range cases {
		if g := got(tc.o); !slices.Equal(g, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, g, tc.want)
		}
	}

	refused := map[string]events.ReplayOptions{
		"unknown subscription": {Subscriptions: []string{"nope"}},
		"cancel subscription":  {Subscriptions: []string{"stop"}},
		"from not before to":   {From: t0, To: t0},
	}
	for name, o := range refused {
		if _, err := b.Replay(ctx, o); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	nolog, _ := events.New(c)
	if _, err := nolog.Replay(ctx, events.ReplayOptions{}); !errors.Is(err, workflow.ErrInvalid) || !strings.Contains(err.Error(), "WithLog") {
		t.Errorf("Replay without a log: %v", err)
	}
}

// TestReplayUpcasts: a message logged at an old version is upcast with
// the upcasters of the replaying process.
func TestReplayUpcasts(t *testing.T) {
	ctx := context.Background()
	var rec recorder
	c := started(t, rec.workflow("w"))
	l := events.NewMemoryLog()
	_ = l.Append(ctx, events.Message{ID: "old", Name: "order.paid", Version: 0, Payload: []byte(`{"amount":7}`)}, t0)
	b, err := events.New(c, events.WithLog(l), events.WithUpcasters("order.paid", 2, orderUpcasters))
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Subscribe(events.Subscription{Name: "s", Event: "order.paid", Workflow: "w"})
	r, err := b.Replay(ctx, events.ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitAll(t, c, events.Published{Deliveries: []events.Delivery{{Ref: r.Deliveries[0].Ref}}})
	if rec.ins[0] != `{"cents":7,"currency":"EUR"}` {
		t.Fatalf("replayed payload = %s", rec.ins[0])
	}
}

// runInfo reads one run.
func runInfo(t *testing.T, c workflow.Client, ref workflow.RunRef) workflow.RunInfo {
	t.Helper()
	page, err := c.(workflow.RunQuerier).Runs(context.Background(), workflow.RunQuery{Ref: ref, Limit: 1})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(%v) = %v, %v", ref, page, err)
	}
	return page.Runs[0]
}

// countRuns counts every run of c.
func countRuns(t *testing.T, c workflow.Client) int {
	t.Helper()
	page, err := c.(workflow.RunQuerier).Runs(context.Background(), workflow.RunQuery{Limit: workflow.MaxRunsLimit})
	if err != nil {
		t.Fatal(err)
	}
	return len(page.Runs)
}
