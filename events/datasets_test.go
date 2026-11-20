package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/events"
)

// datasetsOn returns a bus and a Datasets on c with the given options.
func datasetsOn(t *testing.T, c workflow.Client, opts ...events.DatasetOption) (*events.Bus, *events.Datasets) {
	t.Helper()
	bus, err := events.New(c)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := events.NewDatasets(bus, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return bus, ds
}

// waitTriggered waits for every run an update started.
func waitTriggered(t *testing.T, c workflow.Client, res events.UpdateResult) {
	t.Helper()
	p := events.Published{}
	for _, r := range res.Triggered {
		p.Deliveries = append(p.Deliveries, events.Delivery{Ref: r.Ref})
	}
	waitAll(t, c, p)
	waitAll(t, c, res.Event)
}

func TestDatasetAnyOfFromAStep(t *testing.T) {
	report := &recorder{}
	var ds *events.Datasets
	load := workflow.Workflow{Name: "load", Steps: []workflow.Step{{Name: "write-orders", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		_, err := ds.Updated(ctx, "orders", string(in), map[string]string{"rows": "42"})
		return nil, err
	}}}}
	c := started(t, load, report.workflow("report"))
	fake := clock.NewFake(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
	_, ds = datasetsOn(t, c, events.WithDatasetClock(fake))
	if err := ds.Trigger(events.DatasetTrigger{Name: "daily-report", Datasets: []string{"orders"}, Workflow: "report"}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "load", Payload: []byte("v1")})
	if err != nil {
		t.Fatal(err)
	}
	waitAll(t, c, events.Published{Deliveries: []events.Delivery{{Ref: ref}}})
	for i := 0; report.count() == 0 && i < 2000; i++ {
		time.Sleep(2 * time.Millisecond)
	}
	if report.count() != 1 || report.ins[0] != `{"orders":"v1"}` || report.runs[0].Labels[events.LabelDatasetTrigger] != "daily-report" {
		t.Fatalf("report runs = %d %v", report.count(), report.ins)
	}

	// Lineage names the producing run and step, and what the report read.
	prod, err := events.Producers(ctx, ds.Lineage(), "orders")
	if err != nil || len(prod) != 1 {
		t.Fatal(prod, err)
	}
	p := prod[0]
	if p.Run != ref.RunID || p.Workflow != "load" || p.Step != "write-orders" || p.Version != "v1" || p.Meta["rows"] != "42" || !p.At.Equal(fake.Now()) {
		t.Fatalf("producer = %+v", p)
	}
	outs, err := events.Outputs(ctx, ds.Lineage(), ref.RunID)
	if err != nil || len(outs) != 1 || outs[0].Dataset != "orders" {
		t.Fatalf("outputs = %v, %v", outs, err)
	}
	ins, err := events.Inputs(ctx, ds.Lineage(), report.runs[0].RunID)
	if err != nil || len(ins) != 1 || ins[0].Version != "v1" || ins[0].Trigger != "daily-report" {
		t.Fatalf("inputs = %v, %v", ins, err)
	}

	// The step retried, or the version announced again after the report
	// finished: nothing starts. A new version starts the report again.
	res, err := ds.Updated(ctx, "orders", "v1", nil)
	if err != nil || len(res.Triggered) != 0 {
		t.Fatalf("same version again = %+v, %v", res, err)
	}
	fake.Advance(time.Hour)
	res, err = ds.Updated(ctx, "orders", "v2", nil)
	if err != nil || len(res.Triggered) != 1 || res.Triggered[0].Versions["orders"] != "v2" {
		t.Fatalf("new version = %+v, %v", res, err)
	}
	waitTriggered(t, c, res)
	if report.count() != 2 {
		t.Fatalf("report ran %d times, want 2", report.count())
	}
	// v1 announced from outside a step is a second, separate claim; v2
	// is the newest.
	if prod, _ := events.Producers(ctx, ds.Lineage(), "orders"); len(prod) != 3 || prod[0].Version != "v2" || prod[0].Run != "" {
		t.Fatalf("producers = %+v", prod)
	}
	if cons, _ := events.Consumers(ctx, ds.Lineage(), "orders"); len(cons) != 2 || cons[0].Version != "v2" {
		t.Fatalf("consumers = %+v", cons)
	}
}

func TestDatasetAllOfWaitsForEveryDataset(t *testing.T) {
	report, orderMail := &recorder{}, &recorder{}
	c := started(t, report.workflow("report"), orderMail.workflow("orders-mail"))
	bus, ds := datasetsOn(t, c)
	if err := ds.Trigger(events.DatasetTrigger{Name: "join", Datasets: []string{"orders", "fx"}, All: true, Workflow: "report"}); err != nil {
		t.Fatal(err)
	}
	// A plain bus subscription sees every orders version.
	if err := bus.Subscribe(events.Subscription{Name: "orders-changed", Event: events.EventDatasetUpdated, Workflow: "orders-mail",
		Filter: func(m events.Message) bool { return m.Key == "orders" }}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	step := func(dataset, version string, wantFire map[string]string) {
		t.Helper()
		res, err := ds.Updated(ctx, dataset, version, nil)
		if err != nil {
			t.Fatal(err)
		}
		waitTriggered(t, c, res)
		switch {
		case wantFire == nil && len(res.Triggered) != 0:
			t.Fatalf("%s@%s fired %+v", dataset, version, res.Triggered)
		case wantFire != nil && (len(res.Triggered) != 1 || !maps.Equal(res.Triggered[0].Versions, wantFire)):
			t.Fatalf("%s@%s fired %+v, want %v", dataset, version, res.Triggered, wantFire)
		}
	}
	step("orders", "o1", nil)
	step("fx", "f1", map[string]string{"orders": "o1", "fx": "f1"})
	step("orders", "o2", nil) // fx has not changed since
	step("orders", "o2", nil) // a retried producer
	step("fx", "f2", map[string]string{"orders": "o2", "fx": "f2"})
	if report.count() != 2 || report.ins[1] != `{"fx":"f2","orders":"o2"}` {
		t.Fatalf("report = %d %v", report.count(), report.ins)
	}
	// A plain subscription is deduplicated by event ID only while its run
	// is unfinished, so the finished o2 run's announcement starts it again;
	// a trigger never runs twice for one version.
	if orderMail.count() != 3 {
		t.Fatalf("orders-mail ran %d times, want 3 (o1, o2, o2 again)", orderMail.count())
	}
	var upd events.DatasetUpdate
	if err := json.Unmarshal([]byte(orderMail.ins[0]), &upd); err != nil || upd.Dataset != "orders" || upd.Version != "o1" {
		t.Fatalf("event payload = %s, %v", orderMail.ins[0], err)
	}
	if id := orderMail.runs[0].Labels[events.LabelEventID]; id != events.DatasetEventID("orders", "o1") {
		t.Fatalf("event ID = %q", id)
	}
}

func TestDatasetSharedStoreAcrossProcesses(t *testing.T) {
	report := &recorder{}
	c := started(t, report.workflow("report"))
	store := events.NewMemoryLineage()
	trig := events.DatasetTrigger{Name: "join", Datasets: []string{"orders", "fx"}, All: true, Workflow: "report"}
	_, a := datasetsOn(t, c, events.WithLineage(store))
	_, b := datasetsOn(t, c, events.WithLineage(store))
	for _, d := range []*events.Datasets{a, b} {
		if err := d.Trigger(trig); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if res, err := a.Updated(ctx, "orders", "o1", nil); err != nil || len(res.Triggered) != 0 {
		t.Fatalf("a: %+v, %v", res, err)
	}
	res, err := b.Updated(ctx, "fx", "f1", nil)
	if err != nil || len(res.Triggered) != 1 {
		t.Fatalf("b: %+v, %v", res, err)
	}
	waitTriggered(t, c, res)
	// The other process announcing the same version decides the same way.
	if res, err := a.Updated(ctx, "fx", "f1", nil); err != nil || len(res.Triggered) != 0 {
		t.Fatalf("a again: %+v, %v", res, err)
	}
	if report.count() != 1 {
		t.Fatalf("report ran %d times", report.count())
	}
}

func TestDatasetTriggerFailureRetries(t *testing.T) {
	report := &recorder{}
	c := started(t, report.workflow("report"))
	_, ds := datasetsOn(t, c)
	calls := 0
	err := ds.Trigger(events.DatasetTrigger{Name: "t", Datasets: []string{"orders"}, Job: func(v map[string]string) (workflow.Job, error) {
		calls++
		if calls == 1 {
			return workflow.Job{}, errors.New("config not loaded")
		}
		return workflow.Job{Workflow: "report", Payload: []byte(v["orders"]), Labels: map[string]string{"team": "bi"}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := ds.Updated(ctx, "orders", "o1", nil)
	if err == nil || len(res.Triggered) != 1 || res.Triggered[0].Err == nil {
		t.Fatalf("first = %+v, %v", res, err)
	}
	// Nothing was consumed, so announcing the version again starts it.
	res, err = ds.Updated(ctx, "orders", "o1", nil)
	if err != nil || len(res.Triggered) != 1 {
		t.Fatalf("again = %+v, %v", res, err)
	}
	waitTriggered(t, c, res)
	if report.count() != 1 || report.ins[0] != "o1" || report.runs[0].Labels["team"] != "bi" {
		t.Fatalf("report = %d %v", report.count(), report.ins)
	}
}

func TestDatasetRefusals(t *testing.T) {
	c := started(t, (&recorder{}).workflow("report"))
	if _, err := events.NewDatasets(nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("nil bus: %v", err)
	}
	_, ds := datasetsOn(t, c)
	bad := []events.DatasetTrigger{
		{Datasets: []string{"a"}, Workflow: "report"},
		{Name: "x", Workflow: "report"},
		{Name: "x", Datasets: []string{"a", "a"}, Workflow: "report"},
		{Name: "x", Datasets: []string{""}, Workflow: "report"},
		{Name: "x", Datasets: []string{"a"}},
	}
	for i, tr := range bad {
		if err := ds.Trigger(tr); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("trigger %d: %v", i, err)
		}
	}
	if err := ds.Trigger(events.DatasetTrigger{Name: "ok", Datasets: []string{"a"}, Job: func(map[string]string) (workflow.Job, error) {
		return workflow.Job{Workflow: "report", Labels: map[string]string{events.LabelDatasetTrigger: "forged"}}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := ds.Trigger(events.DatasetTrigger{Name: "ok", Datasets: []string{"b"}, Workflow: "report"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("repeated name: %v", err)
	}
	ctx := context.Background()
	if _, err := ds.Updated(ctx, "", "v", nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty dataset: %v", err)
	}
	if _, err := ds.Updated(ctx, "a", "", nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty version: %v", err)
	}
	// A reserved label from the trigger's Job fails that trigger.
	if _, err := ds.Updated(ctx, "a", "v1", nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("reserved label: %v", err)
	}
	// An app cannot pass a run off as a trigger's.
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "report", Labels: map[string]string{events.LabelDatasetTrigger: "x"}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("forged label: %v", err)
	}
}

func TestDatasetEventIDEscapes(t *testing.T) {
	if events.DatasetEventID("a@b", "c") == events.DatasetEventID("a", "b@c") {
		t.Fatal("an @ inside a name made two versions share an event ID")
	}
}

func TestMemoryLineage(t *testing.T) {
	ctx := context.Background()
	m := events.NewMemoryLineage()
	t0 := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	recs := []events.Lineage{
		{Kind: events.LineageProduced, Dataset: "a", Version: "1", Run: "r1", At: t0, Meta: map[string]string{"n": "1"}},
		{Kind: events.LineageProduced, Dataset: "a", Version: "2", Run: "r2", At: t0.Add(time.Minute)},
		{Kind: events.LineageProduced, Dataset: "a", Version: "3", Run: "r3", At: t0.Add(time.Minute)},
		{Kind: events.LineageConsumed, Dataset: "a", Version: "3", Run: "r9", Trigger: "t", At: t0.Add(2 * time.Minute)},
		{Kind: events.LineageProduced, Dataset: "a", Version: "1", Run: "r1", At: t0.Add(time.Hour), Meta: map[string]string{"n": "2"}}, // a retry: ignored
	}
	for _, r := range recs {
		if err := m.Record(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	prod, err := events.Producers(ctx, m, "a")
	if err != nil {
		t.Fatal(err)
	}
	var versions []string
	for _, p := range prod {
		versions = append(versions, p.Version)
	}
	// Newest first; versions 2 and 3 share At, so the later record wins.
	if !slices.Equal(versions, []string{"3", "2", "1"}) || prod[2].Meta["n"] != "1" {
		t.Fatalf("producers = %v (%+v)", versions, prod)
	}
	prod[2].Meta["n"] = "changed"
	if again, _ := events.Producers(ctx, m, "a"); again[2].Meta["n"] != "1" {
		t.Fatal("Find handed out the store's own Meta map")
	}
	if got, _ := m.Find(ctx, events.LineageQuery{Dataset: "a", Limit: 1}); len(got) != 1 || got[0].Kind != events.LineageConsumed {
		t.Fatalf("limit 1 = %+v", got)
	}
	if got, _ := events.Inputs(ctx, m, "r9"); len(got) != 1 || got[0].Trigger != "t" {
		t.Fatalf("inputs = %+v", got)
	}
	if got, _ := events.Outputs(ctx, m, "r2"); len(got) != 1 || got[0].Version != "2" {
		t.Fatalf("outputs = %+v", got)
	}
	if _, err := events.Outputs(ctx, m, ""); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty run: %v", err)
	}
	if _, err := events.Inputs(ctx, m, ""); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty run: %v", err)
	}
	if _, err := m.Find(ctx, events.LineageQuery{Limit: -1}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("negative limit: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := m.Record(cancelled, recs[0]); err == nil {
		t.Error("Record on a cancelled ctx succeeded")
	}
}
