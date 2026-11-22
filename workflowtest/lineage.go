package workflowtest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
)

// LineageStoreContract checks an events.LineageStore against the contract
// every store keeps, MemoryLineage's semantics: Find of nothing is empty;
// every field round-trips (Meta included, At to the nanosecond); Find
// returns the newest first by At, ties in reverse order of recording;
// Kind, Dataset, Run and Trigger filter alone and together; Limit cuts
// after the order is applied and a negative Limit is workflow.ErrInvalid;
// a record equal in Kind, Dataset, Version, Run, Workflow, Step and
// Trigger to one already recorded, through either store, is ignored and
// the first one's Meta and At stay, while a record differing in any one
// of those fields is another record; of many concurrent Records of one
// fact exactly one is kept (run it under -race); Find hands out copies;
// and a cancelled context fails Record and Find.
//
// a and b are two handles on the same backing data (two processes); pass
// the same store twice for an in-process one. The store must start empty.
//
//ds:def id=workflowtest-lineagestorecontract-u8kuk8pt owner=@khanakia stability=stable
func LineageStoreContract(t *testing.T, a, b events.LineageStore) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	find := func(s events.LineageStore, q events.LineageQuery) []events.Lineage {
		t.Helper()
		got, err := s.Find(ctx, q)
		if err != nil {
			t.Fatalf("Find(%+v): %v", q, err)
		}
		return got
	}
	record := func(s events.LineageStore, l events.Lineage) {
		t.Helper()
		if err := s.Record(ctx, l); err != nil {
			t.Fatalf("Record(%+v): %v", l, err)
		}
	}
	// names lists each record as "<kind>/<dataset>/<version>".
	names := func(ls []events.Lineage) string {
		var out []string
		for _, l := range ls {
			out = append(out, fmt.Sprintf("%s/%s/%s", l.Kind, l.Dataset, l.Version))
		}
		return strings.Join(out, " ")
	}
	expect := func(s events.LineageStore, q events.LineageQuery, want string) {
		t.Helper()
		if got := names(find(s, q)); got != want {
			t.Fatalf("Find(%+v) = [%s]\nwant [%s]", q, got, want)
		}
	}

	if got := find(a, events.LineageQuery{}); len(got) != 0 {
		t.Fatalf("Find on an empty store = %+v", got)
	}

	const P, C = events.LineageProduced, events.LineageConsumed
	long := "s3://warehouse/" + strings.Repeat("partition=2026-11-20/", 20) // past any indexable width
	first := events.Lineage{Kind: P, Dataset: "orders", Version: "v1", Run: "r1", Workflow: "load", Step: "write",
		Meta: map[string]string{"rows": "42", "path": "s3://bucket/orders/v1", "note": "ünïcode, \"quoted\"", "empty": ""},
		At:   t0.Add(123456789 * time.Nanosecond)}
	steps := []struct {
		s events.LineageStore
		l events.Lineage
	}{
		{a, first},
		{b, events.Lineage{Kind: P, Dataset: "orders", Version: "v2", Run: "r2", Workflow: "load", Step: "write", At: t0.Add(time.Minute)}},
		// v3 shares v2's At and is recorded later, so it comes first.
		{a, events.Lineage{Kind: P, Dataset: "orders", Version: "v3", Run: "r3", Workflow: "load", Step: "write", At: t0.Add(time.Minute)}},
		{b, events.Lineage{Kind: P, Dataset: "fx", Version: "f1", Run: "r2", Workflow: "load", Step: "rates", At: t0.Add(30 * time.Second)}},
		{a, events.Lineage{Kind: C, Dataset: "orders", Version: "v3", Run: "r9", Workflow: "report", Trigger: "daily", At: t0.Add(2 * time.Minute)}},
		{b, events.Lineage{Kind: C, Dataset: "fx", Version: "f1", Run: "r9", Workflow: "report", Trigger: "daily", At: t0.Add(2 * time.Minute)}},
		{a, events.Lineage{Kind: C, Dataset: "orders", Version: "v3", Run: "r10", Workflow: "audit", Trigger: "audit", At: t0.Add(3 * time.Minute)}},
		// Recorded newest first: the order comes from At, to the
		// nanosecond, not from recording.
		{b, events.Lineage{Kind: P, Dataset: "ns", Version: "2", At: t0.Add(2)}},
		{a, events.Lineage{Kind: P, Dataset: "ns", Version: "1", At: t0.Add(1)}},
		{b, events.Lineage{Kind: P, Dataset: long, Version: "x", Meta: map[string]string{}, At: t0.Add(-time.Hour)}},
	}
	for _, st := range steps {
		record(st.s, st.l)
	}

	// The order: At latest first, ties latest recorded first.
	expect(b, events.LineageQuery{}, "consumed/orders/v3 consumed/fx/f1 consumed/orders/v3 produced/orders/v3 produced/orders/v2 produced/fx/f1 produced/orders/v1 produced/ns/2 produced/ns/1 produced/"+long+"/x")
	expect(a, events.LineageQuery{Kind: P, Dataset: "orders"}, "produced/orders/v3 produced/orders/v2 produced/orders/v1")
	expect(a, events.LineageQuery{Dataset: "orders"}, "consumed/orders/v3 consumed/orders/v3 produced/orders/v3 produced/orders/v2 produced/orders/v1")
	expect(b, events.LineageQuery{Kind: C}, "consumed/orders/v3 consumed/fx/f1 consumed/orders/v3")
	expect(b, events.LineageQuery{Trigger: "daily"}, "consumed/fx/f1 consumed/orders/v3")
	expect(b, events.LineageQuery{Kind: C, Trigger: "daily", Dataset: "orders"}, "consumed/orders/v3")
	expect(a, events.LineageQuery{Run: "r2"}, "produced/orders/v2 produced/fx/f1")
	expect(a, events.LineageQuery{Run: "r9", Kind: C, Dataset: "fx"}, "consumed/fx/f1")
	expect(a, events.LineageQuery{Run: "r2", Kind: C}, "")
	expect(a, events.LineageQuery{Dataset: "order"}, "")
	expect(a, events.LineageQuery{Trigger: "dail"}, "")
	expect(a, events.LineageQuery{Dataset: "ns"}, "produced/ns/2 produced/ns/1")
	expect(a, events.LineageQuery{Dataset: long}, "produced/"+long+"/x")

	// Limit cuts after ordering; zero means all.
	expect(a, events.LineageQuery{Kind: P, Dataset: "orders", Limit: 1}, "produced/orders/v3")
	expect(b, events.LineageQuery{Dataset: "orders", Limit: 3}, "consumed/orders/v3 consumed/orders/v3 produced/orders/v3")
	expect(b, events.LineageQuery{Trigger: "daily", Limit: 10}, "consumed/fx/f1 consumed/orders/v3")
	if _, err := a.Find(ctx, events.LineageQuery{Limit: -1}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a negative Limit = %v; want ErrInvalid", err)
	}

	// Every field round-trips.
	got := find(b, events.LineageQuery{Run: "r1"})
	if len(got) != 1 {
		t.Fatalf("Find run r1 = %+v", got)
	}
	g := got[0]
	if g.Kind != first.Kind || g.Dataset != first.Dataset || g.Version != first.Version || g.Run != first.Run ||
		g.Workflow != first.Workflow || g.Step != first.Step || g.Trigger != "" || !g.At.Equal(first.At) || !maps.Equal(g.Meta, first.Meta) {
		t.Fatalf("round trip = %+v\nwant %+v", g, first)
	}
	cons := find(a, events.LineageQuery{Kind: C, Run: "r10"})
	if len(cons) != 1 || cons[0].Trigger != "audit" || cons[0].Workflow != "audit" || cons[0].Step != "" || len(cons[0].Meta) != 0 {
		t.Fatalf("consumed round trip = %+v", cons)
	}
	if l := find(a, events.LineageQuery{Dataset: long}); len(l) != 1 || l[0].Meta == nil || len(l[0].Meta) != 0 {
		t.Fatalf("an empty Meta map = %#v; want empty, not nil", l)
	}
	if l := find(a, events.LineageQuery{Dataset: "ns", Limit: 1}); len(l) != 1 || l[0].Meta != nil {
		t.Fatalf("a nil Meta = %#v; want nil", l)
	}

	// Find hands out copies.
	g.Meta["rows"] = "changed"
	if again := find(a, events.LineageQuery{Run: "r1"}); again[0].Meta["rows"] != "42" {
		t.Fatal("changing a found record's Meta changed the store")
	}

	// The same fact again, through the other store, with another Meta
	// and At: ignored, the first stays.
	retry := first
	retry.Meta = map[string]string{"rows": "43"}
	retry.At = t0.Add(time.Hour)
	record(b, retry)
	record(a, retry)
	got = find(a, events.LineageQuery{Run: "r1"})
	if len(got) != 1 || got[0].Meta["rows"] != "42" || !got[0].At.Equal(first.At) {
		t.Fatalf("after recording the same fact again: %+v; want the first record only", got)
	}
	// A record differing in any one identifying field is another record.
	variants := []func(*events.Lineage){
		func(l *events.Lineage) { l.Kind = C },
		func(l *events.Lineage) { l.Dataset = "orders2" },
		func(l *events.Lineage) { l.Version = "v1 " },
		func(l *events.Lineage) { l.Workflow = "load2" },
		func(l *events.Lineage) { l.Step = "" },
		func(l *events.Lineage) { l.Trigger = "t" },
	}
	for _, change := range variants {
		v := first
		change(&v)
		record(b, v)
	}
	if n := len(find(a, events.LineageQuery{Run: "r1"})); n != 1+len(variants) {
		t.Fatalf("%d records of run r1; want %d (each variant is its own fact)", n, 1+len(variants))
	}
	run := first
	run.Run = "r1b"
	record(a, run)
	if n := len(find(b, events.LineageQuery{Run: "r1b"})); n != 1 {
		t.Fatalf("a record differing only in Run: %d records, want 1", n)
	}

	// Of many concurrent Records of one fact, exactly one is kept.
	race := events.Lineage{Kind: P, Dataset: "race", Version: "1", Run: "rr", At: t0}
	var wg sync.WaitGroup
	var failed atomic.Int32
	for i := range 20 {
		s := a
		if i%2 == 1 {
			s = b
		}
		l := race
		l.At = t0.Add(time.Duration(i) * time.Second)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Record(ctx, l); err != nil {
				failed.Add(1)
			}
		}()
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("%d concurrent Records of one fact failed", failed.Load())
	}
	if got := find(b, events.LineageQuery{Dataset: "race"}); len(got) != 1 {
		t.Fatalf("%d records after concurrent Records of one fact; want 1", len(got))
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := a.Record(cancelled, events.Lineage{Kind: P, Dataset: "late", Version: "1", At: t0}); err == nil {
		t.Fatal("Record on a cancelled context succeeded")
	}
	if _, err := a.Find(cancelled, events.LineageQuery{}); err == nil {
		t.Fatal("Find on a cancelled context succeeded")
	}
	if got := find(b, events.LineageQuery{Dataset: "late"}); len(got) != 0 {
		t.Fatalf("a Record on a cancelled context was kept: %+v", got)
	}
}

// DatasetsSharedLineage checks that events.Datasets in two processes (two
// clients on one backend, each with its own bus and Datasets) sharing
// lineage through la and lb (two handles on one store) start each
// trigger once per version: an any-of trigger fires on the first
// announcement and not when the other process announces the same version;
// an all-of trigger waits for both datasets and fires once even when both
// processes announce its last dataset at the same moment (the shared
// store decides the first, the idempotency key of the run still
// unfinished decides the second); and nothing fires when both announce
// the versions again.
//
// start returns a started client on the shared backend with wfs
// registered; it is called twice. The clients must deduplicate by
// idempotency key across each other (CapIdempotencyKey), as two
// processes on one database or Redis do.
//
//ds:def id=workflowtest-datasetssharedlineage-7mgupked owner=@khanakia stability=stable
func DatasetsSharedLineage(t *testing.T, start func(t *testing.T, wfs ...workflow.Workflow) workflow.Client, la, lb events.LineageStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var single, join atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	wfs := []workflow.Workflow{
		{Name: "lineage-single", Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) {
			single.Add(1)
			return nil, nil
		}}}},
		// The join's run waits for the test, so it is unfinished while
		// both processes decide, and its idempotency key holds.
		{Name: "lineage-join", Steps: []workflow.Step{{Name: "run", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			join.Add(1)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, nil
		}}}},
	}
	type proc struct {
		c  workflow.Client
		ds *events.Datasets
	}
	var procs []proc
	for _, store := range []events.LineageStore{la, lb} {
		c := start(t, wfs...)
		bus, err := events.New(c)
		if err != nil {
			t.Fatal(err)
		}
		ds, err := events.NewDatasets(bus, events.WithLineage(store))
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range []events.DatasetTrigger{
			{Name: "single", Datasets: []string{"orders"}, Workflow: "lineage-single"},
			{Name: "join", Datasets: []string{"orders", "fx"}, All: true, Workflow: "lineage-join"},
		} {
			if err := ds.Trigger(tr); err != nil {
				t.Fatal(err)
			}
		}
		procs = append(procs, proc{c, ds})
	}
	a, b := procs[0], procs[1]
	type started struct {
		c   workflow.Client
		ref workflow.RunRef
	}
	var runs []started
	update := func(p proc, ds, version string) []events.TriggerRun {
		t.Helper()
		res, err := p.ds.Updated(ctx, ds, version, nil)
		if err != nil {
			t.Fatalf("Updated(%s, %s): %v", ds, version, err)
		}
		for _, r := range res.Triggered {
			runs = append(runs, started{p.c, r.Ref})
		}
		return res.Triggered
	}

	if got := update(a, "orders", "o1"); len(got) != 1 || got[0].Trigger != "single" {
		t.Fatalf("first announcement of orders o1 started %+v; want the any-of trigger", got)
	}
	if got := update(b, "orders", "o1"); len(got) != 0 {
		t.Fatalf("the other process announcing orders o1 again started %+v; want nothing", got)
	}

	// Both processes announce the join's last dataset at once.
	var wg sync.WaitGroup
	results := make([][]events.TriggerRun, 2)
	errs := make([]error, 2)
	for i, p := range []proc{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := p.ds.Updated(ctx, "fx", "f1", nil)
			results[i], errs[i] = res.Triggered, err
		}()
	}
	wg.Wait()
	joins := 0
	for i, p := range []proc{a, b} {
		if errs[i] != nil {
			t.Fatalf("concurrent Updated(fx, f1): %v", errs[i])
		}
		for _, r := range results[i] {
			if r.Trigger != "join" {
				t.Fatalf("fx f1 started trigger %q", r.Trigger)
			}
			joins++
			runs = append(runs, started{p.c, r.Ref})
		}
	}
	if joins == 0 {
		t.Fatal("fx f1 after orders o1 started no join")
	}
	t.Logf("%d of the two processes decided to start the join; its runs: %d", joins, join.Load())
	open()
	for _, r := range runs {
		if _, err := workflow.Await(ctx, r.c, r.ref, workflow.AwaitOptions{Poll: 5 * time.Millisecond}); err != nil {
			t.Fatalf("await %+v: %v", r.ref, err)
		}
	}
	if single.Load() != 1 || join.Load() != 1 {
		t.Fatalf("single ran %d times and join %d times; want once each", single.Load(), join.Load())
	}

	// Announcing every version again, from either process, starts nothing.
	for _, p := range []proc{a, b} {
		for _, ds := range []string{"orders", "fx"} {
			version := map[string]string{"orders": "o1", "fx": "f1"}[ds]
			if got := update(p, ds, version); len(got) != 0 {
				t.Fatalf("re-announcing %s %s started %+v", ds, version, got)
			}
		}
	}
	prod, err := events.Producers(ctx, la, "orders")
	if err != nil || len(prod) != 1 || prod[0].Version != "o1" {
		t.Fatalf("producers of orders = %+v, %v; want o1 once, though both processes announced it", prod, err)
	}
	cons, err := events.Consumers(ctx, lb, "fx")
	if err != nil || len(cons) == 0 || !slices.ContainsFunc(cons, func(l events.Lineage) bool { return l.Trigger == "join" && l.Version == "f1" }) {
		t.Fatalf("consumers of fx = %+v, %v; want the join's f1", cons, err)
	}
}
