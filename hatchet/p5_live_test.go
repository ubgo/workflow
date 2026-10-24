package hatchet_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// TestTenantNodeLive proves CapTenant and the node data on the engine:
// Job.Tenant reaches StepInfo.Tenant, RunInfo.Tenant and RunEvent.Tenant,
// RunQuery.Tenant filters on the engine; StepInfo.Node is Config.NodeID
// and Nodes lists the node from the engine's worker labels; a failed run's
// RunEvent.Err is a *workflow.RunError naming the run, step and attempt,
// while RunInfo.Error stays the step's own message (DX-15).
func TestTenantNodeLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfp5tn_" + uuid.NewString()[:8]
	node := "node-" + prefix
	type seen struct{ tenant, node string }
	got := make(chan seen, 4)
	wf := workflow.Workflow{Name: prefix, Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		got <- seen{info.Tenant, info.Node}
		if string(in) == `"fail"` {
			return nil, errors.New("boom")
		}
		return in, nil
	}}}}
	cfg := hatchet.Config{WorkerName: prefix, NodeID: node, RunFinishedPoll: 500 * time.Millisecond}
	c := hatchet.New(cfg)
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	events := make(chan workflow.RunEvent, 4)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { events <- ev }); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"ok"`), Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"ok"`), Tenant: "globex"})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	waitStatus(t, c, other, workflow.StatusSucceeded)
	for range 2 {
		s := <-got
		if s.node != node || (s.tenant != "acme" && s.tenant != "globex") {
			t.Fatalf("StepInfo tenant %q node %q, want acme|globex and %q", s.tenant, s.node, node)
		}
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || page.Runs[0].Ref.RunID != ref.RunID || page.Runs[0].Tenant != "acme" {
		t.Fatalf("Runs(tenant acme) = %+v, want only %s", page.Runs, ref.RunID)
	}
	nodes, err := c.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(nodes, func(n workflow.NodeInfo) bool { return n.ID == node })
	if i < 0 || nodes[i].Workers < 1 || nodes[i].StartedAt.IsZero() || nodes[i].LastSeen.IsZero() {
		t.Fatalf("Nodes = %+v, want %s with workers and times", nodes, node)
	}

	failed, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`"fail"`), Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, failed, workflow.StatusFailed)
	deadline := time.After(liveWait)
	for {
		var ev workflow.RunEvent
		select {
		case ev = <-events:
		case <-deadline:
			t.Fatal("no run-finished event for the failed run")
		}
		if ev.Ref.RunID != failed.RunID {
			continue
		}
		re, ok := workflow.RunErrorOf(ev.Err)
		if !ok || re.RunID != failed.RunID || re.Workflow != wf.Name || re.Step != "a" || re.Attempt != 2 || ev.Tenant != "acme" {
			t.Fatalf("failed RunEvent = %+v (RunError %+v), want a RunError for step a attempt 2, tenant acme", ev, re)
		}
		// The engine stores the message JSON-quoted ("boom"), as before.
		if !strings.Contains(ev.Err.Error(), "boom") || strings.Contains(ev.Err.Error(), "attempt") || !strings.Contains(re.Describe(), `step "a" attempt 2: `) {
			t.Fatalf("RunEvent.Err = %v (%s), want the step's text, described with step and attempt", ev.Err, re.Describe())
		}
		break
	}
	page, err = c.Runs(ctx, workflow.RunQuery{Ref: failed})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(failed) = %+v, %v", page, err)
	}
	if e := page.Runs[0].Error; !strings.Contains(e, "boom") || strings.Contains(e, "attempt") {
		t.Fatalf("RunInfo.Error = %q, want the step's own message", e)
	}
}

// TestResultLive proves CapResult: Result and RunInfo.Output of a
// one-step run are the step's output, of a two-sink DAG the JSON object
// workflow.RunOutput builds; a running run's Result is ErrWrongState; and
// an enqueue-only client that never registered the workflow reads the
// same result from the run's shape.
func TestResultLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfp5res_" + uuid.NewString()[:8]
	release := make(chan struct{})
	one := workflow.Workflow{Name: prefix + "_one", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
		if string(in) == `"hold"` {
			<-release
		}
		return []byte(`{"echo":` + string(in) + `}`), nil
	}}}}
	out := func(v string) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) { return []byte(v), nil }
	}
	dag := workflow.Workflow{Name: prefix + "_dag", Steps: []workflow.Step{
		{Name: "root", Run: out(`1`)},
		{Name: "left", After: []string{"root"}, Run: out(`"l"`)},
		{Name: "right", After: []string{"root"}, Run: out(`"r"`)},
	}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, one, dag)
	ctx := context.Background()

	held, err := c.Enqueue(ctx, workflow.Job{Workflow: one.Name, Payload: []byte(`"hold"`)})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, held, workflow.StatusRunning)
	if _, err := c.Result(ctx, held); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("Result of a running run: %v, want ErrWrongState", err)
	}
	close(release)
	got, err := workflow.AwaitResult(ctx, c, held, workflow.AwaitOptions{})
	if err != nil || string(got) != `{"echo":"hold"}` {
		t.Fatalf("AwaitResult = %s, %v", got, err)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: held})
	if err != nil || len(page.Runs) != 1 || string(page.Runs[0].Output) != `{"echo":"hold"}` {
		t.Fatalf("RunInfo.Output = %+v, %v", page.Runs, err)
	}

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: dag.Name})
	if err != nil {
		t.Fatal(err)
	}
	got, err = workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]string
	if err := json.Unmarshal(got, &obj); err != nil || obj["left"] != "l" || obj["right"] != "r" || len(obj) != 2 {
		t.Fatalf("DAG result = %s (%v), want left and right", got, err)
	}
	web := hatchet.New(hatchet.Config{EnqueueOnly: true})
	if err := web.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// Without the definition the keys come in name order.
	again, err := web.Result(ctx, ref)
	if err != nil || string(again) != `{"left":"l","right":"r"}` {
		t.Fatalf("enqueue-only Result = %s, %v; want left and right in name order", again, err)
	}
	if _, err := c.Result(ctx, workflow.RunRef{RunID: workflow.RunID(uuid.NewString())}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Result of an unknown run: %v, want ErrNotFound", err)
	}
}

// TestRescheduleLive proves workflow.RunRescheduler and CapDueQuery: a
// delayed start moved later keeps its RunRef and lists with the new
// RunInfo.RunAt in a DueFrom/DueTo window that excludes the old time; a
// start moved minutes earlier fires at the new time and Status finds the
// fired run under the same ref (the v7-token search); a start due within
// the guard and a fired run are ErrWrongState; a cancelled one ErrNotFound.
func TestRescheduleLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfp5rs_" + uuid.NewString()[:8]
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()
	now := time.Now()

	far := now.Add(2 * time.Hour).Truncate(time.Millisecond)
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, RunAt: now.Add(time.Hour), Payload: []byte(`1`), Labels: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Cancel(context.Background(), ref) })
	moved, err := c.Reschedule(ctx, ref, far)
	if err != nil || moved != ref {
		t.Fatalf("Reschedule later = %+v, %v; want the same ref", moved, err)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, DueFrom: far.Add(-time.Minute), DueTo: far.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Runs) != 1 || page.Runs[0].Ref != ref || !page.Runs[0].RunAt.Equal(far) || page.Runs[0].Labels["k"] != "v" || string(page.Runs[0].Payload) != "1" {
		t.Fatalf("due query at the new time = %+v, want %+v at %s", page.Runs, ref, far)
	}
	page, err = c.Runs(ctx, workflow.RunQuery{Workflow: wf.Name, DueFrom: now.Add(50 * time.Minute), DueTo: now.Add(70 * time.Minute)})
	if err != nil || len(page.Runs) != 0 {
		t.Fatalf("due query at the old time = %+v, %v; want none", page.Runs, err)
	}

	// Moved from an hour away to 12s away: it fires then, long before
	// the RunAt its ScheduledID still names, and is inside the guard.
	soon, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, RunAt: now.Add(time.Hour), Payload: []byte(`2`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reschedule(ctx, soon, time.Now().Add(12*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reschedule(ctx, soon, time.Now().Add(time.Hour)); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("Reschedule within the guard: %v, want ErrWrongState", err)
	}
	waitStatus(t, c, soon, workflow.StatusSucceeded)
	page, err = c.Runs(ctx, workflow.RunQuery{Ref: soon})
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Ref.RunID.IsZero() || page.Runs[0].RunAt.After(time.Now()) {
		t.Fatalf("fired run = %+v, %v", page.Runs, err)
	}
	if _, err := c.Reschedule(ctx, soon, time.Now().Add(time.Hour)); !errors.Is(err, workflow.ErrWrongState) {
		t.Fatalf("Reschedule of a fired run: %v, want ErrWrongState", err)
	}

	gone, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, RunAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(ctx, gone); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reschedule(ctx, gone, far); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Reschedule of a cancelled run: %v, want ErrNotFound", err)
	}
}

// TestCronSecondsTickLive proves CapCronSeconds and the cron half of
// CapTickTime: a six-field schedule (Schedule.Seconds) fires on the
// engine every two seconds, each run's StepInfo.TickAt is a whole even
// second at or before the step start, Schedules lists it with Seconds,
// and a manual TriggerSchedule run carries the trigger time as its tick;
// both carry the schedule's Tenant (Schedule.Tenant), which Schedules
// lists.
func TestCronSecondsTickLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfp5sec_" + uuid.NewString()[:8]
	type fire struct {
		tick, start time.Time
		ok          bool
		tenant      string
	}
	fires := make(chan fire, 64)
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		tick, ok := info.TickAt()
		fires <- fire{tick, time.Now(), ok, info.Tenant}
		return nil, nil
	}}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()
	if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Cron: "* * * * *", Seconds: true, Location: time.UTC}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("five fields with Seconds: %v, want ErrInvalid", err)
	}
	id, err := c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Name: "every2s", Cron: "*/2 * * * * *", Seconds: true, Location: time.UTC, Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Unschedule(context.Background(), id) })
	var ticks []time.Time
	deadline := time.After(40 * time.Second)
	for len(ticks) < 3 {
		select {
		case f := <-fires:
			if !f.ok || f.tick.Nanosecond() != 0 || f.tick.Second()%2 != 0 || f.tick.After(f.start) || f.tenant != "acme" {
				t.Fatalf("fire tick %s (ok %v) started %s: want a whole even second before the start", f.tick, f.ok, f.start)
			}
			ticks = append(ticks, f.tick)
		case <-deadline:
			t.Fatalf("only %d fires of a two-second cron in 40s", len(ticks))
		}
	}
	for i := 1; i < len(ticks); i++ {
		if d := ticks[i].Sub(ticks[i-1]); d <= 0 || d%(2*time.Second) != 0 {
			t.Fatalf("ticks %v are not two-second steps", ticks)
		}
	}
	if info := scheduleOf(t, c, wf.Name, id); !info.Seconds || info.Next.IsZero() || info.Tenant != "acme" {
		t.Fatalf("listed schedule = %+v, want Seconds and a Next", info)
	}
	if err := c.Unschedule(ctx, id); err != nil {
		t.Fatal(err)
	}

	manual, err := c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Name: "yearly", Cron: "0 0 3 1 1 *", Seconds: true, Location: time.UTC, Tenant: "globex"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Unschedule(context.Background(), manual) })
	drain(fires)
	before := time.Now()
	ref, err := c.TriggerSchedule(ctx, manual)
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	for f := range fires {
		if f.tenant == "acme" {
			continue // a fire of the two-second schedule still in flight
		}
		if !f.ok || f.tick.Before(before.Add(-time.Second)) || f.tick.After(f.start) || f.tenant != "globex" {
			t.Fatalf("manual tick %s (ok %v), want the trigger time after %s", f.tick, f.ok, before)
		}
		break
	}
}

// drain empties ch without blocking.
func drain[T any](ch chan T) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// TestHealthPayloadOutcomeLive proves Health (ready once started, not
// after the stop, live until a recycle), the payload cap (a JSON object
// at Config.PayloadLimit runs end to end, one byte more is refused before
// the engine with ErrPayloadTooLarge) and the enqueue outcome flags (a
// repeated idempotency key is reported a duplicate).
func TestHealthPayloadOutcomeLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfp5hp_" + uuid.NewString()[:8]
	var mu sync.Mutex
	var gotLen int
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
		mu.Lock()
		gotLen = len(in)
		mu.Unlock()
		return nil, nil
	}}}}
	c := hatchet.New(hatchet.Config{WorkerName: prefix})
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if rep := c.Health(ctx); !rep.Live || rep.Ready {
		t.Fatalf("Health before Start = %+v, want live, not ready", rep)
	}
	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	if err := c.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	if rep := c.Health(ctx); !rep.Live || !rep.Ready || len(rep.Checks) != 3 {
		t.Fatalf("Health after Start = %+v, want live and ready with client, engine and workers checks", rep)
	}

	limit := c.PayloadLimit()
	payload := jsonObjectOfSize(limit)
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: payload})
	if err != nil {
		t.Fatalf("payload at the limit (%d bytes): %v", limit, err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	mu.Lock()
	if gotLen != limit {
		t.Fatalf("step got %d bytes, want %d", gotLen, limit)
	}
	mu.Unlock()
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: jsonObjectOfSize(limit + 1)})
	var tooLarge *workflow.PayloadTooLargeError
	if !errors.As(err, &tooLarge) || !errors.Is(err, workflow.ErrPayloadTooLarge) || tooLarge.Limit != limit {
		t.Fatalf("payload over the limit: %v, want a PayloadTooLargeError at %d", err, limit)
	}
	delayed, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: payload, RunAt: time.Now().Add(2 * time.Second)})
	if err != nil {
		t.Fatalf("delayed payload at the limit: %v", err)
	}
	waitStatus(t, c, delayed, workflow.StatusSucceeded)

	key := prefix + "-key"
	octx, first := workflow.WithEnqueueOutcome(ctx)
	a, err := c.Enqueue(octx, workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
	if err != nil || !first.Reported || first.Duplicate {
		t.Fatalf("first enqueue: %v, outcome %+v; want reported, not duplicate", err, *first)
	}
	octx, second := workflow.WithEnqueueOutcome(ctx)
	b, err := c.Enqueue(octx, workflow.Job{Workflow: wf.Name, IdempotencyKey: key})
	if err != nil || !second.Duplicate || b.RunID != a.RunID {
		t.Fatalf("repeated key: %+v, %v, outcome %+v; want the first run, duplicate", b, err, *second)
	}

	stop()
	<-c.Done()
	if rep := c.Health(ctx); !rep.Live || rep.Ready {
		t.Fatalf("Health after stop = %+v, want live, not ready", rep)
	}
}

// jsonObjectOfSize returns a JSON object of exactly n bytes (n >= 8).
func jsonObjectOfSize(n int) []byte {
	const head, tail = `{"x":"`, `"}`
	return []byte(head + strings.Repeat("a", n-len(head)-len(tail)) + tail)
}

// TestKeyFIFOLive proves CapKeyFIFO beyond the conformance case: 40 runs
// over two keys, enqueued back to back while each key's first run holds
// its slot, start in enqueue order within each key.
func TestKeyFIFOLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfp5fifo_" + uuid.NewString()[:8]
	release := make(chan struct{})
	var mu sync.Mutex
	order := map[string][]int{}
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		var job struct {
			K string
			N int
		}
		if err := json.Unmarshal(in, &job); err != nil {
			return nil, workflow.Permanent(err)
		}
		mu.Lock()
		order[job.K] = append(order[job.K], job.N)
		mu.Unlock()
		if job.N == 0 {
			<-release
		}
		return nil, nil
	}}}}
	wf.Concurrency = &workflow.Concurrency{Key: workflow.KeyExpr(`input.K`), MaxRuns: 1, FIFO: true}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix, Slots: 4}, wf)
	ctx := context.Background()
	const perKey = 20
	var refs []workflow.RunRef
	for n := range perKey {
		for _, k := range []string{"x", "y"} {
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`{"K":"` + k + `","N":` + strconv.Itoa(n) + `}`)})
			if err != nil {
				t.Fatal(err)
			}
			refs = append(refs, ref)
		}
	}
	time.Sleep(2 * time.Second)
	close(release)
	for _, ref := range refs {
		waitStatus(t, c, ref, workflow.StatusSucceeded)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, k := range []string{"x", "y"} {
		got := order[k]
		if len(got) != perKey {
			t.Fatalf("key %s ran %d runs, want %d", k, len(got), perKey)
		}
		for i, n := range got {
			if n != i {
				t.Fatalf("key %s start order = %v, want enqueue order", k, got)
			}
		}
	}
}
