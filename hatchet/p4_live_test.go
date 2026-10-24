package hatchet_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// scheduleOf returns the listed schedule id of workflow wf.
func scheduleOf(t *testing.T, c *hatchet.Client, wf string, id workflow.ScheduleID) workflow.ScheduleInfo {
	t.Helper()
	list, err := c.Schedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range list {
		if info.Workflow == wf && info.ID == id {
			return info
		}
	}
	t.Fatalf("schedule %s of %s not listed in %+v", id, wf, list)
	return workflow.ScheduleInfo{}
}

// TestTriggerScheduleLive proves CapScheduleTrigger and the manual half
// of CapScheduleHistory on the engine: TriggerSchedule runs a named
// trigger's workflow now with the trigger's input and labels plus the
// triggered labels, on a trigger that will not fire for months, and
// Schedules then reports that run as Last with its end status; a
// declared trigger is triggered by its listed ID the same way; an
// unknown ID is ErrNotFound.
func TestTriggerScheduleLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wftrig_" + uuid.NewString()[:8]
	got := make(chan []byte, 4)
	wf := workflow.Workflow{Name: prefix, Cron: []string{"0 3 1 1 *"}, Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
		got <- in
		return in, nil
	}}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()
	id, err := c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Name: "yearly", Cron: "0 1 1 1 *", Location: time.UTC, Input: []byte(`{"n":7}`), Labels: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Unschedule(context.Background(), id) })
	if info := scheduleOf(t, c, wf.Name, id); !info.Last.Fire.IsZero() || !info.Last.Run.RunID.IsZero() {
		t.Fatalf("Last before any fire = %+v", info.Last)
	}

	before := time.Now()
	ref, err := c.TriggerSchedule(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	if in := <-got; string(in) != `{"n":7}` {
		t.Fatalf("triggered run got input %s", in)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(ref) = %+v, %v", page, err)
	}
	labels := page.Runs[0].Labels
	if labels["k"] != "v" || labels[workflow.LabelSchedule] != string(id) || labels[workflow.LabelScheduleName] != "yearly" || labels[workflow.LabelTrigger] != workflow.LabelTriggerManual {
		t.Fatalf("triggered run labels = %v", labels)
	}
	info := scheduleOf(t, c, wf.Name, id)
	if info.Last.Run.RunID != ref.RunID || info.Last.Status != workflow.StatusSucceeded {
		t.Fatalf("Last = %+v, want run %s succeeded", info.Last, ref.RunID)
	}
	if info.Last.Fire.Before(before.Add(-time.Second)) || info.Last.Fire.After(time.Now()) {
		t.Fatalf("Last.Fire = %s, want between %s and now", info.Last.Fire, before)
	}

	var declared workflow.ScheduleInfo
	list, err := c.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range list {
		if s.Workflow == wf.Name && s.Declared {
			declared = s
		}
	}
	if declared.ID.IsZero() {
		t.Fatalf("declared trigger not listed: %+v", list)
	}
	dref, err := c.TriggerSchedule(ctx, declared.ID)
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, dref, workflow.StatusSucceeded)
	if in := <-got; string(in) != "null" && len(in) != 0 {
		t.Fatalf("declared trigger run got input %s", in)
	}
	if info := scheduleOf(t, c, wf.Name, declared.ID); info.Last.Run.RunID != dref.RunID {
		t.Fatalf("declared Last = %+v, want run %s", info.Last, dref.RunID)
	}
	// The named trigger's Last is unchanged by the declared one's fire.
	if info := scheduleOf(t, c, wf.Name, id); info.Last.Run.RunID != ref.RunID {
		t.Fatalf("named Last moved to %+v", info.Last)
	}

	if _, err := c.TriggerSchedule(ctx, workflow.ScheduleID(uuid.NewString())); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("unknown trigger: %v, want ErrNotFound", err)
	}
}

// TestScheduleHistoryTickLive proves the cron half of CapScheduleHistory:
// after the engine fires an every-minute trigger, Schedules reports the
// tick's time and run. It waits up to two minutes, so it runs only with
// WFTEST_REAL_CRON=1.
func TestScheduleHistoryTickLive(t *testing.T) {
	skipWithoutEngine(t)
	if os.Getenv("WFTEST_REAL_CRON") == "" {
		t.Skip("WFTEST_REAL_CRON is not set: waits for a real cron tick")
	}
	prefix := "wfhist_" + uuid.NewString()[:8]
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: okStep}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()
	id, err := c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Name: "minutely", Cron: "* * * * *", Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Unschedule(context.Background(), id) })
	deadline := time.Now().Add(150 * time.Second)
	for time.Now().Before(deadline) {
		info := scheduleOf(t, c, wf.Name, id)
		if !info.Last.Run.RunID.IsZero() && info.Last.Status == workflow.StatusSucceeded {
			if info.Last.Fire.Second() != 0 || info.Last.Fire.Nanosecond() != 0 {
				t.Fatalf("Last.Fire = %s, want a minute boundary", info.Last.Fire)
			}
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("no tick reported as Last within 150s")
}

// TestEnqueueOnlyLive proves CapEnqueueOnly on the engine: a client with
// Config.EnqueueOnly, never started and with nothing registered, enqueues
// a run that a separate worker client runs, and reads its status; a
// workflow registered on it keeps its own checks.
func TestEnqueueOnlyLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfeo_" + uuid.NewString()[:8]
	ran := make(chan []byte, 1)
	wf := workflow.Workflow{Name: prefix, Idempotency: workflow.IdempotencyRequired, Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
		ran <- in
		return in, nil
	}}}}
	startLive(t, hatchet.Config{WorkerName: prefix}, wf)
	ctx := context.Background()

	web := hatchet.New(hatchet.Config{EnqueueOnly: true})
	ref, err := web.Enqueue(ctx, workflow.Job{Workflow: wf.Name, Payload: []byte(`{"x":1}`), IdempotencyKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, web, ref, workflow.StatusSucceeded)
	if in := <-ran; string(in) != `{"x":1}` {
		t.Fatalf("worker got %s", in)
	}
	checked := hatchet.New(hatchet.Config{EnqueueOnly: true})
	if err := checked.Register(wf); err != nil {
		t.Fatal(err)
	}
	if _, err := checked.Enqueue(ctx, workflow.Job{Workflow: wf.Name}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("registered IdempotencyRequired without a key: %v, want ErrInvalid", err)
	}
}

// TestRateLimitScopeLive proves a rate limit's buckets belong to its
// workflow: the engine keys dynamic buckets by the key's value per
// tenant, so before the driver scoped the expression, a run of workflow B
// waited for workflow A's bucket of the same value (seen as the
// conformance ratelimit case timing out when run back to back). With
// Limit 1 per minute on both, A's first run and B's run start at once,
// and A's second run of the same key is still held.
func TestRateLimitScopeLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfrl_" + uuid.NewString()[:8]
	started := make(chan string, 8)
	limited := func(name string) workflow.Workflow {
		return workflow.Workflow{
			Name:       name,
			RateLimits: []workflow.RateLimit{{Key: workflow.KeyExpr(`additional_metadata["k"]`), Limit: 1, Per: time.Minute}},
			Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
				started <- name + ":" + string(in)
				return in, nil
			}}},
		}
	}
	a, b := limited(prefix+"_a"), limited(prefix+"_b")
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix}, a, b)
	ctx := context.Background()
	key := map[string]string{"k": prefix}
	enqueue := func(wf, payload string) {
		t.Helper()
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: wf, Payload: []byte(payload), Labels: key}); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(want string, within time.Duration) {
		t.Helper()
		select {
		case got := <-started:
			if got != want {
				t.Fatalf("started %s, want %s", got, want)
			}
		case <-time.After(within):
			t.Fatalf("%s did not start within %s", want, within)
		}
	}
	enqueue(a.Name, `1`)
	expect(a.Name+":1", 30*time.Second)
	enqueue(b.Name, `1`)
	expect(b.Name+":1", 30*time.Second)
	enqueue(a.Name, `2`)
	select {
	case got := <-started:
		t.Fatalf("%s started inside A's window", got)
	case <-time.After(10 * time.Second):
	}
}
