package workflowtest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
)

func startedFake(t *testing.T, opts []FakeOption, wfs ...workflow.Workflow) *Fake {
	t.Helper()
	f := NewFake(opts...)
	for _, wf := range wfs {
		if err := f.Register(wf); err != nil {
			t.Fatalf("Register(%s): %v", wf.Name, err)
		}
	}
	if err := f.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return f
}

func TestFakeRecordsAndAsserts(t *testing.T) {
	ctx := context.Background()
	f := startedFake(t, nil, oneStep("email", noopHandler), oneStep("sms", noopHandler))
	runAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	jobs := []workflow.Job{
		{Workflow: "email", Payload: []byte(`{"to":"a@x","n":1}`), Labels: map[string]string{"tenant": "t1"}, IdempotencyKey: "k1"},
		{Workflow: "email", Payload: []byte(`{"n":2,"to":"b@x"}`), RunAt: runAt, Priority: 3},
	}
	for _, j := range jobs {
		if _, err := f.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	jobs[0].Labels["tenant"] = "changed" // recorded copies must not move

	f.AssertEnqueued(t, "email")
	f.AssertEnqueued(t, "email", HasLabel("tenant", "t1"), HasKey("k1"))
	f.AssertEnqueued(t, "email", HasPayloadJSON(map[string]any{"to": "b@x", "n": 2}), HasRunAt(runAt), HasPriority(3))
	f.AssertEnqueued(t, "email", HasPayload([]byte(`{"to":"a@x","n":1}`)))
	f.AssertEnqueuedTimes(t, "email", 2)
	f.AssertEnqueuedTimes(t, "email", 1, Where("first", func(j workflow.Job) bool { return strings.Contains(string(j.Payload), "a@x") }))
	f.AssertNotEnqueued(t, "sms")
	f.AssertNotEnqueued(t, "email", HasLabel("tenant", "t2"))

	spy := &tbSpy{TB: t}
	f.AssertEnqueued(spy, "sms")
	f.AssertEnqueuedTimes(spy, "email", 3)
	f.AssertNotEnqueued(spy, "email", HasKey("k1"))
	f.AssertNothingEnqueued(spy)
	if len(spy.errs) != 4 {
		t.Fatalf("failing assertions reported %d errors, want 4: %q", len(spy.errs), spy.errs)
	}
	for _, msg := range spy.errs {
		if !strings.Contains(msg, "2 enqueued:") || !strings.Contains(msg, `tenant:t1`) {
			t.Errorf("failure message does not list what was recorded: %s", msg)
		}
	}
	if !strings.Contains(spy.errs[2], `idempotency key "k1"`) {
		t.Errorf("failure message does not name the matcher: %s", spy.errs[2])
	}

	empty := startedFake(t, nil, oneStep("email", noopHandler))
	empty.AssertNothingEnqueued(t)
	spy = &tbSpy{TB: t}
	empty.AssertEnqueued(spy, "email")
	if len(spy.errs) != 1 || !strings.Contains(spy.errs[0], "nothing was enqueued") {
		t.Errorf("empty fake message: %q", spy.errs)
	}
}

func TestFakeRefusesLikeADriver(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	if err := f.Register(workflow.Workflow{Name: "bad"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("invalid workflow: %v, want ErrInvalid", err)
	}
	cronWF := oneStep("tick", noopHandler)
	cronWF.Cron = []string{"not a cron"}
	if err := f.Register(cronWF); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("bad cron: %v, want ErrInvalid", err)
	}
	req := oneStep("req", noopHandler)
	req.Idempotency = workflow.IdempotencyRequired
	if err := f.Register(req); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Enqueue(ctx, workflow.Job{Workflow: "req", IdempotencyKey: "k"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Errorf("Enqueue before Start: %v, want ErrNotStarted", err)
	}
	if err := f.Signal(ctx, workflow.Event{Name: "e"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Errorf("Signal before Start: %v, want ErrNotStarted", err)
	}
	if err := f.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(ctx); !errors.Is(err, workflow.ErrAlreadyStarted) {
		t.Errorf("second Start: %v", err)
	}
	if err := f.Register(oneStep("late", noopHandler)); !errors.Is(err, workflow.ErrAlreadyStarted) {
		t.Errorf("Register after Start: %v", err)
	}
	if _, err := f.Enqueue(ctx, workflow.Job{Workflow: "nope"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("unknown workflow: %v, want ErrNotFound", err)
	}
	if _, err := f.Enqueue(ctx, workflow.Job{Workflow: "req"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("IdempotencyRequired without key: %v, want ErrInvalid", err)
	}
	f.AssertNothingEnqueued(t)

	// Capabilities: a job or workflow the real driver cannot honour.
	limited := NewFake(FakeCaps(workflow.CapCancel))
	if err := limited.Register(oneStep("plain", noopHandler)); err != nil {
		t.Fatal(err)
	}
	wait := workflow.Workflow{Name: "wait", Steps: []workflow.Step{{Name: "w", Wait: &workflow.WaitSpec{Event: "e"}}}}
	if err := limited.Register(wait); err != nil {
		t.Fatal(err)
	}
	if err := limited.Start(ctx); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Start with a wait step and no CapWaitForEvent: %v, want ErrUnsupported", err)
	}
	limited = startedFake(t, []FakeOption{FakeCaps(workflow.CapCancel)}, oneStep("plain", noopHandler))
	for name, j := range map[string]workflow.Job{
		"RunAt":          {Workflow: "plain", RunAt: time.Now()},
		"IdempotencyKey": {Workflow: "plain", IdempotencyKey: "k"},
	} {
		if _, err := limited.Enqueue(ctx, j); !errors.Is(err, workflow.ErrUnsupported) {
			t.Errorf("%s without its capability: %v, want ErrUnsupported", name, err)
		}
	}
	if _, err := limited.Schedule(ctx, workflow.Schedule{Workflow: "plain", Cron: "* * * * *", Location: time.UTC}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Schedule without CapCron: %v", err)
	}
	if err := NewFake(FakeCaps(0)).Cancel(ctx, workflow.RunRef{RunID: "x"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("Cancel without CapCancel: %v", err)
	}

	// Expression keys: refused unless FakeKeyExpr, as on the memory driver.
	exprWF := oneStep("expr", noopHandler)
	exprWF.Concurrency = &workflow.Concurrency{Key: workflow.KeyExpr("input.tenant"), MaxRuns: 1}
	f2 := NewFake()
	_ = f2.Register(exprWF)
	if err := f2.Start(ctx); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("KeyExpr without FakeKeyExpr: %v, want ErrUnsupported", err)
	}
	startedFake(t, []FakeOption{FakeKeyExpr()}, exprWF)

	// FakeLike copies a real client's capabilities, minus RunFinished.
	if got := NewFake(FakeLike(NewFake(FakeCaps(workflow.CapCron | workflow.CapRunFinished)))).Capabilities(); got != workflow.CapCron {
		t.Errorf("FakeLike caps = %s, want Cron", got)
	}
}

func TestFakeIdempotencyStatusAndCancel(t *testing.T) {
	ctx := context.Background()
	off := oneStep("off", noopHandler)
	off.Idempotency = workflow.IdempotencyOff
	f := startedFake(t, nil, oneStep("job", noopHandler), off)

	a, _ := f.Enqueue(ctx, workflow.Job{Workflow: "job", IdempotencyKey: "k"})
	b, _ := f.Enqueue(ctx, workflow.Job{Workflow: "job", IdempotencyKey: "k"})
	if a != b {
		t.Errorf("same key while unfinished: refs %v and %v, want the same", a, b)
	}
	f.AssertEnqueuedTimes(t, "job", 1)
	f.Enqueue(ctx, workflow.Job{Workflow: "off", IdempotencyKey: "k"})
	f.Enqueue(ctx, workflow.Job{Workflow: "off", IdempotencyKey: "k"})
	f.AssertEnqueuedTimes(t, "off", 2)

	if st, _ := f.Status(ctx, a); st != workflow.StatusQueued {
		t.Errorf("Status = %q, want queued", st)
	}
	if err := f.Finish(a, workflow.StatusRunning); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Finish with a non-terminal status: %v", err)
	}
	if err := f.Finish(a, workflow.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	c, _ := f.Enqueue(ctx, workflow.Job{Workflow: "job", IdempotencyKey: "k"})
	if c == a {
		t.Errorf("key not released after Finish")
	}
	f.AssertEnqueuedTimes(t, "job", 2)

	d, _ := f.Enqueue(ctx, workflow.Job{Workflow: "job", RunAt: time.Now().Add(time.Hour)})
	if d.ScheduledID.IsZero() || !d.RunID.IsZero() {
		t.Errorf("RunAt ref = %+v, want a ScheduledID only", d)
	}
	if st, _ := f.Status(ctx, d); st != workflow.StatusScheduled {
		t.Errorf("delayed Status = %q, want scheduled", st)
	}
	if err := f.Cancel(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := f.Cancel(ctx, a); err != nil { // finished: no-op
		t.Fatal(err)
	}
	if st, _ := f.Status(ctx, d); st != workflow.StatusCancelled {
		t.Errorf("after Cancel = %q", st)
	}
	if st, _ := f.Status(ctx, a); st != workflow.StatusSucceeded {
		t.Errorf("Cancel changed a finished run to %q", st)
	}
	f.AssertCancelled(t, d)
	if _, err := f.Status(ctx, workflow.RunRef{RunID: "nope"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("unknown Status: %v", err)
	}
	if err := f.Cancel(ctx, workflow.RunRef{RunID: "nope"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("unknown Cancel: %v", err)
	}
	spy := &tbSpy{TB: t}
	f.AssertCancelled(spy, c)
	if len(spy.errs) != 1 {
		t.Errorf("AssertCancelled of an uncancelled run passed")
	}
}

func TestFakeSchedulesAndSignals(t *testing.T) {
	ctx := context.Background()
	f := startedFake(t, nil, oneStep("tick", noopHandler))
	if _, err := f.Schedule(ctx, workflow.Schedule{Workflow: "tick", Cron: "* * * * *"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("Schedule without Location: %v, want ErrInvalid", err)
	}
	if _, err := f.Schedule(ctx, workflow.Schedule{Workflow: "nope", Cron: "* * * * *", Location: time.UTC}); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("Schedule of an unknown workflow: %v", err)
	}
	id, err := f.Schedule(ctx, workflow.Schedule{Name: "n", Workflow: "tick", Cron: "*/5 * * * *", Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	f.AssertScheduled(t, "tick")
	if err := f.Unschedule(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := f.Unschedule(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err := f.Unschedule(ctx, "nope"); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("unknown Unschedule: %v", err)
	}
	spy := &tbSpy{TB: t}
	f.AssertScheduled(spy, "tick")
	if len(spy.errs) != 1 || !strings.Contains(spy.errs[0], "unscheduled: true") {
		t.Errorf("AssertScheduled after Unschedule: %q", spy.errs)
	}

	if err := f.Signal(ctx, workflow.Event{Name: "paid", Key: "o1"}); err != nil {
		t.Fatal(err)
	}
	f.AssertSignalled(t, "paid", "o1")
	spy = &tbSpy{TB: t}
	f.AssertSignalled(spy, "paid", "o2")
	if len(spy.errs) != 1 || !strings.Contains(spy.errs[0], "paid/o1") {
		t.Errorf("AssertSignalled message: %q", spy.errs)
	}
}

func TestFakeConcurrent(t *testing.T) {
	ctx := context.Background()
	f := startedFake(t, nil, oneStep("job", noopHandler))
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.Enqueue(ctx, workflow.Job{Workflow: "job"}); err != nil {
				t.Error(err)
			}
			f.AssertEnqueued(t, "job")
			_ = f.Signal(ctx, workflow.Event{Name: "e"})
		}()
	}
	wg.Wait()
	f.AssertEnqueuedTimes(t, "job", 50)
	if n := len(f.Signals()); n != 50 {
		t.Errorf("signals = %d, want 50", n)
	}
}

func TestFakeSeams(t *testing.T) {
	ctx := context.Background()
	zoned := workflow.Schedule{Workflow: "w", Cron: "0 9 * * *", Location: time.FixedZone("UTC+5", 5*3600)}

	// The default Fake accepts what the memory driver accepts.
	def := startedFake(t, nil, oneStep("w", noopHandler))
	if _, err := def.Schedule(ctx, zoned); err != nil {
		t.Fatalf("default Fake refused a zoned schedule: %v", err)
	}

	// A Fake shaped like a UTC-only engine refuses the zone and a misfire
	// policy, as that engine would.
	utc := startedFake(t, []FakeOption{FakeCaps(workflow.CapCron)}, oneStep("w", noopHandler))
	var ue *workflow.UnsupportedError
	if _, err := utc.Schedule(ctx, zoned); !errors.As(err, &ue) || ue.Field != "Schedule.Location" {
		t.Fatalf("UTC-only Fake: zoned schedule err = %v, want Schedule.Location refused", err)
	}
	skip := workflow.Schedule{Workflow: "w", Cron: "0 9 * * *", Location: time.UTC, Misfire: workflow.Misfire{Policy: workflow.MisfireSkip}}
	if _, err := utc.Schedule(ctx, skip); !errors.As(err, &ue) || ue.Field != "Schedule.Misfire" {
		t.Fatalf("UTC-only Fake: misfire err = %v, want Schedule.Misfire refused", err)
	}

	// FakeCronParser matches a driver built with a custom parser, and a
	// parser that returns nothing fails closed.
	alias := func(expr string) (cron.Schedule, error) {
		if expr == "every-hour" {
			return cron.Parse("0 * * * *")
		}
		return nil, errors.New("only every-hour")
	}
	custom := startedFake(t, []FakeOption{FakeCronParser(alias)}, oneStep("w", noopHandler))
	if _, err := custom.Schedule(ctx, workflow.Schedule{Workflow: "w", Cron: "every-hour", Location: time.UTC}); err != nil {
		t.Fatalf("custom parser Fake refused its own dialect: %v", err)
	}
	if _, err := custom.Schedule(ctx, workflow.Schedule{Workflow: "w", Cron: "0 * * * *", Location: time.UTC}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("custom parser Fake: %v, want ErrInvalid", err)
	}
	broken := NewFake(FakeCronParser(func(string) (cron.Schedule, error) { return nil, nil }))
	wf := oneStep("c", noopHandler)
	wf.Cron = []string{"0 * * * *"}
	if err := broken.Register(wf); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a parser returning (nil, nil): %v, want ErrInvalid", err)
	}
}
