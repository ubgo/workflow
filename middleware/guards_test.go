package middleware_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	_ "time/tzdata" // the zones the quiet-hours cases use, whatever the host has

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

var guardStart = time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)

// capture is an enqueue func that records the job and whether the
// driver's label check would pass under the ctx it got.
type capture struct {
	job      workflow.Job
	labelErr error
	calls    int
}

func (c *capture) next(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	c.calls++
	c.job = job
	c.labelErr = workflow.CheckJobLabels(ctx, job)
	return workflow.RunRef{RunID: "r1"}, nil
}

func guardStepCtx(info workflow.StepInfo) context.Context {
	return workflow.WithStepInfo(context.Background(), info)
}

// TestExpireAfter pins the stamped expiry: d after now or after a later
// RunAt, an ExpireAt label kept, a bad one refused, and the label let
// through the driver's reserved-label check (which refuses it without
// the allowance).
func TestExpireAfter(t *testing.T) {
	clk := clock.NewFake(guardStart)
	mw := middleware.ExpireAfter(time.Hour, clk)
	cases := []struct {
		name string
		job  workflow.Job
		want string
		err  error
	}{
		{"now", workflow.Job{Workflow: "w"}, "2026-03-02T13:00:00Z", nil},
		{"delayed", workflow.Job{Workflow: "w", RunAt: guardStart.Add(24 * time.Hour)}, "2026-03-03T13:00:00Z", nil},
		{"past RunAt", workflow.Job{Workflow: "w", RunAt: guardStart.Add(-time.Hour)}, "2026-03-02T13:00:00Z", nil},
		{"explicit", middleware.ExpireAt(workflow.Job{Workflow: "w"}, time.Date(2026, 3, 2, 12, 5, 0, 0, time.FixedZone("x", 3600))), "2026-03-02T11:05:00Z", nil},
		{"bad label", workflow.Job{Workflow: "w", Labels: map[string]string{middleware.LabelExpiresAt: "tomorrow"}}, "", workflow.ErrInvalid},
	}
	for _, tc := range cases {
		var c capture
		_, err := mw(c.next)(context.Background(), tc.job)
		if !errors.Is(err, tc.err) || (tc.err != nil && c.calls != 0) {
			t.Fatalf("%s: err %v calls %d, want %v", tc.name, err, c.calls, tc.err)
		}
		if tc.err != nil {
			continue
		}
		if got := c.job.Labels[middleware.LabelExpiresAt]; got != tc.want {
			t.Errorf("%s: expiry %q, want %q", tc.name, got, tc.want)
		}
		if c.labelErr != nil {
			t.Errorf("%s: driver would refuse the label: %v", tc.name, c.labelErr)
		}
	}
	// Without ExpireAfter the driver refuses the reserved label.
	job := middleware.ExpireAt(workflow.Job{Workflow: "w"}, guardStart)
	if err := workflow.CheckJobLabels(context.Background(), job); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("label passed without the allowance: %v", err)
	}
	// A d of zero stamps nothing and leaves the job alone.
	var c capture
	if _, err := middleware.ExpireAfter(0, clk)(c.next)(context.Background(), workflow.Job{Workflow: "w"}); err != nil || c.job.Labels != nil {
		t.Fatalf("d=0 changed the job: %+v, %v", c.job.Labels, err)
	}
	// ExpireAt never changes the caller's map, and a zero time removes it.
	labels := map[string]string{"a": "b"}
	j := middleware.ExpireAt(workflow.Job{Labels: labels}, guardStart)
	if len(labels) != 1 || middleware.ExpireAt(j, time.Time{}).Labels[middleware.LabelExpiresAt] != "" {
		t.Fatal("ExpireAt mutated the caller's map or kept a zero expiry")
	}
}

// TestExpire pins the step decision: a first attempt after the expiry is
// discarded (or failed), a retry is not, ExpireMaxWait expires a long
// queue wait, and ExpireSteps narrows the check.
func TestExpire(t *testing.T) {
	clk := clock.NewFake(guardStart)
	expiry := map[string]string{middleware.LabelExpiresAt: guardStart.Add(-time.Second).Format(time.RFC3339Nano)}
	future := map[string]string{middleware.LabelExpiresAt: guardStart.Add(time.Second).Format(time.RFC3339Nano)}
	type want int
	const (
		ran want = iota
		discarded
		failed
		invalid
	)
	cases := []struct {
		name string
		opts []middleware.ExpireOption
		info *workflow.StepInfo
		want want
	}{
		{"no info", nil, nil, ran},
		{"no expiry", nil, &workflow.StepInfo{Step: "s", Attempt: 1}, ran},
		{"in time", nil, &workflow.StepInfo{Step: "s", Attempt: 1, Labels: future}, ran},
		{"late", nil, &workflow.StepInfo{Step: "s", Attempt: 1, Labels: expiry}, discarded},
		{"late fail", []middleware.ExpireOption{middleware.ExpireFail()}, &workflow.StepInfo{Step: "s", Attempt: 1, Labels: expiry}, failed},
		{"late retry", nil, &workflow.StepInfo{Step: "s", Attempt: 2, Labels: expiry}, ran},
		{"other step", []middleware.ExpireOption{middleware.ExpireSteps("first")}, &workflow.StepInfo{Step: "s", Attempt: 1, Labels: expiry}, ran},
		{"named step", []middleware.ExpireOption{middleware.ExpireSteps("first")}, &workflow.StepInfo{Step: "first", Attempt: 1, Labels: expiry}, discarded},
		{"bad label", nil, &workflow.StepInfo{Step: "s", Attempt: 1, Labels: map[string]string{middleware.LabelExpiresAt: "x"}}, invalid},
		{"waited long", []middleware.ExpireOption{middleware.ExpireMaxWait(time.Minute)}, &workflow.StepInfo{Step: "s", Attempt: 3, ReadyAt: guardStart.Add(-2 * time.Minute)}, discarded},
		{"waited exactly", []middleware.ExpireOption{middleware.ExpireMaxWait(time.Minute)}, &workflow.StepInfo{Step: "s", Attempt: 1, ReadyAt: guardStart.Add(-time.Minute)}, ran},
		{"ready unknown", []middleware.ExpireOption{middleware.ExpireMaxWait(time.Minute)}, &workflow.StepInfo{Step: "s", Attempt: 1}, ran},
	}
	for _, tc := range cases {
		calls := 0
		h := middleware.Expire(clk, tc.opts...)(func(context.Context, []byte) ([]byte, error) { calls++; return []byte("ok"), nil })
		ctx := context.Background()
		if tc.info != nil {
			ctx = guardStepCtx(*tc.info)
		}
		_, err := h(ctx, nil)
		var got want
		switch {
		case err == nil && calls == 1:
			got = ran
		case calls != 0:
			t.Fatalf("%s: step ran and failed: %v", tc.name, err)
		case workflow.IsDiscarded(err) && errors.Is(err, middleware.ErrExpired):
			got = discarded
		case workflow.IsPermanent(err) && errors.Is(err, middleware.ErrExpired):
			got = failed
		case workflow.IsPermanent(err) && errors.Is(err, workflow.ErrInvalid):
			got = invalid
		default:
			t.Fatalf("%s: unexpected %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: outcome %d, want %d (%v)", tc.name, got, tc.want, err)
		}
	}
}

// TestExpire_MemoryDiscardsLateRun: a delayed run whose start is past
// its expiry ends discarded on the memory driver without running.
func TestExpire_MemoryDiscardsLateRun(t *testing.T) {
	clk := clock.NewFake(guardStart)
	m := memory.New(memory.WithClock(clk), memory.WithMiddleware(middleware.Expire(clk)))
	var ran atomic.Int32
	if err := m.Register(workflow.Workflow{Name: "otp", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
		ran.Add(1)
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); m.Stop() }()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(m, middleware.ExpireAfter(0, clk))
	job := middleware.ExpireAt(workflow.Job{Workflow: "otp", RunAt: guardStart.Add(2 * time.Minute)}, guardStart.Add(time.Minute))
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(3 * time.Minute)
	st := awaitStatus(t, m, ref)
	if st != workflow.StatusDiscarded || ran.Load() != 0 {
		t.Fatalf("status %q, ran %d; want discarded without running", st, ran.Load())
	}
}

func awaitStatus(t *testing.T, c workflow.Client, ref workflow.RunRef) workflow.Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond})
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	return st
}

// TestBounded pins the gate: below the limit passes, at it refuses with
// ErrQueueFull (never ErrUnavailable), a zero limit skips the count,
// BoundedDelay delays instead, and a count error fails closed.
func TestBounded(t *testing.T) {
	clk := clock.NewFake(guardStart)
	depth := 0
	counted := 0
	df := func(context.Context, workflow.Job) (int, error) { counted++; return depth, nil }
	limits := middleware.QueueLimits(map[string]int{workflow.DefaultQueue: 3, "bulk": 0})

	var c capture
	refuse := middleware.Bounded(df, limits)(c.next)
	depth = 2
	if _, err := refuse(context.Background(), workflow.Job{Workflow: "w"}); err != nil || c.calls != 1 {
		t.Fatalf("below the limit: %v", err)
	}
	depth = 3
	_, err := refuse(context.Background(), workflow.Job{Workflow: "w"})
	if !errors.Is(err, middleware.ErrQueueFull) || workflow.IsUnavailable(err) || c.calls != 1 {
		t.Fatalf("at the limit: %v (calls %d)", err, c.calls)
	}
	counted = 0
	if _, err := refuse(context.Background(), workflow.Job{Workflow: "w", Queue: "bulk"}); err != nil || counted != 0 {
		t.Fatalf("unbounded queue: %v, counted %d", err, counted)
	}

	delay := middleware.Bounded(df, limits, middleware.BoundedDelay(time.Minute, clk))(c.next)
	if _, err := delay(context.Background(), workflow.Job{Workflow: "w"}); err != nil || !c.job.RunAt.Equal(guardStart.Add(time.Minute)) {
		t.Fatalf("soft limit: RunAt %s, %v", c.job.RunAt, err)
	}
	later := guardStart.Add(time.Hour)
	if _, err := delay(context.Background(), workflow.Job{Workflow: "w", RunAt: later}); err != nil || !c.job.RunAt.Equal(later) {
		t.Fatalf("soft limit moved a later RunAt to %s: %v", c.job.RunAt, err)
	}

	boom := errors.New("count failed")
	broken := middleware.Bounded(func(context.Context, workflow.Job) (int, error) { return 0, boom }, limits)(c.next)
	if _, err := broken(context.Background(), workflow.Job{Workflow: "w"}); !errors.Is(err, boom) {
		t.Fatalf("count error: %v", err)
	}
}

// TestBoundedWait: a full queue blocks until the count drops, polling on
// the clock, and gives up with ErrQueueFull and the ctx error.
func TestBoundedWait(t *testing.T) {
	clk := clock.NewFake(guardStart)
	var depth atomic.Int32
	depth.Store(5)
	df := func(context.Context, workflow.Job) (int, error) { return int(depth.Load()), nil }
	var c capture
	h := middleware.Bounded(df, func(workflow.Job) int { return 5 }, middleware.BoundedWait(time.Second, clk))(c.next)
	done := make(chan error, 1)
	go func() { _, err := h(context.Background(), workflow.Job{Workflow: "w"}); done <- err }()
	if err := blockUntil(clk); err != nil {
		t.Fatal(err)
	}
	depth.Store(4)
	clk.Advance(time.Second)
	if err := <-done; err != nil || c.calls != 1 {
		t.Fatalf("wait: %v, calls %d", err, c.calls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	depth.Store(5)
	go func() { _, err := h(ctx, workflow.Job{Workflow: "w"}); done <- err }()
	if err := blockUntil(clk); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, middleware.ErrQueueFull) || !errors.Is(err, context.Canceled) {
		t.Fatalf("give up: %v", err)
	}
}

// blockUntil waits, a bounded time, for the gate to park on the clock.
func blockUntil(clk *clock.Fake) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return clk.BlockUntil(ctx, 1)
}

// TestDepthAndQuota_Memory counts through the memory driver's run query:
// delayed runs fill the default queue's bound, and one tenant's quota
// leaves another tenant free.
func TestDepthAndQuota_Memory(t *testing.T) {
	clk := clock.NewFake(guardStart)
	m := memory.New(memory.WithClock(clk))
	if err := m.Register(workflow.Workflow{Name: "mail", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); m.Stop() }()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	depth, err := middleware.DepthFromRuns(m)
	if err != nil {
		t.Fatal(err)
	}
	tenants, err := middleware.TenantCount(m)
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(m,
		middleware.Quota(tenants, 2),
		middleware.Bounded(depth, middleware.QueueLimits(map[string]int{workflow.DefaultQueue: 3})))
	later := guardStart.Add(time.Hour)
	enqueue := func(tenant string) error {
		_, err := c.Enqueue(ctx, workflow.Job{Workflow: "mail", RunAt: later, Tenant: tenant})
		return err
	}
	for _, step := range []struct {
		tenant string
		want   error
	}{{"acme", nil}, {"acme", nil}, {"acme", middleware.ErrQuotaExceeded}, {"globex", nil}, {"globex", middleware.ErrQueueFull}} {
		if err := enqueue(step.tenant); !errors.Is(err, step.want) {
			t.Fatalf("enqueue for %s: %v, want %v", step.tenant, err, step.want)
		}
	}

	type noQuery struct{ workflow.Client }
	if _, err := middleware.DepthFromRuns(noQuery{&stubCaps{}}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("DepthFromRuns without CapRunQuery: %v", err)
	}
	if _, err := middleware.TenantCount(&stubCaps{caps: workflow.CapRunQuery}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("TenantCount without CapTenant: %v", err)
	}
}

type stubCaps struct {
	workflow.Client
	caps workflow.Caps
}

func (s *stubCaps) Capabilities() workflow.Caps { return s.caps }
func (s *stubCaps) Runs(context.Context, workflow.RunQuery) (workflow.RunPage, error) {
	return workflow.RunPage{}, nil
}

// TestQuietHours pins the move: a start inside quiet hours goes to the
// first allowed minute on the job's own zone, across a DST gap, and an
// allowed start is left untouched.
func TestQuietHours(t *testing.T) {
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	berlin, _ := time.LoadLocation("Europe/Berlin")
	zones := map[string]*time.Location{"tokyo": tokyo, "berlin": berlin}
	zoneOf := func(j workflow.Job) *time.Location { return zones[j.Labels["user"]] }
	night := workflow.Windows{Unless: []workflow.Window{{From: "21:00", To: "08:00"}}}
	at := func(loc *time.Location, s string) time.Time {
		tm, err := time.ParseInLocation("2006-01-02 15:04:05", s, loc)
		if err != nil {
			t.Fatal(err)
		}
		return tm
	}
	cases := []struct {
		name  string
		w     workflow.Windows
		now   time.Time
		user  string
		runAt time.Time
		want  time.Time // zero: RunAt left unset
	}{
		{"day in berlin", night, at(berlin, "2026-03-02 13:00:00"), "berlin", time.Time{}, time.Time{}},
		{"night in tokyo", night, at(berlin, "2026-03-02 13:00:00"), "tokyo", time.Time{}, at(tokyo, "2026-03-03 08:00:00")},
		{"utc fallback", night, at(time.UTC, "2026-03-02 22:30:10"), "", time.Time{}, at(time.UTC, "2026-03-03 08:00:00")},
		{"delayed into night", night, at(time.UTC, "2026-03-02 09:00:00"), "berlin", at(berlin, "2026-03-02 23:15:00"), at(berlin, "2026-03-03 08:00:00")},
		{"delayed in day", night, at(time.UTC, "2026-03-02 09:00:00"), "berlin", at(berlin, "2026-03-02 15:00:00"), at(berlin, "2026-03-02 15:00:00")},
		{"between only", workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "17:00"}}}, at(time.UTC, "2026-03-02 17:00:00"), "", time.Time{}, at(time.UTC, "2026-03-03 09:00:00")},
		// 2026-03-29: Berlin skips 02:00-03:00, so a quiet window ending at
		// 02:30 ends at the first minute that exists, 03:00.
		{"dst gap", workflow.Windows{Unless: []workflow.Window{{From: "01:00", To: "02:30"}}}, at(berlin, "2026-03-29 01:30:00"), "berlin", time.Time{}, at(berlin, "2026-03-29 03:00:00")},
	}
	for _, tc := range cases {
		clk := clock.NewFake(tc.now)
		var c capture
		job := workflow.Job{Workflow: "notify", RunAt: tc.runAt, Labels: map[string]string{"user": tc.user}}
		if _, err := middleware.QuietHours(tc.w, zoneOf, clk)(c.next)(context.Background(), job); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !c.job.RunAt.Equal(tc.want) {
			t.Errorf("%s: RunAt %s, want %s", tc.name, c.job.RunAt, tc.want)
		}
	}
	for _, w := range []workflow.Windows{
		{Unless: []workflow.Window{{From: "9:00", To: "17:00"}}},
		{Unless: []workflow.Window{{From: "09:00", To: "09:00"}}},
		{Unless: []workflow.Window{{From: "00:00", To: "12:00"}, {From: "12:00", To: "00:00"}}},
	} {
		var c capture
		if _, err := middleware.QuietHours(w, nil, clock.NewFake(guardStart))(c.next)(context.Background(), workflow.Job{Workflow: "n"}); !errors.Is(err, workflow.ErrInvalid) || c.calls != 0 {
			t.Errorf("windows %+v: %v, want ErrInvalid", w, err)
		}
	}
}

type order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func (o *order) Validate() error {
	if o.Amount <= 0 {
		return fmt.Errorf("amount %d is not positive", o.Amount)
	}
	return nil
}

type plainPayload struct {
	N int `json:"n"`
}

// TestValidatePayload: a payload must decode and pass Validate, chained
// jobs are checked too, other workflows pass, and nothing invalid reaches
// the driver.
func TestValidatePayload(t *testing.T) {
	mw := middleware.ChainEnqueue(middleware.ValidateJSON[order]("charge"), middleware.ValidatePayload(map[string]func([]byte) error{"count": middleware.JSONRule[plainPayload]()}))
	good, _ := json.Marshal(order{ID: "o-1", Amount: 5})
	bad, _ := json.Marshal(order{ID: "o-2", Amount: 0})
	cases := []struct {
		name string
		job  workflow.Job
		ok   bool
	}{
		{"valid", workflow.Job{Workflow: "charge", Payload: good}, true},
		{"rule breach", workflow.Job{Workflow: "charge", Payload: bad}, false},
		{"not json", workflow.Job{Workflow: "charge", Payload: []byte("{")}, false},
		{"empty", workflow.Job{Workflow: "charge"}, false},
		{"no Validate method", workflow.Job{Workflow: "count", Payload: []byte(`{"n":-1}`)}, true},
		{"no rule", workflow.Job{Workflow: "other", Payload: []byte("anything")}, true},
		{"bad chained job", workflow.Job{Workflow: "other", Chain: []workflow.Job{{Workflow: "charge", Payload: bad}}}, false},
	}
	for _, tc := range cases {
		var c capture
		_, err := mw(c.next)(context.Background(), tc.job)
		if tc.ok != (err == nil) || tc.ok != (c.calls == 1) {
			t.Errorf("%s: err %v calls %d", tc.name, err, c.calls)
		}
		if !tc.ok && !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v does not match ErrInvalid", tc.name, err)
		}
	}
}

// TestCheck: pre and post breaches become permanent ErrConstraint errors
// and stop the step or drop its output; step errors pass as they are.
func TestCheck(t *testing.T) {
	pre := func(_ context.Context, in []byte) error {
		if len(in) == 0 {
			return errors.New("empty input")
		}
		return nil
	}
	post := func(_ context.Context, in, out []byte) error {
		if string(out) == "bad" {
			return errors.New("output not allowed")
		}
		return nil
	}
	stepErr := errors.New("upstream 503")
	ran := 0
	h := middleware.Check(pre, post)(func(_ context.Context, in []byte) ([]byte, error) {
		ran++
		switch string(in) {
		case "fail":
			return nil, stepErr
		case "inv":
			return nil, workflow.Invariant(false, "stock below zero")
		}
		return in, nil
	})
	if out, err := h(context.Background(), []byte("good")); err != nil || string(out) != "good" {
		t.Fatalf("good: %q %v", out, err)
	}
	if _, err := h(context.Background(), nil); !errors.Is(err, workflow.ErrConstraint) || !workflow.IsPermanent(err) || ran != 1 {
		t.Fatalf("pre breach: %v, ran %d", err, ran)
	}
	if out, err := h(context.Background(), []byte("bad")); !errors.Is(err, workflow.ErrConstraint) || !workflow.IsPermanent(err) || out != nil {
		t.Fatalf("post breach: %q %v", out, err)
	}
	if _, err := h(context.Background(), []byte("fail")); !errors.Is(err, stepErr) || workflow.IsPermanent(err) {
		t.Fatalf("step error changed: %v", err)
	}
	if _, err := h(context.Background(), []byte("inv")); err.Error() != "workflow: constraint violated: stock below zero" {
		t.Fatalf("invariant rewrapped: %v", err)
	}
	if out, err := middleware.Check(nil, nil)(func(_ context.Context, in []byte) ([]byte, error) { return in, nil })(context.Background(), nil); err != nil || out != nil {
		t.Fatalf("nil conditions: %q %v", out, err)
	}
}

// failingUnique is a UniqueStore whose Acquire fails.
type failingUnique struct{ middleware.UniqueStore }

func (failingUnique) Acquire(context.Context, string, string, time.Duration) (bool, error) {
	return false, errors.New("store down")
}

// TestPool_Unit pins the slot gate: a busy pool snoozes (or fails with a
// RetryAfter), the slot is freed after the step whatever it returned,
// PoolSteps narrows it, and bad inputs or store errors never run the step.
func TestPool_Unit(t *testing.T) {
	store := middleware.NewMemoryUnique(nil)
	ctx := guardStepCtx(workflow.StepInfo{Workflow: "w", Step: "render", RunID: "r1", Attempt: 1})
	ran := 0
	step := func(context.Context, []byte) ([]byte, error) { ran++; return nil, errors.New("render failed") }
	pool := middleware.Pool(store, "gpu", 1, time.Minute, middleware.PoolRetry(250*time.Millisecond))

	// Hold the only slot, as another worker would.
	if ok, _ := store.Acquire(context.Background(), "pool/gpu/0", "other", time.Minute); !ok {
		t.Fatal("setup acquire")
	}
	_, err := pool(step)(ctx, nil)
	if d, ok := workflow.SnoozeDelay(err); !ok || d != 250*time.Millisecond || ran != 0 {
		t.Fatalf("busy pool: %v (snooze %s %v), ran %d", err, d, ok, ran)
	}
	_, err = middleware.Pool(store, "gpu", 1, time.Minute, middleware.PoolRetryAfter())(step)(ctx, nil)
	if d, ok := workflow.RetryAfterDelay(err, 1); !errors.Is(err, middleware.ErrPoolBusy) || !ok || d != middleware.DefaultPoolRetry {
		t.Fatalf("busy pool with RetryAfter: %v", err)
	}
	if _, err := middleware.Pool(store, "gpu", 1, time.Minute, middleware.PoolSteps("other-step"))(step)(ctx, nil); ran != 1 || err == nil {
		t.Fatalf("PoolSteps did not let an unnamed step through: ran %d", ran)
	}
	_ = store.Release(context.Background(), "pool/gpu/0", "other")

	// A free slot runs the step and is free again after it, even on failure.
	if _, err := pool(step)(ctx, nil); err == nil || ran != 2 {
		t.Fatalf("free pool: %v, ran %d", err, ran)
	}
	if ok, _ := store.Acquire(context.Background(), "pool/gpu/0", "after", time.Minute); !ok {
		t.Fatal("the slot was not released after the step")
	}

	for name, mw := range map[string]workflow.Middleware{
		"no name":   middleware.Pool(store, "", 1, time.Minute),
		"no slots":  middleware.Pool(store, "p", 0, time.Minute),
		"no ttl":    middleware.Pool(store, "p", 1, 0),
		"no store":  middleware.Pool(nil, "p", 1, time.Minute),
		"cs no ttl": middleware.CriticalSection(store, "s", 0),
	} {
		if _, err := mw(step)(ctx, nil); !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := middleware.Pool(failingUnique{}, "p", 1, time.Minute)(step)(ctx, nil); err == nil || workflow.IsPermanent(err) || ran != 2 {
		t.Fatalf("store error: %v, ran %d", err, ran)
	}
	if _, err := middleware.CriticalSection(store, "s", time.Minute)(step)(context.Background(), nil); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("critical section without StepInfo: %v", err)
	}
}

// TestCriticalSection_Unit: the section is per run, so the same section
// in another run is free while this run holds it.
func TestCriticalSection_Unit(t *testing.T) {
	store := middleware.NewMemoryUnique(nil)
	cs := middleware.CriticalSection(store, "ledger", time.Minute, "debit", "credit")
	inside := make(chan struct{})
	release := make(chan struct{})
	hold := cs(func(context.Context, []byte) ([]byte, error) { close(inside); <-release; return nil, nil })
	done := make(chan error, 1)
	go func() {
		_, err := hold(guardStepCtx(workflow.StepInfo{Step: "debit", RunID: "run-1", Attempt: 1}), nil)
		done <- err
	}()
	<-inside
	quick := cs(func(context.Context, []byte) ([]byte, error) { return []byte("ran"), nil })
	if _, err := quick(guardStepCtx(workflow.StepInfo{Step: "credit", RunID: "run-1", Attempt: 1}), nil); !errors.Is(err, workflow.ErrSnoozed) {
		t.Fatalf("same run, same section: %v, want a snooze", err)
	}
	if out, err := quick(guardStepCtx(workflow.StepInfo{Step: "credit", RunID: "run-2", Attempt: 1}), nil); err != nil || string(out) != "ran" {
		t.Fatalf("other run: %q %v", out, err)
	}
	if out, err := quick(guardStepCtx(workflow.StepInfo{Step: "notify", RunID: "run-1", Attempt: 1}), nil); err != nil || string(out) != "ran" {
		t.Fatalf("step outside the section: %q %v", out, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// RunPoolOnMemory runs n runs of one step under pool on the memory driver
// and returns the most steps that ran at once. It is shared with the
// durable-store test in sqlstore/enginetest through a copy, not an import.
func runPoolOnMemory(t *testing.T, pool workflow.Middleware, n int) int32 {
	t.Helper()
	m := memory.New(memory.WithWorkers(n), memory.WithMiddleware(pool))
	var cur, peak atomic.Int32
	if err := m.Register(workflow.Workflow{Name: "render", Steps: []workflow.Step{{Name: "gpu", Run: func(context.Context, []byte) ([]byte, error) {
		now := cur.Add(1)
		for {
			p := peak.Load()
			if now <= p || peak.CompareAndSwap(p, now) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		cur.Add(-1)
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); m.Stop() }()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var refs []workflow.RunRef
	for range n {
		ref, err := m.Enqueue(ctx, workflow.Job{Workflow: "render"})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	for _, ref := range refs {
		if st := awaitStatus(t, m, ref); st != workflow.StatusSucceeded {
			t.Fatalf("run ended %q", st)
		}
	}
	return peak.Load()
}

// TestPool_Memory: eight runs on eight workers share a pool of two
// slots; at most two steps ever run at once and every run succeeds, the
// others snoozing until a slot frees.
func TestPool_Memory(t *testing.T) {
	pool := middleware.Pool(middleware.NewMemoryUnique(nil), "gpu", 2, time.Minute, middleware.PoolRetry(2*time.Millisecond))
	if peak := runPoolOnMemory(t, pool, 8); peak != 2 {
		t.Fatalf("peak concurrency %d, want exactly 2", peak)
	}
}

// TestCriticalSection_Memory: two parallel steps of one run, both in the
// section, never overlap on the memory driver.
func TestCriticalSection_Memory(t *testing.T) {
	cs := middleware.CriticalSection(middleware.NewMemoryUnique(nil), "ledger", time.Minute)
	m := memory.New(memory.WithWorkers(4), memory.WithMiddleware(middleware.Chain(cs)))
	var cur, peak atomic.Int32
	body := func(context.Context, []byte) ([]byte, error) {
		if n := cur.Add(1); n > peak.Load() {
			peak.Store(n)
		}
		time.Sleep(10 * time.Millisecond)
		cur.Add(-1)
		return nil, nil
	}
	if err := m.Register(workflow.Workflow{Name: "transfer", Steps: []workflow.Step{{Name: "debit", Run: body}, {Name: "credit", Run: body}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); m.Stop() }()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := m.Enqueue(ctx, workflow.Job{Workflow: "transfer"})
	if err != nil {
		t.Fatal(err)
	}
	if st := awaitStatus(t, m, ref); st != workflow.StatusSucceeded || peak.Load() != 1 {
		t.Fatalf("status %q, peak %d; want succeeded with the steps one at a time", st, peak.Load())
	}
}

// TestAutoDisable pins the switch: it trips after n consecutive
// failures, calls onDisable once, discards later attempts without
// running them until Enable, and a success resets the count.
func TestAutoDisable(t *testing.T) {
	store := middleware.NewMemoryDisable()
	var disabled []string
	var mu sync.Mutex
	key := func(_ workflow.StepInfo, in []byte) string { return strings.SplitN(string(in), "|", 2)[0] }
	ran := 0
	h := middleware.AutoDisable(store, key, 3, func(_ context.Context, k string) {
		mu.Lock()
		disabled = append(disabled, k)
		mu.Unlock()
	})(func(_ context.Context, in []byte) ([]byte, error) {
		ran++
		switch strings.SplitN(string(in), "|", 2)[1] {
		case "ok":
			return nil, nil
		case "snooze":
			return nil, workflow.Snooze(time.Second)
		}
		return nil, errors.New("endpoint returned 500")
	})
	ctx := guardStepCtx(workflow.StepInfo{Step: "deliver", Attempt: 1})
	send := func(in string) error { _, err := h(ctx, []byte(in)); return err }

	for _, in := range []string{"a|fail", "a|fail", "a|ok", "a|fail", "a|snooze", "a|fail"} {
		_ = send(in)
	}
	if len(disabled) != 0 {
		t.Fatalf("disabled after a reset: %v", disabled)
	}
	_ = send("a|fail") // third in a row
	_ = send("b|fail") // another key is counted apart
	if len(disabled) != 1 || disabled[0] != "a" {
		t.Fatalf("disabled %v, want [a]", disabled)
	}
	before := ran
	if err := send("a|ok"); !workflow.IsDiscarded(err) || !errors.Is(err, middleware.ErrEndpointDisabled) || ran != before {
		t.Fatalf("disabled key: %v, ran %d more", err, ran-before)
	}
	if err := send("b|ok"); err != nil {
		t.Fatalf("other key: %v", err)
	}
	if err := store.Enable(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if err := send("a|ok"); err != nil || len(disabled) != 1 {
		t.Fatalf("after Enable: %v, disabled %v", err, disabled)
	}
	// An empty key is never counted or switched.
	for range 5 {
		_ = send("|fail")
	}
	if off, _ := store.Disabled(context.Background(), ""); off {
		t.Fatal("an empty key was disabled")
	}
}

// TestAutoDisable_CountFinal: with DisableCountFinal only the failures
// the policy will not retry count.
func TestAutoDisable_CountFinal(t *testing.T) {
	store := middleware.NewMemoryDisable()
	trips := 0
	h := middleware.AutoDisable(store, func(workflow.StepInfo, []byte) string { return "hook" }, 2,
		func(context.Context, string) { trips++ },
		middleware.DisableCountFinal(workflow.RetryPolicy{Max: 2}),
	)(func(context.Context, []byte) ([]byte, error) { return nil, errors.New("timeout") })
	for attempt := 1; attempt <= 2; attempt++ { // retried attempts: not counted
		_, _ = h(guardStepCtx(workflow.StepInfo{Attempt: attempt}), nil)
	}
	if off, _ := store.Disabled(context.Background(), "hook"); off {
		t.Fatal("retried attempts tripped the switch")
	}
	_, _ = h(guardStepCtx(workflow.StepInfo{Attempt: 3}), nil) // final
	_, _ = h(guardStepCtx(workflow.StepInfo{Attempt: 3}), nil) // final again
	if trips != 1 {
		t.Fatalf("trips %d, want 1 after two final failures", trips)
	}
	if _, err := middleware.AutoDisable(store, nil, 1, nil)(nil)(context.Background(), nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil key: %v", err)
	}
}
