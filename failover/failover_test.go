package failover_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/memory"
)

var start = time.Date(2026, 1, 1, 10, 0, 30, 0, time.UTC)

// errUnavailable stands for a transport failure: a core sentinel it is
// not, so DefaultClassifier fails over on it.
var errUnavailable = errors.New("connection refused")

// switchable is a memory driver whose Enqueue fails as unavailable while
// down is set, and which counts the enqueues it was asked for. downErr,
// set before Start, replaces errUnavailable as the error while down.
type switchable struct {
	*memory.Client
	down    atomic.Bool
	calls   atomic.Int32
	downErr error
}

func (s *switchable) Enqueue(ctx context.Context, j workflow.Job) (workflow.RunRef, error) {
	s.calls.Add(1)
	if s.down.Load() {
		if s.downErr != nil {
			return workflow.RunRef{}, s.downErr
		}
		return workflow.RunRef{}, errUnavailable
	}
	return s.Client.Enqueue(ctx, j)
}

// noCaps hides capabilities of a memory driver: it reports caps minus
// hidden, which is all failover reads.
type noCaps struct {
	*memory.Client
	hidden workflow.Caps
}

func (n noCaps) Capabilities() workflow.Caps { return n.Client.Capabilities() &^ n.hidden }

// bare is a client that does not implement RunNotifier.
type bare struct{ workflow.Client }

type fixture struct {
	t    *testing.T
	clk  *clock.Fake
	p, s *switchable
	c    *failover.Client
	ctx  context.Context
}

func newFixture(t *testing.T, opts ...failover.Option) *fixture {
	t.Helper()
	clk := clock.NewFake(start)
	fx := &fixture{
		t:   t,
		clk: clk,
		p:   &switchable{Client: memory.New(memory.WithClock(clk))},
		s:   &switchable{Client: memory.New(memory.WithClock(clk))},
		ctx: t.Context(),
	}
	fx.c = failover.New(fx.p, fx.s, opts...)
	return fx
}

func (fx *fixture) up(wfs ...workflow.Workflow) {
	fx.t.Helper()
	for _, wf := range wfs {
		if err := fx.c.Register(wf); err != nil {
			fx.t.Fatalf("Register(%s): %v", wf.Name, err)
		}
	}
	if err := fx.c.Start(fx.ctx); err != nil {
		fx.t.Fatalf("Start: %v", err)
	}
}

func (fx *fixture) enqueue(key string) workflow.RunRef {
	fx.t.Helper()
	ref, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "wait", IdempotencyKey: key})
	if err != nil {
		fx.t.Fatalf("Enqueue(%s): %v", key, err)
	}
	return ref
}

// waitStatus polls until ref reaches want or fails the test after 5s.
func (fx *fixture) waitStatus(ref workflow.RunRef, want workflow.Status) {
	fx.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := fx.c.Status(fx.ctx, ref)
		if err == nil && st == want {
			return
		}
		if time.Now().After(deadline) {
			fx.t.Fatalf("Status(%+v) = %q, %v; want %q", ref, st, err, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitWF suspends on an event until Signal or cancellation, so its runs
// stay unfinished while a test routes calls to them.
var waitWF = workflow.Workflow{Name: "wait", Steps: []workflow.Step{{Name: "w", Wait: &workflow.WaitSpec{Event: "go"}}}}

func TestPrimaryDownRunsOnSecondary(t *testing.T) {
	fx := newFixture(t)
	ran := make(chan workflow.StepInfo, 1)
	fx.up(workflow.Workflow{Name: "job", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		ran <- info
		return nil, nil
	}}}})
	fx.p.down.Store(true)

	ref, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "job", IdempotencyKey: "k1"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !strings.HasPrefix(string(ref.RunID), "secondary:") {
		t.Fatalf("RunID = %q, want a secondary: prefix", ref.RunID)
	}
	fx.waitStatus(ref, workflow.StatusSucceeded)
	if info := <-ran; info.RunID != ref.RunID {
		t.Errorf("StepInfo.RunID = %q, want the ref's %q", info.RunID, ref.RunID)
	}
	if fx.p.calls.Load() != 1 || fx.s.calls.Load() != 1 {
		t.Errorf("enqueue calls primary=%d secondary=%d, want 1 and 1", fx.p.calls.Load(), fx.s.calls.Load())
	}
}

func TestPrimaryUpKeepsTheJob(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	ref := fx.enqueue("k1")
	if !strings.HasPrefix(string(ref.RunID), "primary:") {
		t.Fatalf("RunID = %q, want a primary: prefix", ref.RunID)
	}
	if fx.s.calls.Load() != 0 {
		t.Errorf("secondary got %d enqueues, want 0", fx.s.calls.Load())
	}
}

func TestJobWithoutKeyIsRefused(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	_, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "wait"})
	if !errors.Is(err, failover.ErrKeyRequired) || !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Enqueue without key: %v, want ErrKeyRequired and ErrInvalid", err)
	}
	if n := fx.p.calls.Load() + fx.s.calls.Load(); n != 0 {
		t.Errorf("a refused job reached a backend %d times", n)
	}

	fx2 := newFixture(t, failover.AllowWithoutKey())
	fx2.up(waitWF)
	if _, err := fx2.c.Enqueue(fx2.ctx, workflow.Job{Workflow: "wait"}); err != nil {
		t.Fatalf("Enqueue without key under AllowWithoutKey: %v", err)
	}
}

func TestCallerFaultsDoNotFailOver(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)

	// An unknown workflow is ErrNotFound on any backend.
	if _, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "nope", IdempotencyKey: "k"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("unknown workflow: %v, want ErrNotFound", err)
	}
	// A cancelled caller ctx is the caller's choice, whatever the primary
	// answered.
	fx.p.down.Store(true)
	ctx, cancel := context.WithCancel(fx.ctx)
	cancel()
	if _, err := fx.c.Enqueue(ctx, workflow.Job{Workflow: "wait", IdempotencyKey: "k"}); !errors.Is(err, errUnavailable) {
		t.Fatalf("cancelled ctx: %v, want the primary's error", err)
	}
	if fx.s.calls.Load() != 0 {
		t.Errorf("secondary got %d enqueues, want 0", fx.s.calls.Load())
	}
}

func TestBothDownNamesBoth(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	fx.p.down.Store(true)
	fx.s.down.Store(true)
	_, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "wait", IdempotencyKey: "k"})
	if err == nil || !strings.Contains(err.Error(), "primary") || !strings.Contains(err.Error(), "secondary") {
		t.Fatalf("both down: %v, want an error naming both", err)
	}
}

func TestClassifierDecides(t *testing.T) {
	fx := newFixture(t, failover.WithClassifier(func(error) bool { return false }))
	fx.up(waitWF)
	fx.p.down.Store(true)
	if _, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "wait", IdempotencyKey: "k"}); !errors.Is(err, errUnavailable) {
		t.Fatalf("classifier says no: %v, want the primary's error", err)
	}
	if fx.s.calls.Load() != 0 {
		t.Errorf("secondary got %d enqueues, want 0", fx.s.calls.Load())
	}
}

// TestRoutingByRef puts one run on each backend. Both memory drivers number
// their runs the same way, so the driver IDs collide; only the prefix tells
// Status and Cancel which run is meant.
func TestRoutingByRef(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	onP := fx.enqueue("p")
	fx.p.down.Store(true)
	onS := fx.enqueue("s")
	fx.p.down.Store(false)

	pid := strings.TrimPrefix(string(onP.RunID), "primary:")
	sid := strings.TrimPrefix(string(onS.RunID), "secondary:")
	if pid != sid {
		t.Fatalf("driver IDs %q and %q differ; the test needs them to collide", pid, sid)
	}
	fx.waitStatus(onP, workflow.StatusWaiting)
	fx.waitStatus(onS, workflow.StatusWaiting)

	if err := fx.c.Cancel(fx.ctx, onS); err != nil {
		t.Fatalf("Cancel(secondary run): %v", err)
	}
	fx.waitStatus(onS, workflow.StatusCancelled)
	fx.waitStatus(onP, workflow.StatusWaiting)

	if err := fx.c.Cancel(fx.ctx, onP); err != nil {
		t.Fatalf("Cancel(primary run): %v", err)
	}
	fx.waitStatus(onP, workflow.StatusCancelled)
}

func TestUnprefixedRefFallsBackToSecondary(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	fx.p.down.Store(true)
	ref := fx.enqueue("k")
	raw := workflow.RunRef{RunID: workflow.RunID(strings.TrimPrefix(string(ref.RunID), "secondary:"))}
	fx.waitStatus(raw, workflow.StatusWaiting) // the primary does not know it

	if _, err := fx.c.Status(fx.ctx, workflow.RunRef{RunID: "primary:a", ScheduledID: "secondary:b"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("ref naming two backends: %v, want ErrNotFound", err)
	}
	if _, err := fx.c.Status(fx.ctx, workflow.RunRef{RunID: "nope"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("unknown unprefixed ref: %v, want ErrNotFound", err)
	}
}

func TestSignalReachesBothBackends(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	onP := fx.enqueue("p")
	fx.p.down.Store(true)
	onS := fx.enqueue("s")
	fx.waitStatus(onP, workflow.StatusWaiting)
	fx.waitStatus(onS, workflow.StatusWaiting)
	if err := fx.c.Signal(fx.ctx, workflow.Event{Name: "go"}); err != nil {
		t.Fatalf("Signal: %v", err)
	}
	fx.waitStatus(onP, workflow.StatusSucceeded)
	fx.waitStatus(onS, workflow.StatusSucceeded)
}

func TestDelayedRefRoutes(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	fx.p.down.Store(true)
	ref, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "wait", IdempotencyKey: "k", RunAt: start.Add(time.Hour)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !strings.HasPrefix(string(ref.ScheduledID), "secondary:") {
		t.Fatalf("ScheduledID = %q, want a secondary: prefix", ref.ScheduledID)
	}
	fx.waitStatus(ref, workflow.StatusScheduled)
	if err := fx.c.Cancel(fx.ctx, ref); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	fx.waitStatus(ref, workflow.StatusCancelled)
}

func TestCapabilitiesAreTheIntersection(t *testing.T) {
	clk := clock.NewFake(start)
	p := memory.New(memory.WithClock(clk))
	s := noCaps{Client: memory.New(memory.WithClock(clk)), hidden: workflow.CapWaitForEvent | workflow.CapCron}
	c := failover.New(p, s)
	caps := c.Capabilities()
	if caps.Has(workflow.CapWaitForEvent) {
		t.Errorf("caps %s claim WaitForEvent the secondary lacks", caps)
	}
	if !caps.Has(workflow.CapCron) {
		t.Errorf("caps %s lack Cron; schedules live on the primary only", caps)
	}
	if both := failover.New(p, s, failover.WithSchedulesOnBoth()).Capabilities(); both.Has(workflow.CapCron) {
		t.Errorf("WithSchedulesOnBoth caps %s claim Cron the secondary lacks", both)
	}

	if err := c.Register(waitWF); err != nil {
		t.Fatalf("Register: %v", err)
	}
	err := c.Start(t.Context())
	if !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Start with a wait step the secondary cannot run: %v, want ErrUnsupported", err)
	}
	// Nothing started: the primary still accepts Register.
	if err := p.Register(waitWF); err != nil {
		t.Errorf("primary after a refused Start: %v, want it unstarted", err)
	}
}

func TestJobNeedingMissingCapIsRefused(t *testing.T) {
	clk := clock.NewFake(start)
	p := memory.New(memory.WithClock(clk))
	s := noCaps{Client: memory.New(memory.WithClock(clk)), hidden: workflow.CapDelayedStart}
	c := failover.New(p, s)
	if err := c.Register(waitWF); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, err := c.Enqueue(t.Context(), workflow.Job{Workflow: "wait", IdempotencyKey: "k", RunAt: start.Add(time.Hour)})
	var ue *workflow.UnsupportedError
	if !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapDelayedStart) {
		t.Fatalf("RunAt the secondary cannot honour: %v, want UnsupportedError missing DelayedStart", err)
	}
}

func TestSchedulesLiveOnThePrimary(t *testing.T) {
	for _, tc := range []struct {
		name         string
		opts         []failover.Option
		onSecondary  bool
		wantIDPrefix string
	}{
		{"primary only", nil, false, "primary:"},
		{"both", []failover.Option{failover.WithSchedulesOnBoth()}, true, "both:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t, tc.opts...)
			var mu sync.Mutex
			fired := map[string]int{}
			count := func(ctx context.Context, _ []byte) ([]byte, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				mu.Lock()
				fired[strings.SplitN(string(info.RunID), ":", 2)[0]]++
				mu.Unlock()
				return nil, nil
			}
			fx.up(workflow.Workflow{Name: "tick", Cron: []string{"* * * * *"}, Steps: []workflow.Step{{Name: "a", Run: count}}})
			id, err := fx.c.Schedule(fx.ctx, workflow.Schedule{Name: "s", Workflow: "tick", Cron: "* * * * *", Location: time.UTC})
			if err != nil {
				t.Fatalf("Schedule: %v", err)
			}
			if !strings.HasPrefix(string(id), tc.wantIDPrefix) {
				t.Errorf("ScheduleID = %q, want prefix %q", id, tc.wantIDPrefix)
			}
			// Declared cron and Schedule each fire once per backend that
			// holds them; each arms one timer first.
			want := map[string]int{"primary": 2}
			if tc.onSecondary {
				want["secondary"] = 2
			}
			bctx, cancel := context.WithTimeout(fx.ctx, 5*time.Second)
			defer cancel()
			if err := fx.clk.BlockUntil(bctx, want["primary"]+want["secondary"]); err != nil {
				t.Fatalf("cron timers armed: %d, want %d", fx.clk.Pending(), want["primary"]+want["secondary"])
			}
			fx.clk.Advance(time.Minute)
			deadline := time.Now().Add(5 * time.Second)
			for {
				mu.Lock()
				got := map[string]int{"primary": fired["primary"], "secondary": fired["secondary"]}
				mu.Unlock()
				if got["primary"] == want["primary"] && got["secondary"] == want["secondary"] {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("fires %v, want %v", got, want)
				}
				time.Sleep(time.Millisecond)
			}
			if err := fx.c.Unschedule(fx.ctx, id); err != nil {
				t.Fatalf("Unschedule: %v", err)
			}
			if err := fx.c.Unschedule(fx.ctx, ""); err != nil {
				t.Fatalf("Unschedule empty: %v", err)
			}
		})
	}
}

func TestRunFinishedFromBothBackends(t *testing.T) {
	fx := newFixture(t)
	evs := make(chan workflow.RunEvent, 4)
	if err := fx.c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { evs <- ev }); err != nil {
		t.Fatalf("OnRunFinished: %v", err)
	}
	fx.up(workflow.Workflow{Name: "job", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}})
	onP, _ := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "job", IdempotencyKey: "p"})
	fx.p.down.Store(true)
	onS, _ := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "job", IdempotencyKey: "s"})
	got := map[workflow.RunID]bool{}
	for range 2 {
		select {
		case ev := <-evs:
			got[ev.Ref.RunID] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("events so far %v", got)
		}
	}
	if !got[onP.RunID] || !got[onS.RunID] {
		t.Errorf("events for %v, want %s and %s", got, onP.RunID, onS.RunID)
	}

	clk := clock.NewFake(start)
	c := failover.New(memory.New(memory.WithClock(clk)), bare{memory.New(memory.WithClock(clk))})
	if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) {}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("OnRunFinished with a secondary that cannot notify: %v, want ErrUnsupported", err)
	}
	if c.Capabilities().Has(workflow.CapRunFinished) {
		t.Errorf("caps claim RunFinished the secondary lacks")
	}
}

func TestCoolOffSkipsThePrimary(t *testing.T) {
	clk := clock.NewFake(start)
	p := &switchable{Client: memory.New(memory.WithClock(clk))}
	s := &switchable{Client: memory.New(memory.WithClock(clk))}
	c := failover.New(p, s, failover.WithCoolOff(2, time.Minute), failover.WithClock(clk))
	if err := c.Register(waitWF); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	enq := func(key string) workflow.RunRef {
		t.Helper()
		ref, err := c.Enqueue(t.Context(), workflow.Job{Workflow: "wait", IdempotencyKey: key})
		if err != nil {
			t.Fatalf("Enqueue(%s): %v", key, err)
		}
		return ref
	}
	p.down.Store(true)
	enq("a")
	enq("b") // second failure: cool-off starts
	enq("c")
	if n := p.calls.Load(); n != 2 {
		t.Fatalf("primary tried %d times, want 2 (the third enqueue cools off)", n)
	}
	p.down.Store(false)
	clk.Advance(time.Minute)
	if ref := enq("d"); !strings.HasPrefix(string(ref.RunID), "primary:") {
		t.Fatalf("after the cool-off: %q, want the primary", ref.RunID)
	}
	p.down.Store(true)
	enq("e") // the success ended the streak: one failure does not cool off
	enq("f")
	if n := p.calls.Load(); n != 5 {
		t.Fatalf("primary tried %d times, want 5", n)
	}
}

func TestLifecycle(t *testing.T) {
	fx := newFixture(t)
	if _, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "wait", IdempotencyKey: "k"}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Errorf("Enqueue before Start: %v, want ErrNotStarted", err)
	}
	if fx.s.calls.Load() != 0 {
		t.Errorf("ErrNotStarted failed over")
	}
	fx.up(waitWF)
	if err := fx.c.Register(waitWF); !errors.Is(err, workflow.ErrAlreadyStarted) {
		t.Errorf("Register after Start: %v", err)
	}
	if err := fx.c.Start(fx.ctx); !errors.Is(err, workflow.ErrAlreadyStarted) {
		t.Errorf("second Start: %v", err)
	}
	if err := fx.c.Register(workflow.Workflow{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("invalid workflow: %v", err)
	}
}
