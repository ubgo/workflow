package regional_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/regional"
	"github.com/ubgo/workflow/workflowtest"
)

var start = time.Date(2026, 1, 1, 10, 0, 30, 0, time.UTC)

// router sends a job to its "region" label, and to eu without one.
var router = regional.ByLabel("region", "eu")

// TestConformance runs the shared driver suite over a regional client of
// two memory regions on one fake clock. Jobs without a region label go to
// eu, so every Status, Cancel, Signal and run-finished event the suite
// checks goes through the prefixed refs.
func TestConformance(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		c, err := regional.New(map[string]workflow.Client{
			"eu": memory.New(memory.WithClock(clk)),
			"us": memory.New(memory.WithClock(clk)),
		}, router, regional.WithScheduleRegion("eu"))
		if err != nil {
			t.Fatal(err)
		}
		return workflowtest.Instance{Client: c, Clock: clk}
	}})
}

// TestConformanceOtherRegion runs the suite with every job routed to the
// region the declared crons do not fire in, so a run of a region other
// than the schedule region is reached through every call the suite makes.
func TestConformanceOtherRegion(t *testing.T) {
	workflowtest.Run(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(start)
		c, err := regional.New(map[string]workflow.Client{
			"eu": memory.New(memory.WithClock(clk)),
			"us": memory.New(memory.WithClock(clk)),
		}, regional.ByLabel("region", "us"), regional.WithScheduleRegion("eu"))
		if err != nil {
			t.Fatal(err)
		}
		return workflowtest.Instance{Client: c, Clock: clk}
	}})
}

// noCaps hides capabilities of a memory driver.
type noCaps struct {
	*memory.Client
	hidden workflow.Caps
}

func (n noCaps) Capabilities() workflow.Caps { return n.Client.Capabilities().Without(n.hidden) }

type fixture struct {
	t      *testing.T
	clk    *clock.Fake
	eu, us *memory.Client
	c      *regional.Client
	ctx    context.Context
}

func newFixture(t *testing.T, opts ...regional.Option) *fixture {
	t.Helper()
	clk := clock.NewFake(start)
	fx := &fixture{t: t, clk: clk, eu: memory.New(memory.WithClock(clk)), us: memory.New(memory.WithClock(clk)), ctx: t.Context()}
	c, err := regional.New(map[string]workflow.Client{"eu": fx.eu, "us": fx.us}, router, opts...)
	if err != nil {
		t.Fatal(err)
	}
	fx.c = c
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

func (fx *fixture) enqueue(region string) workflow.RunRef {
	fx.t.Helper()
	job := workflow.Job{Workflow: "wait"}
	if region != "" {
		job.Labels = map[string]string{"region": region}
	}
	ref, err := fx.c.Enqueue(fx.ctx, job)
	if err != nil {
		fx.t.Fatalf("Enqueue(%s): %v", region, err)
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

// waitWF suspends on an event until Signal or cancellation.
var waitWF = workflow.Workflow{Name: "wait", Steps: []workflow.Step{{Name: "w", Wait: &workflow.WaitSpec{Event: "go"}}}}

// inner strips the region prefix, giving the driver's own ref.
func inner(ref workflow.RunRef) workflow.RunRef {
	_, id, _ := strings.Cut(string(ref.RunID), ":")
	return workflow.RunRef{RunID: workflow.RunID(id)}
}

func TestRouting(t *testing.T) {
	fx := newFixture(t)
	ran := make(chan workflow.StepInfo, 1)
	fx.up(waitWF, workflow.Workflow{Name: "job", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		ran <- info
		return nil, nil
	}}}})

	us := fx.enqueue("us")
	eu := fx.enqueue("")
	if !strings.HasPrefix(string(us.RunID), "us:") || !strings.HasPrefix(string(eu.RunID), "eu:") {
		t.Fatalf("refs = %q, %q; want us: and eu: prefixes", us.RunID, eu.RunID)
	}
	// The run is in the region's own driver, and only there.
	if _, err := fx.us.Status(fx.ctx, inner(us)); err != nil {
		t.Fatalf("us driver: %v", err)
	}
	if _, err := fx.eu.Status(fx.ctx, inner(us)); !errors.Is(err, workflow.ErrNotFound) && inner(us) != inner(eu) {
		t.Fatalf("eu driver holds the us run: %v", err)
	}

	ref, err := fx.c.Enqueue(fx.ctx, workflow.Job{Workflow: "job", Labels: map[string]string{"region": "us"}})
	if err != nil {
		t.Fatal(err)
	}
	fx.waitStatus(ref, workflow.StatusSucceeded)
	if info := <-ran; info.RunID != ref.RunID {
		t.Errorf("StepInfo.RunID = %q, want the ref's %q", info.RunID, ref.RunID)
	}
}

func TestRefs_StatusCancelRuns(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	// Both memory drivers number runs alike, so a call that reached the
	// wrong region would act on the other region's run.
	eu := fx.enqueue("eu")
	us := fx.enqueue("us")
	fx.waitStatus(eu, workflow.StatusWaiting)
	fx.waitStatus(us, workflow.StatusWaiting)

	if err := fx.c.Cancel(fx.ctx, us); err != nil {
		t.Fatal(err)
	}
	fx.waitStatus(us, workflow.StatusCancelled)
	fx.waitStatus(eu, workflow.StatusWaiting)

	page, err := fx.c.Runs(fx.ctx, workflow.RunQuery{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[workflow.RunID]workflow.Status{}
	for _, r := range page.Runs {
		got[r.Ref.RunID] = r.Status
	}
	if len(got) != 2 || got[eu.RunID] != workflow.StatusWaiting || got[us.RunID] != workflow.StatusCancelled {
		t.Fatalf("Runs = %v", got)
	}

	one, err := fx.c.Runs(fx.ctx, workflow.RunQuery{Ref: us})
	if err != nil || len(one.Runs) != 1 || one.Runs[0].Ref != us {
		t.Fatalf("Runs(Ref us) = %+v, %v", one.Runs, err)
	}

	// Paging one run at a time returns every run once.
	seen := map[workflow.RunID]bool{}
	q := workflow.RunQuery{Limit: 1}
	for i := 0; ; i++ {
		p, err := fx.c.Runs(fx.ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range p.Runs {
			if seen[r.Ref.RunID] {
				t.Fatalf("run %s twice", r.Ref.RunID)
			}
			seen[r.Ref.RunID] = true
		}
		if p.Next == "" || i > 5 {
			break
		}
		q.Cursor = p.Next
	}
	if len(seen) != 2 {
		t.Fatalf("paged %d runs, want 2", len(seen))
	}
	if _, err := fx.c.Runs(fx.ctx, workflow.RunQuery{Cursor: "not-ours"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("foreign cursor: %v", err)
	}

	for _, bad := range []workflow.RunRef{inner(us), {RunID: "ap:" + inner(us).RunID}, {RunID: eu.RunID, ScheduledID: "us:x"}} {
		if _, err := fx.c.Status(fx.ctx, bad); !errors.Is(err, workflow.ErrNotFound) {
			t.Errorf("Status(%+v) = %v, want ErrNotFound", bad, err)
		}
	}
	none, err := fx.c.Runs(fx.ctx, workflow.RunQuery{Ref: inner(us)})
	if err != nil || len(none.Runs) != 0 {
		t.Fatalf("Runs(unprefixed ref) = %+v, %v; want none", none.Runs, err)
	}
}

func TestSignalReachesOtherRegion(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	us := fx.enqueue("us")
	fx.waitStatus(us, workflow.StatusWaiting)
	if err := fx.c.Signal(fx.ctx, workflow.Event{Name: "go"}); err != nil {
		t.Fatal(err)
	}
	fx.waitStatus(us, workflow.StatusSucceeded)
}

func TestSignalRouter(t *testing.T) {
	fx := newFixture(t, regional.WithSignalRouter(func(context.Context, workflow.Event) ([]string, error) {
		return []string{"eu"}, nil
	}))
	fx.up(waitWF)
	eu, us := fx.enqueue("eu"), fx.enqueue("us")
	fx.waitStatus(us, workflow.StatusWaiting)
	if err := fx.c.Signal(fx.ctx, workflow.Event{Name: "go"}); err != nil {
		t.Fatal(err)
	}
	fx.waitStatus(eu, workflow.StatusSucceeded)
	fx.clk.Advance(time.Minute)
	if st, _ := fx.c.Status(fx.ctx, us); st != workflow.StatusWaiting {
		t.Fatalf("us run = %q, want it still waiting: the router left us out", st)
	}

	bad := newFixture(t, regional.WithSignalRouter(func(context.Context, workflow.Event) ([]string, error) {
		return []string{"ap"}, nil
	}))
	bad.up(waitWF)
	if err := bad.c.Signal(bad.ctx, workflow.Event{Name: "go"}); !errors.Is(err, regional.ErrUnknownRegion) || !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("unknown signal region: %v", err)
	}
}

func TestUnknownRegionRefused(t *testing.T) {
	clk := clock.NewFake(start)
	regions := map[string]workflow.Client{"eu": memory.New(memory.WithClock(clk)), "us": memory.New(memory.WithClock(clk))}
	c, err := regional.New(regions, regional.ByLabel("region", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Register(waitWF); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, err = c.Enqueue(t.Context(), workflow.Job{Workflow: "wait", Labels: map[string]string{"region": "ap"}})
	if !errors.Is(err, regional.ErrUnknownRegion) || !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("unknown region: %v", err)
	}
	if _, err := c.Enqueue(t.Context(), workflow.Job{Workflow: "wait"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("no label, no fallback: %v", err)
	}

	for name, tc := range map[string]struct {
		regions map[string]workflow.Client
		router  regional.Router
		opts    []regional.Option
	}{
		"no regions":          {nil, router, nil},
		"nil router":          {regions, nil, nil},
		"nil client":          {map[string]workflow.Client{"eu": nil}, router, nil},
		"colon in name":       {map[string]workflow.Client{"e:u": memory.New()}, router, nil},
		"empty name":          {map[string]workflow.Client{"": memory.New()}, router, nil},
		"bad schedule region": {regions, router, []regional.Option{regional.WithScheduleRegion("ap")}},
	} {
		if _, err := regional.New(tc.regions, tc.router, tc.opts...); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
}

func TestCapabilitiesIntersection(t *testing.T) {
	clk := clock.NewFake(start)
	c, err := regional.New(map[string]workflow.Client{
		"eu": memory.New(memory.WithClock(clk)),
		"us": noCaps{Client: memory.New(memory.WithClock(clk)), hidden: workflow.CapsOf(workflow.CapRetryRun, workflow.CapTenant)},
	}, router)
	if err != nil {
		t.Fatal(err)
	}
	caps := c.Capabilities()
	full := memory.New().Capabilities()
	if caps.Has(workflow.CapRetryRun) || caps.Has(workflow.CapTenant) {
		t.Fatalf("caps %s include what us lacks", caps)
	}
	if caps.Has(workflow.CapClosure) || caps.Has(workflow.CapWorkerScale) {
		t.Fatalf("caps %s include closures or worker scaling", caps)
	}
	if want := full.Without(workflow.CapRetryRun, workflow.CapTenant, workflow.CapClosure, workflow.CapWorkerScale); caps != want {
		t.Fatalf("caps = %s, want %s", caps, want)
	}
	if err := c.Register(waitWF); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Retry(t.Context(), workflow.RunRef{RunID: "eu:1"}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("Retry without CapRetryRun in us: %v", err)
	}
}

func TestSchedules(t *testing.T) {
	fx := newFixture(t, regional.WithScheduleRegion("eu"))
	report := workflow.Workflow{Name: "report", Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
	nightly := report
	nightly.Name, nightly.Cron = "nightly", []string{"0 3 * * *"}
	fx.up(report, nightly)

	id, err := fx.c.Schedule(fx.ctx, workflow.Schedule{Name: "hourly", Workflow: "report", Cron: "0 * * * *", Location: time.UTC, Labels: map[string]string{"region": "us"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(id), "us:") {
		t.Fatalf("ScheduleID = %q, want a us: prefix", id)
	}
	usList, _ := fx.us.Schedules(fx.ctx)
	euList, _ := fx.eu.Schedules(fx.ctx)
	if len(usList) != 1 || usList[0].Name != "hourly" {
		t.Fatalf("us schedules = %+v, want the hourly schedule only (the declared cron fires in eu)", usList)
	}
	if len(euList) != 1 || euList[0].Workflow != "nightly" {
		t.Fatalf("eu schedules = %+v, want the declared nightly cron only", euList)
	}

	all, err := fx.c.Schedules(fx.ctx)
	if err != nil || len(all) != 2 {
		t.Fatalf("Schedules = %+v, %v", all, err)
	}
	for _, s := range all {
		if s.Name == "hourly" && s.ID != id {
			t.Errorf("listed ID %q, want %q", s.ID, id)
		}
	}
	if err := fx.c.PauseSchedule(fx.ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		t.Fatal(err)
	}
	if usList, _ = fx.us.Schedules(fx.ctx); !usList[0].Paused {
		t.Fatal("PauseSchedule did not reach us")
	}

	ref, err := fx.c.TriggerSchedule(fx.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(ref.RunID), "us:") {
		t.Fatalf("triggered run %q, want it in us", ref.RunID)
	}
	fx.waitStatus(ref, workflow.StatusSucceeded)
	runs, err := fx.c.Runs(fx.ctx, workflow.RunQuery{Labels: map[string]string{workflow.LabelSchedule: string(id)}})
	if err != nil || len(runs.Runs) != 1 || runs.Runs[0].Ref != ref || runs.Runs[0].Labels[workflow.LabelSchedule] != string(id) {
		t.Fatalf("runs of the schedule = %+v, %v", runs.Runs, err)
	}

	if err := fx.c.Unschedule(fx.ctx, id); err != nil {
		t.Fatal(err)
	}
	if usList, _ = fx.us.Schedules(fx.ctx); len(usList) != 0 {
		t.Fatalf("us still has %+v", usList)
	}
	if err := fx.c.Unschedule(fx.ctx, "ap:1"); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Unschedule(unknown region) = %v", err)
	}
	if _, err := fx.c.Schedule(fx.ctx, workflow.Schedule{Workflow: "report", Cron: "0 * * * *", Location: time.UTC, Labels: map[string]string{"region": "ap"}}); !errors.Is(err, regional.ErrUnknownRegion) {
		t.Fatalf("Schedule(unknown region) = %v", err)
	}
}

func TestDeclaredCronNeedsScheduleRegion(t *testing.T) {
	fx := newFixture(t)
	err := fx.c.Register(workflow.Workflow{Name: "nightly", Cron: []string{"0 3 * * *"}, Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}})
	if !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Register(cron, no schedule region) = %v", err)
	}
}

func TestRunFinishedPrefixed(t *testing.T) {
	fx := newFixture(t)
	got := make(chan workflow.RunRef, 1)
	if err := fx.c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { got <- ev.Ref }); err != nil {
		t.Fatal(err)
	}
	fx.up(waitWF)
	us := fx.enqueue("us")
	fx.waitStatus(us, workflow.StatusWaiting)
	if err := fx.c.Cancel(fx.ctx, us); err != nil {
		t.Fatal(err)
	}
	select {
	case ref := <-got:
		if ref != us {
			t.Fatalf("event ref %+v, want %+v", ref, us)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no run-finished event")
	}
}

func TestManageAcrossRegions(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	fx.enqueue("eu")
	fx.enqueue("us")
	n, err := fx.c.CancelAll(fx.ctx, workflow.RunQuery{Workflow: "wait", Statuses: []workflow.Status{workflow.StatusQueued, workflow.StatusWaiting, workflow.StatusRunning}})
	if err != nil || n != 2 {
		t.Fatalf("CancelAll = %d, %v; want 2", n, err)
	}
	if err := fx.c.Pause(fx.ctx, workflow.PauseTarget{Workflow: "wait"}); err != nil {
		t.Fatal(err)
	}
	for name, mc := range map[string]*memory.Client{"eu": fx.eu, "us": fx.us} {
		ps, _ := mc.Paused(fx.ctx)
		if len(ps) != 1 {
			t.Errorf("%s paused = %+v", name, ps)
		}
	}
	ps, err := fx.c.Paused(fx.ctx)
	if err != nil || len(ps) != 1 {
		t.Fatalf("Paused = %+v, %v", ps, err)
	}
	if h := fx.c.Health(fx.ctx); !h.Live || !h.Ready || !strings.HasPrefix(h.Checks[0].Name, "eu ") {
		t.Fatalf("Health = %+v", h)
	}
}
