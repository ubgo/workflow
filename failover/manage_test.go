package failover_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/failover"
)

// TestRuns_MergesBothBackendsAcrossPages pages through a feed whose runs
// alternate between the backends and checks every run comes once, newest
// first, with the prefix of the backend that holds it.
func TestRuns_MergesBothBackendsAcrossPages(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	var want []workflow.RunRef
	for i, onSecondary := range []bool{false, false, true, false, true, true, false} {
		fx.p.down.Store(onSecondary)
		want = append([]workflow.RunRef{fx.enqueue(string(rune('a' + i)))}, want...) // newest first
		fx.clk.Advance(time.Second)
	}
	fx.p.down.Store(false)

	for _, limit := range []int{1, 2, 3, 100} {
		var got []workflow.RunRef
		q := workflow.RunQuery{Workflow: "wait", Limit: limit}
		for pages := 0; ; pages++ {
			if pages > len(want) {
				t.Fatalf("limit %d: paging did not end", limit)
			}
			page, err := fx.c.Runs(fx.ctx, q)
			if err != nil {
				t.Fatalf("limit %d: Runs: %v", limit, err)
			}
			if len(page.Runs) > limit {
				t.Fatalf("limit %d: page of %d", limit, len(page.Runs))
			}
			for _, r := range page.Runs {
				got = append(got, r.Ref)
			}
			if page.Next == "" {
				break
			}
			q.Cursor = page.Next
		}
		if len(got) != len(want) {
			t.Fatalf("limit %d: %d runs, want %d: %v", limit, len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("limit %d: run %d = %v, want %v (all: %v)", limit, i, got[i], want[i], got)
			}
		}
	}
	if _, err := fx.c.Runs(fx.ctx, workflow.RunQuery{Cursor: "not-ours"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("foreign cursor: %v, want ErrInvalid", err)
	}
}

// TestRuns_RefNamesOneBackend checks that a Ref is answered by the backend
// its prefix names alone: the two runs have colliding driver IDs, so a ref
// sent to both backends, or with the prefix left on, finds the wrong run or
// none. A ref this client did not make matches nothing.
func TestRuns_RefNamesOneBackend(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	onPrimary := fx.enqueue("p")
	fx.p.down.Store(true)
	onSecondary := fx.enqueue("s")
	fx.p.down.Store(false)

	for _, want := range []workflow.RunRef{onPrimary, onSecondary} {
		page, err := fx.c.Runs(fx.ctx, workflow.RunQuery{Ref: want})
		if err != nil {
			t.Fatalf("Runs(%v): %v", want, err)
		}
		if len(page.Runs) != 1 || page.Runs[0].Ref != want || page.Next != "" {
			t.Fatalf("Runs(%v) = %+v, want exactly that run", want, page)
		}
	}
	bare := workflow.RunRef{RunID: workflow.RunID(strings.TrimPrefix(string(onPrimary.RunID), "primary:"))}
	mixed := workflow.RunRef{RunID: onPrimary.RunID, ScheduledID: workflow.ScheduledID("secondary:x")}
	for _, ref := range []workflow.RunRef{bare, mixed} {
		page, err := fx.c.Runs(fx.ctx, workflow.RunQuery{Ref: ref})
		if err != nil || len(page.Runs) != 0 {
			t.Fatalf("Runs(%v) = %+v, %v; want no runs", ref, page, err)
		}
	}
}

// TestRetryAndDelete_RouteByPrefix uses two runs whose driver IDs collide,
// so a call routed to the wrong backend acts on the other run.
func TestRetryAndDelete_RouteByPrefix(t *testing.T) {
	fx := newFixture(t)
	var healed atomic.Bool
	flaky := workflow.Workflow{Name: "wait", Steps: []workflow.Step{{Name: "s", Run: func(context.Context, []byte) ([]byte, error) {
		if healed.Load() {
			return nil, nil
		}
		return nil, workflow.Permanent(errors.New("boom"))
	}}}}
	fx.up(flaky)
	onP := fx.enqueue("p")
	fx.p.down.Store(true)
	onS := fx.enqueue("s")
	fx.p.down.Store(false)
	fx.waitStatus(onP, workflow.StatusFailed)
	fx.waitStatus(onS, workflow.StatusFailed)

	healed.Store(true)
	ref, err := fx.c.Retry(fx.ctx, onS)
	if err != nil || !strings.HasPrefix(string(ref.RunID), "secondary:") {
		t.Fatalf("Retry(secondary run) = %+v, %v", ref, err)
	}
	fx.waitStatus(onS, workflow.StatusSucceeded)
	fx.waitStatus(onP, workflow.StatusFailed) // untouched

	if err := fx.c.Delete(fx.ctx, onP); err != nil {
		t.Fatalf("Delete(primary run): %v", err)
	}
	if _, err := fx.c.Status(fx.ctx, onP); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("deleted run: %v, want ErrNotFound", err)
	}
	fx.waitStatus(onS, workflow.StatusSucceeded) // the secondary's run with the same ID stays

	// An unprefixed ref is found on the backend that knows it, and Retry
	// answers with that backend's prefix.
	healed.Store(false)
	fx.p.down.Store(true)
	onS2 := fx.enqueue("s2")
	fx.p.down.Store(false)
	fx.waitStatus(onS2, workflow.StatusFailed)
	healed.Store(true)
	raw := workflow.RunRef{RunID: workflow.RunID(strings.TrimPrefix(string(onS2.RunID), "secondary:"))}
	if ref, err := fx.c.Retry(fx.ctx, raw); err != nil || ref.RunID != onS2.RunID {
		t.Fatalf("Retry(unprefixed) = %+v, %v; want %v", ref, err, onS2.RunID)
	}
}

func TestCancelAllAndPause_ReachBothBackends(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	onP := fx.enqueue("p")
	fx.p.down.Store(true)
	onS := fx.enqueue("s")
	fx.p.down.Store(false)
	fx.waitStatus(onP, workflow.StatusWaiting)
	fx.waitStatus(onS, workflow.StatusWaiting)

	pause := workflow.PauseWorkflow("wait")
	if err := fx.c.Pause(fx.ctx, pause); err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string]workflow.Pauser{"primary": fx.p.Client, "secondary": fx.s.Client} {
		if got, _ := b.Paused(fx.ctx); len(got) != 1 || got[0] != pause {
			t.Fatalf("%s paused = %v", name, got)
		}
	}
	if got, err := fx.c.Paused(fx.ctx); err != nil || len(got) != 1 || got[0] != pause {
		t.Fatalf("Paused = %v, %v; want one target, not one per backend", got, err)
	}
	if err := fx.c.Resume(fx.ctx, pause); err != nil {
		t.Fatal(err)
	}
	if got, _ := fx.c.Paused(fx.ctx); len(got) != 0 {
		t.Fatalf("after Resume: %v", got)
	}

	n, err := fx.c.CancelAll(fx.ctx, workflow.RunQuery{Workflow: "wait", Statuses: []workflow.Status{workflow.StatusWaiting}})
	if err != nil || n != 2 {
		t.Fatalf("CancelAll = %d, %v; want 2", n, err)
	}
	fx.waitStatus(onP, workflow.StatusCancelled)
	fx.waitStatus(onS, workflow.StatusCancelled)
}

// TestScheduleFamily_IsThePrimarys checks the schedule capabilities come
// from the primary alone, that listing, pausing and ticks name the
// primary, and that a secondary without them is not asked.
func TestScheduleFamily_IsThePrimarys(t *testing.T) {
	fx := newFixture(t)
	sched := workflow.CapScheduleList | workflow.CapSchedulePause | workflow.CapTickEvents | workflow.CapScheduleWhen | workflow.CapScheduleWindow
	sec := noCaps{Client: fx.s.Client, hidden: sched}
	fx.c = failover.New(fx.p, sec)
	if !fx.c.Capabilities().Has(sched) {
		t.Fatalf("capabilities %s lack the primary's schedule family", fx.c.Capabilities())
	}
	if both := failover.New(fx.p, sec, failover.WithSchedulesOnBoth()); both.Capabilities()&sched != 0 {
		t.Fatalf("WithSchedulesOnBoth over a secondary without them claims %s", both.Capabilities()&sched)
	}

	ticks := make(chan workflow.Tick, 4)
	if err := fx.c.OnTick(func(_ context.Context, tk workflow.Tick) { ticks <- tk }); err != nil {
		t.Fatal(err)
	}
	// The condition is registered on the primary only: the secondary,
	// which lacks CapScheduleWhen, never sees it.
	fx.up(workflow.Workflow{Name: "tick", ScheduleWhen: func(context.Context, workflow.Tick) bool { return true },
		Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}})
	id, err := fx.c.Schedule(fx.ctx, workflow.Schedule{Name: "s", Workflow: "tick", Cron: "* * * * *", Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	infos, err := fx.c.Schedules(fx.ctx)
	if err != nil || len(infos) != 1 || infos[0].ID != id {
		t.Fatalf("Schedules = %+v, %v; want one with ID %q", infos, err, id)
	}

	if err := fx.c.PauseSchedule(fx.ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		t.Fatal(err)
	}
	if infos, _ := fx.c.Schedules(fx.ctx); !infos[0].Paused {
		t.Fatal("PauseSchedule did not reach the primary")
	}
	if err := fx.c.ResumeSchedule(fx.ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		t.Fatal(err)
	}
	if err := fx.c.PauseSchedule(fx.ctx, workflow.ScheduleTarget{ID: "secondary:x"}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("pausing a secondary ID without schedules there: %v, want ErrNotFound", err)
	}

	bctx, cancel := context.WithTimeout(fx.ctx, 5*time.Second)
	defer cancel()
	if err := fx.clk.BlockUntil(bctx, 1); err != nil {
		t.Fatal(err)
	}
	fx.clk.Advance(time.Minute)
	select {
	case tk := <-ticks:
		if tk.ScheduleID != id || !strings.HasPrefix(string(tk.Run.RunID), "primary:") {
			t.Fatalf("tick = %+v; want ScheduleID %q and a primary run", tk, id)
		}
		fx.waitStatus(tk.Run, workflow.StatusSucceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("no tick")
	}
}

// TestRefusesWhatOneBackendLacks hides each run-family capability from
// the secondary and checks the call is refused, not half done.
func TestRefusesWhatOneBackendLacks(t *testing.T) {
	ctx := t.Context()
	cases := []struct {
		hide workflow.Caps
		call func(c *failover.Client) error
	}{
		{workflow.CapRunQuery, func(c *failover.Client) error { _, err := c.Runs(ctx, workflow.RunQuery{}); return err }},
		{workflow.CapRetryRun, func(c *failover.Client) error {
			_, err := c.Retry(ctx, workflow.RunRef{RunID: "primary:1"})
			return err
		}},
		{workflow.CapDeleteRun, func(c *failover.Client) error { return c.Delete(ctx, workflow.RunRef{RunID: "primary:1"}) }},
		{workflow.CapCancelAll, func(c *failover.Client) error { _, err := c.CancelAll(ctx, workflow.RunQuery{}); return err }},
		{workflow.CapPauseWorkflow, func(c *failover.Client) error { return c.Pause(ctx, workflow.PauseAll()) }},
		{workflow.CapPauseQueue, func(c *failover.Client) error { return c.Pause(ctx, workflow.PauseQueue("q")) }},
		{workflow.CapDrain, func(c *failover.Client) error { return c.Drain(ctx) }},
	}
	for _, tc := range cases {
		fx := newFixture(t)
		c := failover.New(fx.p, noCaps{Client: fx.s.Client, hidden: tc.hide})
		var ue *workflow.UnsupportedError
		if err := tc.call(c); !errors.As(err, &ue) || ue.Missing&tc.hide == 0 {
			t.Errorf("hiding %s: err = %v, want an UnsupportedError naming it", tc.hide, err)
		}
		if c.Capabilities().Has(tc.hide) {
			t.Errorf("hiding %s: still claimed", tc.hide)
		}
	}
}

func TestDrain_StopsBoth(t *testing.T) {
	fx := newFixture(t)
	fx.up(waitWF)
	ctx, cancel := context.WithTimeout(fx.ctx, 5*time.Second)
	defer cancel()
	done := fx.c.Done()
	if err := fx.c.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	for name, d := range map[string]<-chan struct{}{"failover": done, "primary": fx.p.Done(), "secondary": fx.s.Done()} {
		select {
		case <-d:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s Done not closed", name)
		}
	}
}

// TestRegister_SecondaryCopyHasNoScheduleFields: without
// WithSchedulesOnBoth the secondary's copy drops Cron, so it must drop
// CronWindows too, which validation refuses without a Cron.
func TestRegister_SecondaryCopyHasNoScheduleFields(t *testing.T) {
	fx := newFixture(t)
	err := fx.c.Register(workflow.Workflow{Name: "w", Cron: []string{"0 9 * * *"},
		CronWindows: workflow.Windows{Between: []workflow.Window{{From: "08:00", To: "18:00"}}},
		Misfire:     workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: time.Hour},
		CronOverlap: workflow.OverlapSkip,
		Steps:       []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
}

func TestTriggerAndForce_RouteByPrefix(t *testing.T) {
	fx := newFixture(t)
	g := make(chan struct{})
	t.Cleanup(func() { close(g) })
	fx.up(workflow.Workflow{Name: "wait", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-g:
		case <-ctx.Done():
		}
		return nil, ctx.Err()
	}}}})
	id, err := fx.c.Schedule(fx.ctx, workflow.Schedule{Name: "n", Workflow: "wait", Cron: "0 3 * * *", Location: time.UTC, Overlap: workflow.OverlapSkip})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := fx.c.TriggerSchedule(fx.ctx, id)
	if err != nil || !strings.HasPrefix(string(ref.RunID), "primary:") {
		t.Fatalf("TriggerSchedule = %+v, %v; want a primary run", ref, err)
	}
	if _, err := fx.c.TriggerSchedule(fx.ctx, id); !errors.Is(err, workflow.ErrOverlap) {
		t.Fatalf("second trigger: %v, want ErrOverlap", err)
	}
	infos, err := fx.c.Schedules(fx.ctx)
	if err != nil || len(infos) != 1 || infos[0].Last.Run != ref {
		t.Fatalf("Schedules = %+v, %v; want Last.Run %+v (prefixed)", infos, err, ref)
	}
	if err := fx.c.ForceComplete(fx.ctx, ref); err != nil {
		t.Fatalf("ForceComplete: %v", err)
	}
	fx.waitStatus(ref, workflow.StatusSucceeded)

	fx.p.down.Store(true)
	onS := fx.enqueue("s")
	fx.p.down.Store(false)
	if err := fx.c.ForceFail(fx.ctx, onS, "stop"); err != nil {
		t.Fatalf("ForceFail(secondary run): %v", err)
	}
	fx.waitStatus(onS, workflow.StatusFailed)
}

// TestScheduleLabel_RoundTrips takes the LabelSchedule of a fired run from
// every place a run is handed out (StepInfo, Runs, RunEvent) and checks it
// is the ID this client issued, usable with PauseSchedule and as a run
// query filter, on the primary and, under WithSchedulesOnBoth, on the
// secondary's copy.
func TestScheduleLabel_RoundTrips(t *testing.T) {
	for _, schedBoth := range []bool{false, true} {
		var opts []failover.Option
		if schedBoth {
			opts = append(opts, failover.WithSchedulesOnBoth())
		}
		fx := newFixture(t, opts...)
		stepLabels := make(chan string, 4)
		events := make(chan string, 4)
		if err := fx.c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { events <- ev.Labels[workflow.LabelSchedule] }); err != nil {
			t.Fatal(err)
		}
		fx.up(workflow.Workflow{Name: "fire", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			stepLabels <- info.Labels[workflow.LabelSchedule]
			return nil, nil
		}}}})
		id, err := fx.c.Schedule(fx.ctx, workflow.Schedule{Name: "n", Workflow: "fire", Cron: "0 3 * * *", Location: time.UTC})
		if err != nil {
			t.Fatal(err)
		}
		infos, err := fx.c.Schedules(fx.ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, info := range infos {
			ref, err := fx.c.TriggerSchedule(fx.ctx, info.ID)
			if err != nil {
				t.Fatalf("both=%v: TriggerSchedule(%s): %v", schedBoth, info.ID, err)
			}
			fx.waitStatus(ref, workflow.StatusSucceeded)
			want := string(info.ID)
			if got := <-stepLabels; got != want {
				t.Errorf("both=%v: StepInfo label = %q, want %q", schedBoth, got, want)
			}
			if got := <-events; got != want {
				t.Errorf("both=%v: RunEvent label = %q, want %q", schedBoth, got, want)
			}
			page, err := fx.c.Runs(fx.ctx, workflow.RunQuery{Labels: map[string]string{workflow.LabelSchedule: want}})
			if err != nil || len(page.Runs) != 1 || page.Runs[0].Ref != ref {
				t.Fatalf("both=%v: Runs by label %q = %+v, %v; want the one run %+v", schedBoth, want, page, err, ref)
			}
			label := page.Runs[0].Labels[workflow.LabelSchedule]
			if label != want {
				t.Errorf("both=%v: RunInfo label = %q, want %q", schedBoth, label, want)
			}
			if err := fx.c.PauseSchedule(fx.ctx, workflow.ScheduleTarget{ID: workflow.ScheduleID(label)}); err != nil {
				t.Errorf("both=%v: PauseSchedule(label %q): %v", schedBoth, label, err)
			}
		}
		if !schedBoth && (len(infos) != 1 || infos[0].ID != id) {
			t.Errorf("Schedules = %+v, want the one schedule %q", infos, id)
		}
		paused, _ := fx.c.Schedules(fx.ctx)
		for _, info := range paused {
			if !info.Paused {
				t.Errorf("both=%v: %s not paused through its run's label", schedBoth, info.ID)
			}
		}
	}
}
