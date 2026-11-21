package middleware_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

// latestCase runs one LatestOnly step for a run fired at tick (plus
// extra labels) and reports whether the handler ran.
func latestCase(t *testing.T, cfg middleware.LatestOnlyConfig, tick time.Time, extra map[string]string) bool {
	t.Helper()
	var ran atomic.Bool
	h := middleware.LatestOnly(cfg)(func(context.Context, []byte) ([]byte, error) { ran.Store(true); return nil, nil })
	labels := workflow.FiredLabelsAt(extra, "s1", "", tick)
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{RunID: "r1", Labels: labels})
	if _, err := h(ctx, []byte("input")); err != nil {
		t.Fatal(err)
	}
	return ran.Load()
}

// at is 2026-03-02 (a Monday) at hh:mm UTC.
func at(hh, mm int) time.Time { return time.Date(2026, 3, 2, hh, mm, 0, 0, time.UTC) }

// TestLatestOnlyNextFireHonoursTheSchedule: the tick counted as newer is
// one the schedule would really fire, through the schedule's own rules:
// a tick outside its Windows or Active period, any tick while it is
// paused, and a tick its ScheduleWhen (LatestOnlyConfig.When) rejects do
// not make a run stale; a fire the driver recorded (ScheduleInfo.Last)
// after the run's tick does.
func TestLatestOnlyNextFireHonoursTheSchedule(t *testing.T) {
	hourly := workflow.ScheduleInfo{ID: "s1", Cron: "0 * * * *", Location: time.UTC}
	with := func(f func(*workflow.ScheduleInfo)) fakeLister { s := hourly; f(&s); return fakeLister{s} }
	cases := []struct {
		name    string
		lister  fakeLister
		when    func(context.Context, workflow.Tick) bool
		now     time.Time
		tick    time.Time
		wantRan bool
	}{
		{"plain: the 17:00 tick is due", fakeLister{hourly}, nil, at(17, 5), at(16, 0), false},
		{"windows: 17:00 is outside 09:00-17:00, next is tomorrow 09:00", with(func(s *workflow.ScheduleInfo) {
			s.Windows = workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "17:00"}}}
		}), nil, at(17, 5), at(16, 0), true},
		{"windows: 16:00 is inside, so the 15:00 run is stale", with(func(s *workflow.ScheduleInfo) {
			s.Windows = workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "17:00"}}}
		}), nil, at(16, 5), at(15, 0), false},
		{"active: the period ended at 16:30", with(func(s *workflow.ScheduleInfo) {
			s.Active = workflow.Active{Until: at(16, 30)}
		}), nil, at(17, 5), at(16, 0), true},
		{"paused: no tick fires while paused", with(func(s *workflow.ScheduleInfo) { s.Paused = true }), nil, at(17, 5), at(16, 0), true},
		{"when: the condition skips 17:00", fakeLister{hourly}, func(_ context.Context, tk workflow.Tick) bool {
			return tk.At.Hour() != 17
		}, at(17, 5), at(16, 0), true},
		{"when: the condition fires 17:00", fakeLister{hourly}, func(context.Context, workflow.Tick) bool { return true }, at(17, 5), at(16, 0), false},
		{"overlap skip: no tick fires while this run is unfinished", with(func(s *workflow.ScheduleInfo) { s.Overlap = workflow.OverlapSkip }), nil, at(17, 5), at(16, 0), true},
		{"history: the recorded fire is this run's own", with(func(s *workflow.ScheduleInfo) {
			s.Paused = true
			s.Last = workflow.ScheduleLast{Fire: at(16, 0), Run: workflow.RunRef{RunID: "r1"}}
		}), nil, at(17, 5), at(16, 0), true},
		{"history: a later fire is recorded although paused now", with(func(s *workflow.ScheduleInfo) {
			s.Paused = true
			s.Last = workflow.ScheduleLast{Fire: at(17, 0), Run: workflow.RunRef{RunID: "r2"}}
		}), nil, at(17, 5), at(16, 0), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := middleware.LatestOnlyConfig{Schedules: tc.lister, When: tc.when, Clock: clock.NewFake(tc.now)}
			if got := latestCase(t, cfg, tc.tick, nil); got != tc.wantRan {
				t.Fatalf("handler ran = %v, want %v", got, tc.wantRan)
			}
		})
	}
}

// TestLatestOnlyRunsRequestedRuns: a run someone asked for (LabelTrigger:
// a backfill, a manual trigger, a startup fire) carries a past tick but
// is never stale, with the lookup and with Next alike.
func TestLatestOnlyRunsRequestedRuns(t *testing.T) {
	clk := clock.NewFake(at(17, 5))
	lister := fakeLister{{ID: "s1", Cron: "0 * * * *", Location: time.UTC}}
	due := func(context.Context, workflow.StepInfo, time.Time) (time.Time, bool, error) {
		return at(17, 0), true, nil
	}
	for _, trigger := range []string{workflow.LabelTriggerBackfill, workflow.LabelTriggerManual, workflow.LabelTriggerStartup} {
		extra := map[string]string{workflow.LabelTrigger: trigger}
		if !latestCase(t, middleware.LatestOnlyConfig{Schedules: lister, Clock: clk}, at(9, 0), extra) {
			t.Errorf("%s run skipped (lookup)", trigger)
		}
		if !latestCase(t, middleware.LatestOnlyConfig{Next: due, Clock: clk}, at(9, 0), extra) {
			t.Errorf("%s run skipped (Next)", trigger)
		}
	}
	// A cron fire with the same tick is stale.
	if latestCase(t, middleware.LatestOnlyConfig{Schedules: lister, Clock: clk}, at(9, 0), nil) {
		t.Error("a stale cron fire ran")
	}
}
