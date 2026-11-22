package middleware_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"regexp"
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
//
//ds:def id=mw-testlatestonlyrunsrequestedruns-chgpzhry owner=@khanakia stability=stable
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

// TestLatestOnlyDocNamesRealAPI: every Workflow.X the LatestOnly doc
// comment names is a field or method of workflow.Workflow (it once named
// a Workflow.Use that never existed).
func TestLatestOnlyDocNamesRealAPI(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "latestonly.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var doc string
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == "LatestOnly" {
			doc = fn.Doc.Text()
		}
	}
	wf := reflect.TypeOf(workflow.Workflow{})
	for _, m := range regexp.MustCompile(`\bWorkflow\.([A-Z]\w*)`).FindAllStringSubmatch(doc, -1) {
		_, field := wf.FieldByName(m[1])
		_, method := wf.MethodByName(m[1])
		_, ptrMethod := reflect.PointerTo(wf).MethodByName(m[1])
		if !field && !method && !ptrMethod {
			t.Errorf("the LatestOnly doc comment names Workflow.%s, which does not exist", m[1])
		}
	}
}

// TestLatestOnlyMisfireRule: a due tick counts as newer only when it
// really fired or will fire. Without schedule history (ScheduleInfo.Last
// zero) that is a tick the schedule's misfire policy fires on a catch-up
// from the run's tick to now (Misfire.Due): whatever happened in
// between, it or a newer tick fires. With history, a recorded newer fire
// counts too, whatever the policy; an unrecorded tick is judged by the
// policy, so a catch-up still being enqueued makes its older runs stale.
//
//ds:def id=mw-testlatestonlymisfirerule-nytp7njq owner=@khanakia stability=stable
func TestLatestOnlyMisfireRule(t *testing.T) {
	hourly := workflow.ScheduleInfo{ID: "s1", Cron: "0 * * * *", Location: time.UTC}
	with := func(f func(*workflow.ScheduleInfo)) fakeLister { s := hourly; f(&s); return fakeLister{s} }
	skip := workflow.Misfire{Policy: workflow.MisfireSkip}
	own := workflow.ScheduleLast{Fire: at(9, 0), Run: workflow.RunRef{RunID: "r1"}}
	cases := []struct {
		name     string
		lister   fakeLister
		fallback workflow.Misfire // LatestOnlyConfig.DefaultMisfire
		now      time.Time
		wantRan  bool
	}{
		{"skip: 10:00-12:00 dropped by a late wake, nothing fires", with(func(s *workflow.ScheduleInfo) { s.Misfire = skip }), workflow.Misfire{}, at(12, 30), true},
		{"skip: 12:00 is on time and fires", with(func(s *workflow.ScheduleInfo) { s.Misfire = skip }), workflow.Misfire{}, at(12, 0).Add(30 * time.Second), false},
		{"fire once within 10m: 12:00 is 30m late, dropped", with(func(s *workflow.ScheduleInfo) {
			s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: 10 * time.Minute}
		}), workflow.Misfire{}, at(12, 30), true},
		{"fire once within 10m: 12:00 is 5m late, fires", with(func(s *workflow.ScheduleInfo) {
			s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: 10 * time.Minute}
		}), workflow.Misfire{}, at(12, 5), false},
		{"fire all: the missed ticks fire", with(func(s *workflow.ScheduleInfo) {
			s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 3}
		}), workflow.Misfire{}, at(12, 30), false},
		{"fire once: the newest due tick is outside the window, nothing fires", with(func(s *workflow.ScheduleInfo) {
			s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce}
			s.Windows = workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "11:00"}}}
		}), workflow.Misfire{}, at(12, 30), true},
		{"default policy: fire once, so a due tick fires", fakeLister{hourly}, workflow.Misfire{}, at(12, 30), false},
		{"default policy given as skip (a driver that never catches up)", fakeLister{hourly}, skip, at(12, 30), true},
		{"history: 10:00 is on time and not recorded yet", with(func(s *workflow.ScheduleInfo) { s.Last = own }), workflow.Misfire{}, at(10, 0).Add(30 * time.Second), false},
		{"history: a catch-up still enqueueing 10:00-12:00", with(func(s *workflow.ScheduleInfo) {
			s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 3}
			s.Last = own
		}), workflow.Misfire{}, at(12, 30), false},
		{"history: within 10m, 12:00 is 30m late and never fired", with(func(s *workflow.ScheduleInfo) {
			s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: 10 * time.Minute}
			s.Last = own
		}), workflow.Misfire{}, at(12, 30), true},
		{"history: skip policy, 12:00 fired on time", with(func(s *workflow.ScheduleInfo) {
			s.Misfire = skip
			s.Last = workflow.ScheduleLast{Fire: at(12, 0), Run: workflow.RunRef{RunID: "r4"}}
		}), workflow.Misfire{}, at(12, 30), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := middleware.LatestOnlyConfig{Schedules: tc.lister, DefaultMisfire: tc.fallback, Clock: clock.NewFake(tc.now)}
			if got := latestCase(t, cfg, at(9, 0), nil); got != tc.wantRan {
				t.Fatalf("handler ran = %v, want %v", got, tc.wantRan)
			}
		})
	}
}
