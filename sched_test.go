package workflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
)

func mustCron(t *testing.T, expr string) cron.Schedule {
	t.Helper()
	s, err := cron.Parse(expr)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestActive(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := from.Add(3 * time.Hour)
	a := workflow.Active{From: from, Until: until}
	hourly := mustCron(t, "0 * * * *")
	cases := []struct {
		after, want time.Time
		why         string
	}{
		{from.Add(-48 * time.Hour), from, "a tick exactly at From fires"},
		{from, from.Add(time.Hour), "strictly after"},
		{from.Add(90 * time.Minute), from.Add(2 * time.Hour), "inside"},
		{from.Add(2 * time.Hour), time.Time{}, "Until is exclusive"},
		{until.Add(time.Hour), time.Time{}, "ended"},
	}
	for _, c := range cases {
		if got := workflow.NextActive(hourly, c.after, time.UTC, a); !got.Equal(c.want) {
			t.Errorf("%s: NextActive(%v) = %v, want %v", c.why, c.after, got, c.want)
		}
	}
	if !a.Ended(until.Add(-time.Nanosecond)) || a.Ended(until.Add(-time.Hour)) {
		t.Error("Ended is wrong at the bounds")
	}
	if got := workflow.NextActive(hourly, from, time.UTC, workflow.Active{}); !got.Equal(from.Add(time.Hour)) {
		t.Errorf("zero Active limits: %v", got)
	}
}

func TestNextRuns(t *testing.T) {
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	after := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	runs := workflow.NextRuns(workflow.NextRunsQuery{
		Schedule: mustCron(t, "0 * * * *"),
		Location: berlin,
		After:    after,
		Count:    3,
		Windows:  workflow.Windows{Between: []workflow.Window{{From: "15:00", To: "17:00"}}},
	})
	if len(runs) != 3 {
		t.Fatalf("got %d runs: %v", len(runs), runs)
	}
	// 15:00 and 16:00 Berlin today (13:00, 14:00 UTC), then 15:00 tomorrow.
	wantUTC := []time.Time{
		time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 14, 14, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 15, 13, 0, 0, 0, time.UTC),
	}
	for i, w := range wantUTC {
		if !runs[i].UTC.Equal(w) || runs[i].UTC.Location() != time.UTC || runs[i].Local.Location() != berlin {
			t.Errorf("run %d = %+v, want %v", i, runs[i], w)
		}
	}
	if got, want := runs[0].String(), "2026-10-14 15:00:00 CEST (13:00:00 UTC)"; got != want {
		t.Errorf("String = %q, want %q", got, want)
	}
	late := workflow.NextRun{Local: time.Date(2026, 10, 15, 1, 0, 0, 0, berlin)}
	late.UTC = late.Local.UTC()
	if !strings.Contains(late.String(), "(2026-10-14 23:00:00 UTC)") {
		t.Errorf("a different UTC day must carry its date: %q", late.String())
	}
	if got := workflow.NextRuns(workflow.NextRunsQuery{Schedule: mustCron(t, "* * * * *"), After: after, Count: 1000}); len(got) != workflow.MaxNextRuns {
		t.Errorf("Count is not capped: %d", len(got))
	}
	never := workflow.NextRuns(workflow.NextRunsQuery{Schedule: mustCron(t, "* * * * *"), After: after, Count: 5, Windows: workflow.Windows{Unless: []workflow.Window{{From: "00:00", To: "23:59"}}}})
	if len(never) > 5 {
		t.Errorf("scan is unbounded: %d", len(never))
	}
}

func TestInEnvironment(t *testing.T) {
	ctx := context.Background()
	if !workflow.InEnvironment("production", "staging", "production")(ctx, workflow.Tick{}) {
		t.Error("production refused")
	}
	if workflow.InEnvironment("dev", "production")(ctx, workflow.Tick{}) {
		t.Error("dev allowed")
	}
	if workflow.InEnvironment("production")(ctx, workflow.Tick{}) {
		t.Error("no allowed names must never fire")
	}
	yes := func(context.Context, workflow.Tick) bool { return true }
	no := func(context.Context, workflow.Tick) bool { return false }
	if !workflow.AllWhen(yes, nil)(ctx, workflow.Tick{}) || workflow.AllWhen(yes, no)(ctx, workflow.Tick{}) {
		t.Error("AllWhen is not an AND")
	}
}

func TestLocalCron(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC)
	fake := clock.NewFake(start)
	var errs []error
	lc := workflow.NewLocalCron(workflow.WithLocalCronClock(fake), workflow.WithLocalCronErrors(func(name string, at time.Time, err error) { errs = append(errs, err) }))
	ticks := make(chan time.Time)
	release := make(chan struct{})
	calls := 0
	err := lc.Add("warm", "* * * * *", time.UTC, func(_ context.Context, at time.Time) error {
		calls++
		ticks <- at
		if calls == 1 {
			<-release
		}
		if calls == 2 {
			panic("boom")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []struct {
		name string
		loc  *time.Location
		expr string
	}{{"", time.UTC, "* * * * *"}, {"x", nil, "* * * * *"}, {"x", time.Local, "* * * * *"}, {"x", time.UTC, "nope"}, {"warm", time.UTC, "* * * * *"}} {
		if err := lc.Add(bad.name, bad.expr, bad.loc, func(context.Context, time.Time) error { return nil }); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("Add(%q, %v, %q) = %v, want ErrInvalid", bad.name, bad.loc, bad.expr, err)
		}
	}
	runCtx, stop := context.WithCancel(ctx)
	if err := lc.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	if err := lc.Add("late", "* * * * *", time.UTC, func(context.Context, time.Time) error { return nil }); !errors.Is(err, workflow.ErrLocalCronStarted) {
		t.Errorf("Add after Start = %v", err)
	}
	if err := fake.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fake.Advance(30 * time.Second)
	if at := <-ticks; !at.Equal(start.Add(30 * time.Second)) {
		t.Fatalf("first tick at %v", at)
	}
	// The run is still going while three more minutes pass: those ticks
	// are skipped, and the next run is for the minute after it returns.
	fake.Advance(3 * time.Minute)
	close(release)
	if err := fake.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fake.Advance(time.Minute)
	if at, want := <-ticks, time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC); !at.Equal(want) {
		t.Fatalf("after a long run, tick at %v, want %v", at, want)
	}
	if err := fake.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	stop()
	select {
	case <-lc.Done():
	case <-ctx.Done():
		t.Fatal("Done never closed")
	}
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "panicked") {
		t.Fatalf("errors = %v, want the panic", errs)
	}
}

func TestTickTime(t *testing.T) {
	berlin, _ := cron.LoadZone("Europe/Berlin")
	at := time.Date(2026, 3, 29, 9, 0, 0, 123, berlin)
	labels := workflow.FiredLabelsAt(map[string]string{"a": "1"}, "s1", "digest", at)
	if labels["a"] != "1" || labels[workflow.LabelSchedule] != "s1" || labels[workflow.LabelScheduleName] != "digest" {
		t.Fatalf("labels = %v", labels)
	}
	got, ok := workflow.StepInfo{Labels: labels}.TickAt()
	if !ok || !got.Equal(at) || got.Location() != time.UTC {
		t.Fatalf("TickAt = %v %v, want %v in UTC", got, ok, at)
	}
	if _, ok := workflow.TickTime(map[string]string{workflow.LabelTickAt: "yesterday"}); ok {
		t.Error("an unparsable label was accepted")
	}
	if _, ok := (workflow.StepInfo{}).TickAt(); ok {
		t.Error("a run without the label has a tick")
	}
	if _, reserved := workflow.ReservedLabel(workflow.LabelTickAt); !reserved {
		t.Error("LabelTickAt is not reserved")
	}
}

func TestParseCron(t *testing.T) {
	if _, err := workflow.ParseCron("0 0 9 * * *", true, nil); err != nil {
		t.Errorf("seconds: %v", err)
	}
	if _, err := workflow.ParseCron("0 0 9 * * *", false, nil); err == nil {
		t.Error("six fields parsed without Seconds")
	}
	called := false
	custom := func(expr string) (cron.Schedule, error) { called = true; return cron.Parse("@daily") }
	if _, err := workflow.ParseCron("anything", false, custom); err != nil || !called {
		t.Errorf("custom parser not used: %v", err)
	}
	called = false
	if _, err := workflow.ParseCron("0 0 9 * * *", true, custom); err != nil || called {
		t.Errorf("Seconds must win over a custom parser: %v called=%v", err, called)
	}
}
