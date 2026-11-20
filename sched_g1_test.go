package workflow_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/memory"
)

// tickRecorder is a one-step workflow that records the tick and labels of
// every run it executes.
type tickRecorder struct {
	mu     sync.Mutex
	ticks  []time.Time
	labels []map[string]string
}

func (r *tickRecorder) workflow(name string) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		at, _ := info.TickAt()
		r.mu.Lock()
		r.ticks = append(r.ticks, at)
		r.labels = append(r.labels, info.Labels)
		r.mu.Unlock()
		return nil, nil
	}}}}
}

func (r *tickRecorder) got() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.ticks)
}

// waitTicks polls until r ran n times.
func (r *tickRecorder) waitTicks(t *testing.T, n int) []time.Time {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(r.got()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("ran %d times, want %d: %v", len(r.got()), n, r.got())
		}
		time.Sleep(time.Millisecond)
	}
	return r.got()
}

// g1Client starts a memory client on a fake clock at start.
func g1Client(t *testing.T, start time.Time, wfs []workflow.Workflow, opts ...memory.Option) (*memory.Client, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(start)
	c := memory.New(append([]memory.Option{memory.WithClock(fc)}, opts...)...)
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return c, fc
}

// stepTo waits until n timers are armed, then moves the clock to at.
func stepTo(t *testing.T, fc *clock.Fake, n int, at time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fc.BlockUntil(ctx, n); err != nil {
		t.Fatalf("no timer armed before %v: %v", at, err)
	}
	fc.Set(at)
}

// TestExtendedCronFiresOnMemory: an "L" schedule fires on the last day of
// each month on memory built with cron.Extended, and the default parser
// refuses it.
func TestExtendedCronFiresOnMemory(t *testing.T) {
	start := time.Date(2026, 1, 30, 12, 0, 0, 0, time.UTC)
	rec := &tickRecorder{}
	c, fc := g1Client(t, start, []workflow.Workflow{rec.workflow("eom")}, memory.WithCronParser(cron.Extended))
	ctx := context.Background()
	if _, err := c.Schedule(ctx, workflow.Schedule{Name: "month-end", Workflow: "eom", Cron: "0 9 L * *", Location: time.UTC}); err != nil {
		t.Fatal(err)
	}
	jan31 := time.Date(2026, 1, 31, 9, 0, 0, 0, time.UTC)
	feb28 := time.Date(2026, 2, 28, 9, 0, 0, 0, time.UTC)
	stepTo(t, fc, 1, jan31)
	rec.waitTicks(t, 1)
	stepTo(t, fc, 1, feb28.Add(-time.Hour)) // nothing on Feb 27
	stepTo(t, fc, 1, feb28)
	if got := rec.waitTicks(t, 2); !got[0].Equal(jan31) || !got[1].Equal(feb28) {
		t.Fatalf("ticks %v, want Jan 31 and Feb 28 at 09:00", got)
	}

	plain, _ := g1Client(t, start, []workflow.Workflow{(&tickRecorder{}).workflow("eom")})
	if _, err := plain.Schedule(ctx, workflow.Schedule{Workflow: "eom", Cron: "0 9 L * *", Location: time.UTC}); err == nil {
		t.Fatal("the default parser accepted L")
	}
}

// TestHashedCronRunsOnMemory: cron.Hash's output is a standard
// expression a driver without a custom parser schedules.
func TestHashedCronRunsOnMemory(t *testing.T) {
	expr, err := cron.Hash("H * * * *", "tenant-7/digest")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)
	rec := &tickRecorder{}
	c, fc := g1Client(t, start, []workflow.Workflow{rec.workflow("digest")})
	if _, err := c.Schedule(context.Background(), workflow.Schedule{Workflow: "digest", Cron: expr, Location: time.UTC}); err != nil {
		t.Fatal(err)
	}
	want := mustCron(t, expr).Next(start, time.UTC)
	stepTo(t, fc, 1, want)
	if got := rec.waitTicks(t, 1); !got[0].Equal(want) {
		t.Fatalf("tick %v, want %v", got[0], want)
	}
}

func TestExclusionCalendars(t *testing.T) {
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	holidays, err := workflow.ExcludeDates(berlin, "2026-12-25", "2026-12-26")
	if err != nil {
		t.Fatal(err)
	}
	weekends, err := workflow.ExcludeWeekdays(berlin, time.Saturday, time.Sunday)
	if err != nil {
		t.Fatal(err)
	}
	cal := workflow.AnyExcluded(holidays, weekends, nil)
	cases := []struct {
		at   time.Time
		want bool
	}{
		{time.Date(2026, 12, 24, 22, 59, 0, 0, time.UTC), false}, // 23:59 in Berlin on the 24th
		{time.Date(2026, 12, 24, 23, 0, 0, 0, time.UTC), true},   // midnight in Berlin: the 25th
		{time.Date(2026, 12, 26, 12, 0, 0, 0, time.UTC), true},
		{time.Date(2026, 12, 28, 9, 0, 0, 0, time.UTC), false}, // a Monday
		{time.Date(2026, 12, 27, 9, 0, 0, 0, time.UTC), true},  // a Sunday
	}
	for _, c := range cases {
		if got := cal.Excluded(c.at); got != c.want {
			t.Errorf("Excluded(%v) = %v, want %v", c.at, got, c.want)
		}
	}
	for _, bad := range []func() error{
		func() error { _, err := workflow.ExcludeDates(berlin, "2026-02-30"); return err },
		func() error { _, err := workflow.ExcludeDates(berlin, "2026-2-3"); return err },
		func() error { _, err := workflow.ExcludeDates(nil, "2026-12-25"); return err },
		func() error { _, err := workflow.ExcludeDates(time.Local, "2026-12-25"); return err },
		func() error { _, err := workflow.ExcludeWeekdays(berlin, time.Weekday(7)); return err },
		func() error { _, err := workflow.ExcludeWeekdays(nil, time.Monday); return err },
	} {
		if err := bad(); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("got %v, want ErrInvalid", err)
		}
	}
	odd := workflow.ExcludeFunc(func(t time.Time) bool { return t.Day()%2 == 1 })
	when := workflow.SkipExcluded(odd)
	if when(context.Background(), workflow.Tick{At: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)}) {
		t.Error("an excluded tick fires")
	}
	if !workflow.SkipExcluded(nil)(context.Background(), workflow.Tick{At: time.Now()}) || workflow.ExcludeFunc(nil).Excluded(time.Now()) || workflow.AnyExcluded().Excluded(time.Now()) {
		t.Error("an empty calendar excludes")
	}
}

// TestSkipExcludedOnMemory: the tick of an excluded day is skipped and
// reported as TickSkipped with TickReasonWhen; the next day fires.
func TestSkipExcludedOnMemory(t *testing.T) {
	start := time.Date(2026, 12, 24, 12, 0, 0, 0, time.UTC)
	holidays, _ := workflow.ExcludeDates(time.UTC, "2026-12-25")
	rec := &tickRecorder{}
	wf := rec.workflow("nightly")
	wf.Cron = []string{"0 0 * * *"}
	wf.ScheduleWhen = workflow.AllWhen(workflow.InEnvironment("prod", "prod"), workflow.SkipExcluded(holidays))
	fc := clock.NewFake(start)
	c := memory.New(memory.WithClock(fc))
	var mu sync.Mutex
	var ticks []workflow.Tick
	if err := c.OnTick(func(_ context.Context, tk workflow.Tick) { mu.Lock(); ticks = append(ticks, tk); mu.Unlock() }); err != nil {
		t.Fatal(err)
	}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	dec25 := time.Date(2026, 12, 25, 0, 0, 0, 0, time.UTC)
	dec26 := dec25.AddDate(0, 0, 1)
	stepTo(t, fc, 1, dec25)
	stepTo(t, fc, 1, dec26)
	if got := rec.waitTicks(t, 1); len(got) != 1 || !got[0].Equal(dec26) {
		t.Fatalf("runs for %v, want only Dec 26", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(ticks)
		mu.Unlock()
		if n >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ticks) != 2 || ticks[0].Outcome != workflow.TickSkipped || ticks[0].Reason != workflow.TickReasonWhen || !ticks[0].At.Equal(dec25) || ticks[1].Outcome != workflow.TickFired {
		t.Fatalf("ticks = %+v, want Dec 25 skipped (when), Dec 26 fired", ticks)
	}
}

// capsMask hides capabilities of a client (and its optional interfaces,
// since it embeds only Client).
type capsMask struct {
	workflow.Client
	hide workflow.Caps
}

func (m capsMask) Capabilities() workflow.Caps { return m.Client.Capabilities().Without(m.hide) }

// countRuns counts the runs of wf a RunQuerier keeps.
func countRuns(t *testing.T, c workflow.Client, wf string) int {
	t.Helper()
	page, err := c.(workflow.RunQuerier).Runs(context.Background(), workflow.RunQuery{Workflow: wf, Limit: workflow.MaxRunsLimit})
	if err != nil {
		t.Fatal(err)
	}
	return len(page.Runs)
}

func TestBackfillPlan(t *testing.T) {
	ny, _ := cron.LoadZone("America/New_York")
	day := func(d int) time.Time { return time.Date(2026, 3, d, 0, 0, 0, 0, ny) }
	base := workflow.BackfillSpec{Workflow: "report", Cron: "30 2 * * *", Location: ny, From: day(7), To: day(10)}
	got, err := workflow.BackfillPlan(base)
	if err != nil {
		t.Fatal(err)
	}
	// Mar 8 02:30 does not exist in New York: it fires once at 03:00 EDT.
	want := []time.Time{time.Date(2026, 3, 7, 7, 30, 0, 0, time.UTC), time.Date(2026, 3, 8, 7, 0, 0, 0, time.UTC), time.Date(2026, 3, 9, 6, 30, 0, 0, time.UTC)}
	if len(got) != len(want) {
		t.Fatalf("plan %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("plan %v, want %v", got, want)
		}
	}
	// [From, To): a tick at From is in, one at To is out.
	edge := workflow.BackfillSpec{Workflow: "w", Cron: "0 0 * * *", Location: time.UTC, From: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)}
	if got, _ := workflow.BackfillPlan(edge); len(got) != 2 || !got[0].Equal(edge.From) {
		t.Fatalf("edges: %v", got)
	}
	// The Schedule's Active period and Windows limit the ticks.
	s := workflow.Schedule{Workflow: "w", Cron: "0 * * * *", Location: time.UTC,
		Active:  workflow.Active{Until: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)},
		Windows: workflow.Windows{Between: []workflow.Window{{From: "09:00", To: "17:00"}}}}
	if got, _ := workflow.BackfillPlan(workflow.BackfillSpec{Schedule: &s, From: edge.From, To: edge.To}); len(got) != 3 {
		t.Fatalf("windows and active: %v, want 09:00-11:00", got)
	}
	for name, bad := range map[string]workflow.BackfillSpec{
		"too many":    {Workflow: "w", Cron: "* * * * *", Location: time.UTC, From: edge.From, To: edge.To, Max: 10},
		"default":     {Workflow: "w", Cron: "0 * * * *", Location: time.UTC, From: edge.From, To: edge.From.AddDate(0, 0, 42)},
		"inverted":    {Workflow: "w", Cron: "* * * * *", Location: time.UTC, From: edge.To, To: edge.From},
		"no zone":     {Workflow: "w", Cron: "* * * * *", From: edge.From, To: edge.To},
		"bad cron":    {Workflow: "w", Cron: "0 0 L * *", Location: time.UTC, From: edge.From, To: edge.To},
		"both":        {Schedule: &s, Workflow: "w", From: edge.From, To: edge.To},
		"negative":    {Workflow: "w", Cron: "* * * * *", Location: time.UTC, From: edge.From, To: edge.To, Max: -1},
		"reserved":    {Workflow: "w", Cron: "* * * * *", Location: time.UTC, From: edge.From, To: edge.To, Labels: map[string]string{workflow.LabelTickAt: "x"}},
		"no workflow": {Cron: "* * * * *", Location: time.UTC, From: edge.From, To: edge.To},
	} {
		if _, err := workflow.BackfillPlan(bad); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
	ext := workflow.BackfillSpec{Workflow: "w", Cron: "0 0 L * *", Location: time.UTC, Parser: cron.Extended, From: edge.From, To: edge.From.AddDate(0, 3, 0)}
	if got, err := workflow.BackfillPlan(ext); err != nil || len(got) != 3 {
		t.Fatalf("extended dialect: %v %v", got, err)
	}
}

func TestBackfillOnMemory(t *testing.T) {
	rec := &tickRecorder{}
	c, _ := g1Client(t, time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC), []workflow.Workflow{rec.workflow("report")})
	ctx := context.Background()
	s := workflow.Schedule{Name: "daily", Workflow: "report", Cron: "0 2 * * *", Location: time.UTC, Input: []byte("in"), Labels: map[string]string{"team": "data"}}
	jan := func(d int) time.Time { return time.Date(2026, 1, d, 0, 0, 0, 0, time.UTC) }
	refs, err := workflow.Backfill(ctx, c, workflow.BackfillSpec{Schedule: &s, ScheduleID: "sch-1", From: jan(1), To: jan(4)})
	if err != nil || len(refs) != 3 {
		t.Fatalf("Backfill = %v, %v", refs, err)
	}
	for _, r := range refs {
		waitRun(t, c, r)
	}
	got := rec.waitTicks(t, 3)
	slices.SortFunc(got, func(a, b time.Time) int { return a.Compare(b) })
	for i, at := range got {
		if want := jan(i + 1).Add(2 * time.Hour); !at.Equal(want) {
			t.Fatalf("ticks %v, want 02:00 on Jan 1-3", got)
		}
	}
	l := rec.labels[0]
	if l[workflow.LabelTrigger] != workflow.LabelTriggerBackfill || l[workflow.LabelSchedule] != "sch-1" || l[workflow.LabelScheduleName] != "daily" || l["team"] != "data" || l[workflow.LabelRunKey] == "" {
		t.Fatalf("labels = %v", l)
	}
	// The runs finished, so their keys are free: the run-key lookup is
	// what keeps a repeat from starting them again.
	again, err := workflow.Backfill(ctx, c, workflow.BackfillSpec{Schedule: &s, ScheduleID: "sch-1", From: jan(1), To: jan(4)})
	if err != nil || !slices.Equal(again, refs) {
		t.Fatalf("repeat = %v, %v; want the same refs %v", again, err, refs)
	}
	overlap, err := workflow.Backfill(ctx, c, workflow.BackfillSpec{Schedule: &s, ScheduleID: "sch-1", From: jan(3), To: jan(6)})
	if err != nil || len(overlap) != 3 || overlap[0] != refs[2] {
		t.Fatalf("overlap = %v, %v", overlap, err)
	}
	rec.waitTicks(t, 5)
	if n := countRuns(t, c, "report"); n != 5 {
		t.Fatalf("%d runs, want 5 (3, then 2 new)", n)
	}
	// Another schedule's ticks are its own.
	if other, err := workflow.Backfill(ctx, c, workflow.BackfillSpec{Schedule: &s, ScheduleID: "sch-2", From: jan(1), To: jan(2)}); err != nil || other[0] == refs[0] {
		t.Fatalf("another schedule shares keys: %v %v", other, err)
	}

	// Without CapRunQuery the idempotency key alone cannot see a finished
	// run: the documented limit, proven so the lookup above is needed.
	blind := capsMask{Client: c, hide: workflow.CapRunQuery}
	before := countRuns(t, c, "report")
	r, err := workflow.Backfill(ctx, blind, workflow.BackfillSpec{Schedule: &s, ScheduleID: "sch-1", From: jan(1), To: jan(2)})
	if err != nil {
		t.Fatal(err)
	}
	waitRun(t, c, r[0])
	if n := countRuns(t, c, "report"); n != before+1 {
		t.Fatalf("without the lookup %d runs, want %d", n, before+1)
	}

	noKeys := capsMask{Client: c, hide: workflow.CapIdempotencyKey}
	if _, err := workflow.Backfill(ctx, noKeys, workflow.BackfillSpec{Schedule: &s, From: jan(1), To: jan(2)}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("without CapIdempotencyKey: %v, want ErrUnsupported", err)
	}
	if _, err := workflow.Backfill(ctx, noKeys, workflow.BackfillSpec{Schedule: &s, From: jan(20), To: jan(21)}, workflow.BackfillAllowDuplicates()); err != nil {
		t.Fatalf("with BackfillAllowDuplicates: %v", err)
	}
}

// TestBackfillConcurrentStartsEachTickOnce: two backfills of one range at
// once start each tick once, through the idempotency key.
func TestBackfillConcurrentStartsEachTickOnce(t *testing.T) {
	release := make(chan struct{})
	wf := workflow.Workflow{Name: "slow", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	}}}}
	c, _ := g1Client(t, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), []workflow.Workflow{wf})
	spec := workflow.BackfillSpec{Workflow: "slow", Cron: "0 0 * * *", Location: time.UTC, From: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 1, 11, 0, 0, 0, 0, time.UTC)}
	var wg sync.WaitGroup
	results := make([][]workflow.RunRef, 2)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := workflow.Backfill(context.Background(), capsMask{Client: c, hide: workflow.CapRunQuery}, spec)
			if err != nil {
				t.Error(err)
			}
			results[i] = r
		}()
	}
	wg.Wait()
	close(release)
	if !slices.Equal(results[0], results[1]) || len(results[0]) != 10 {
		t.Fatalf("two backfills got %v and %v", results[0], results[1])
	}
	if n := countRuns(t, c, "slow"); n != 10 {
		t.Fatalf("%d runs, want 10", n)
	}
}

func TestLimitRuns(t *testing.T) {
	from := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		expr    string
		seconds bool
		n       int
	}{{"0 * * * *", false, 3}, {"* * * * * *", true, 2}, {"@every 1s", false, 4}, {"30 1 * * *", false, 5}} {
		s, err := workflow.LimitRuns(workflow.Schedule{Workflow: "w", Cron: c.expr, Seconds: c.seconds, Location: time.UTC}, c.n, from, nil)
		if err != nil {
			t.Fatalf("%q: %v", c.expr, err)
		}
		sched, _ := workflow.ParseCron(c.expr, c.seconds, nil)
		count := 0
		for at := workflow.NextActive(sched, from.Add(-time.Hour), time.UTC, s.Active); !at.IsZero(); at = workflow.NextActive(sched, at, time.UTC, s.Active) {
			count++
		}
		// The first tick after from (a tick exactly at from is not counted:
		// a schedule made at from never fires it).
		if first := sched.Next(from, time.UTC); count != c.n || !s.Active.From.Equal(first) {
			t.Errorf("%q: %d ticks in %+v, want %d from %v", c.expr, count, s.Active, c.n, first)
		}
	}
	// A later From already on the schedule wins; an Until too early is refused.
	later := from.Add(5 * time.Hour)
	s, _ := workflow.LimitRuns(workflow.Schedule{Workflow: "w", Cron: "0 * * * *", Location: time.UTC, Active: workflow.Active{From: later}}, 1, from, nil)
	if !s.Active.From.Equal(later) || !s.Active.Until.Equal(later.Add(time.Second)) {
		t.Errorf("later From: %+v", s.Active)
	}
	for name, bad := range map[string]func() error{
		"n zero": func() error {
			_, err := workflow.LimitRuns(workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC}, 0, from, nil)
			return err
		},
		"zero from": func() error {
			_, err := workflow.LimitRuns(workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC}, 1, time.Time{}, nil)
			return err
		},
		"short until": func() error {
			_, err := workflow.LimitRuns(workflow.Schedule{Workflow: "w", Cron: "0 * * * *", Location: time.UTC, Active: workflow.Active{Until: from.Add(90 * time.Minute)}}, 3, from, nil)
			return err
		},
		"never fires": func() error {
			_, err := workflow.LimitRuns(workflow.Schedule{Workflow: "w", Cron: "0 0 30 2 *", Location: time.UTC}, 1, from, nil)
			return err
		},
		"bad cron": func() error {
			_, err := workflow.LimitRuns(workflow.Schedule{Workflow: "w", Cron: "nope", Location: time.UTC}, 1, from, nil)
			return err
		},
	} {
		if err := bad(); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
}

func TestOneShot(t *testing.T) {
	ny, _ := cron.LoadZone("America/New_York")
	at := time.Date(2026, 10, 14, 13, 30, 0, 0, time.UTC)
	s, err := workflow.OneShot(workflow.Schedule{Workflow: "w", Location: ny}, at)
	if err != nil || s.Cron != "30 9 14 10 *" || s.Seconds || !s.Active.From.Equal(at) {
		t.Fatalf("OneShot = %+v, %v", s, err)
	}
	s, err = workflow.OneShot(workflow.Schedule{Workflow: "w", Location: time.UTC}, at.Add(15*time.Second))
	if err != nil || s.Cron != "15 30 13 14 10 *" || !s.Seconds {
		t.Fatalf("OneShot with seconds = %+v, %v", s, err)
	}
	if _, err := workflow.OneShot(workflow.Schedule{Workflow: "w", Cron: "0 * * * *", Location: time.UTC}, at.Add(-30*time.Minute)); err != nil {
		t.Errorf("hourly at 13:00: %v", err)
	}
	firstNY := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)  // 01:30 EDT
	secondNY := time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC) // 01:30 EST, the repeat
	if _, err := workflow.OneShot(workflow.Schedule{Workflow: "w", Location: ny}, firstNY); err != nil {
		t.Errorf("first occurrence: %v", err)
	}
	for name, bad := range map[string]func() error{
		"repeat of a fall-back hour": func() error {
			_, err := workflow.OneShot(workflow.Schedule{Workflow: "w", Location: ny}, secondNY)
			return err
		},
		"no tick at at": func() error {
			_, err := workflow.OneShot(workflow.Schedule{Workflow: "w", Cron: "0 * * * *", Location: time.UTC}, at.Add(time.Minute))
			return err
		},
		"no zone": func() error { _, err := workflow.OneShot(workflow.Schedule{Workflow: "w"}, at); return err },
		"sub-second": func() error {
			_, err := workflow.OneShot(workflow.Schedule{Workflow: "w", Location: time.UTC}, at.Add(time.Millisecond))
			return err
		},
	} {
		if err := bad(); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
}

// TestLimitRunsOneShotAndPruneOnMemory: a limited schedule fires its n
// ticks and no more, a one-shot fires once, and PruneEndedSchedules
// removes both once ended, leaving the open-ended schedule.
func TestLimitRunsOneShotAndPruneOnMemory(t *testing.T) {
	start := time.Date(2026, 1, 5, 10, 0, 30, 0, time.UTC)
	limited, once, open := &tickRecorder{}, &tickRecorder{}, &tickRecorder{}
	c, fc := g1Client(t, start, []workflow.Workflow{limited.workflow("limited"), once.workflow("once"), open.workflow("open")})
	ctx := context.Background()
	s, err := workflow.LimitRuns(workflow.Schedule{Workflow: "limited", Cron: "* * * * *", Location: time.UTC}, 3, fc.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	limitedID, err := c.Schedule(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	oneAt := time.Date(2026, 1, 5, 10, 2, 0, 0, time.UTC)
	os, err := workflow.OneShot(workflow.Schedule{Workflow: "once", Location: time.UTC}, oneAt)
	if err != nil {
		t.Fatal(err)
	}
	onceID, err := c.Schedule(ctx, os)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Schedule(ctx, workflow.Schedule{Workflow: "open", Cron: "0 0 1 1 *", Location: time.UTC}); err != nil {
		t.Fatal(err)
	}
	// Three schedules arm timers; each ended one stops arming.
	armed := []int{3, 3, 2, 1, 1}
	for i := 1; i <= 5; i++ {
		stepTo(t, fc, armed[i-1], start.Add(time.Duration(i)*time.Minute))
		if i <= 3 {
			limited.waitTicks(t, i)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if n := len(limited.got()); n != 3 {
		t.Fatalf("limited schedule ran %d times, want 3", n)
	}
	if got := once.got(); len(got) != 1 || !got[0].Equal(oneAt) {
		t.Fatalf("one-shot ran at %v, want once at %v", got, oneAt)
	}
	// At 10:03 both have ended, but the grace (MisfireGrace) keeps them.
	if ids, err := workflow.PruneEndedSchedules(ctx, c, time.Date(2026, 1, 5, 10, 3, 0, 0, time.UTC)); err != nil || len(ids) != 0 {
		t.Fatalf("prune inside the grace = %v, %v", ids, err)
	}
	ids, err := workflow.PruneEndedSchedules(ctx, c, fc.Now())
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	want := []workflow.ScheduleID{limitedID, onceID}
	slices.Sort(want)
	if !slices.Equal(ids, want) {
		t.Fatalf("pruned %v, want %v", ids, want)
	}
	list, _ := c.Schedules(ctx)
	if len(list) != 1 || list[0].Workflow != "open" {
		t.Fatalf("left %+v, want the open-ended schedule", list)
	}
	if _, err := workflow.PruneEndedSchedules(ctx, capsMask{Client: c}, fc.Now()); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("a client without ScheduleLister: %v", err)
	}
}

func TestFireAtStartup(t *testing.T) {
	berlin, _ := cron.LoadZone("Europe/Berlin")
	start := time.Date(2026, 3, 2, 8, 0, 0, 0, time.UTC)
	rec := &tickRecorder{}
	c, fc := g1Client(t, start, []workflow.Workflow{rec.workflow("warm")})
	ctx := context.Background()
	s := workflow.Schedule{Name: "warm-cache", Workflow: "warm", Cron: "0 * * * *", Location: berlin, Labels: map[string]string{"k": "v"}}
	ref, err := workflow.FireAtStartup(ctx, c, s, workflow.StartupClock(fc))
	if err != nil {
		t.Fatal(err)
	}
	waitRun(t, c, ref)
	l := rec.labels[0]
	if l[workflow.LabelTrigger] != workflow.LabelTriggerStartup || l[workflow.LabelScheduleName] != "warm-cache" || l["k"] != "v" || rec.got()[0] != start {
		t.Fatalf("labels %v tick %v", l, rec.got())
	}
	if _, has := l[workflow.LabelSchedule]; has {
		t.Fatal("a startup run claims a schedule ID")
	}
	// Per node: a second call starts a second run.
	r2, _ := workflow.FireAtStartup(ctx, c, s, workflow.StartupClock(fc))
	if r2 == ref {
		t.Fatal("per-node startup was deduplicated")
	}
	waitRun(t, c, r2)

	// Once per day across the fleet: the same day starts nothing new,
	// even after the first run finished; the next day starts one.
	day := workflow.StartupOncePer(workflow.PeriodDay, berlin)
	a, err := workflow.FireAtStartup(ctx, c, s, workflow.StartupClock(fc), day)
	if err != nil {
		t.Fatal(err)
	}
	waitRun(t, c, a)
	fc.Advance(time.Hour)
	b, _ := workflow.FireAtStartup(ctx, c, s, workflow.StartupClock(fc), day)
	if b != a {
		t.Fatalf("second node the same day started %v, want %v", b, a)
	}
	if n := countRuns(t, c, "warm"); n != 3 {
		t.Fatalf("%d runs, want 3", n)
	}
	fc.Advance(24 * time.Hour)
	next, _ := workflow.FireAtStartup(ctx, c, s, workflow.StartupClock(fc), day)
	if next == a {
		t.Fatal("the next day was deduplicated")
	}
	waitRun(t, c, next)
	if _, err := workflow.FireAtStartup(ctx, capsMask{Client: c, hide: workflow.CapIdempotencyKey}, s, day); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("once per period without keys: %v", err)
	}
	if _, err := workflow.FireAtStartup(ctx, c, s, workflow.StartupOncePer(workflow.PeriodDay, nil)); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("no zone: %v", err)
	}
	if _, err := workflow.FireAtStartup(ctx, c, workflow.Schedule{Workflow: "warm"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("invalid schedule: %v", err)
	}
}
