// Command cron-schedules shows recurring work: a cron declared on a
// workflow and evaluated in a time zone, schedules created and removed at
// run time, a schedule upserted by name on the SQL and memory drivers, the cron syntax
// and its parse errors, daylight saving time, misfire policies after a
// pause, another cron dialect, and what a UTC-only engine refuses.
//
// The memory parts run on a fake clock, so a day passes when the program
// moves the clock; the SQL part uses SQLite in a temporary directory. It
// is the program behind docs/schedules/cron.md, misfire.md and
// time-zones.md: the guides copy its regions (marked "snippet:") and task
// docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

// quietWindow is how long the program watches for a fire that must not
// happen after Unschedule; the fake clock fires synchronously, so a short
// window is enough.
const quietWindow = 50 * time.Millisecond

// monday is the fake clock's first instant: Monday 5 January 2026, 00:00 UTC.
var monday = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

// layout prints a fire time with its zone, so DST shifts are visible.
const layout = "Mon 2 Jan 15:04 MST"

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "cron-schedules:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	for _, part := range []func(context.Context, io.Writer) error{
		syntax, dst, declared, atRunTime, upsertByName, misfire, dialect, refusals,
	} {
		if err := part(ctx, out); err != nil {
			return err
		}
	}
	return nil
}

// syntax parses expressions with the core parser and prints their next
// fire times. cron.Parse reads no clock: the reference instant and the
// zone are arguments.
func syntax(_ context.Context, out io.Writer) error {
	fmt.Fprintln(out, "syntax:")
	// snippet:parse
	s, err := cron.Parse("0 9 * * MON-FRI") // 09:00 on weekdays
	if err != nil {
		return err
	}
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		return err
	}
	t := monday
	for range 3 {
		t = s.Next(t, ny) // strictly after t, evaluated in New York
		fmt.Fprintln(out, " ", t.Format(layout))
	}
	// snippet:end

	// snippet:vixie
	both, _ := cron.Parse("0 0 1 * MON") // day of month AND day of week restricted
	fires := both.Between(monday, monday.AddDate(0, 1, 0), time.UTC, 10)
	var days []string
	for _, f := range fires {
		days = append(days, f.Format("Mon 2 Jan"))
	}
	fmt.Fprintln(out, "  1st of the month or a Monday:", strings.Join(days, ", "))
	// snippet:end

	// snippet:parse-error
	_, err = cron.Parse("0 25 * * *")
	if pe, ok := errors.AsType[*cron.ParseError](err); ok {
		fmt.Fprintf(out, "  field %s, value %q: %v\n", pe.Field, pe.Value, err)
	}
	// snippet:end
	return nil
}

// dst prints what fixed-time and periodic schedules do on the two days New
// York changes its clocks in 2026: 8 March (02:00 jumps to 03:00) and
// 1 November (02:00 falls back to 01:00).
func dst(_ context.Context, out io.Writer) error {
	fmt.Fprintln(out, "daylight saving time:")
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		return err
	}
	// snippet:dst
	show := func(expr string, from time.Time, n int) {
		s, _ := cron.Parse(expr)
		var got []string
		for _, f := range s.Between(from, from.Add(48*time.Hour), ny, n) {
			got = append(got, f.In(ny).Format("Jan 2 15:04 MST"))
		}
		fmt.Fprintf(out, "  %-12q %s\n", expr, strings.Join(got, ", "))
	}
	spring := time.Date(2026, 3, 7, 12, 0, 0, 0, ny)
	fall := time.Date(2026, 10, 31, 12, 0, 0, 0, ny)
	show("30 2 * * *", spring, 2) // 02:30 does not exist on 8 March
	show("30 * * * *", spring.Add(13*time.Hour), 3)
	show("30 1 * * *", fall, 2) // 01:30 happens twice on 1 November
	show("30 * * * *", fall.Add(12*time.Hour), 3)
	// snippet:end
	return nil
}

// declared registers a workflow whose cron is evaluated in Kolkata and
// moves the fake clock from one fire to the next.
func declared(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "declared cron:")
	clk := clock.NewFake(monday)
	fired := make(chan time.Time, 4)
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return err
	}
	c := memory.New(memory.WithClock(clk))
	// snippet:declared
	err = c.Register(workflow.Workflow{
		Name:         "digest",
		Cron:         []string{"0 9 * * *"}, // 09:00 every day
		CronLocation: kolkata,               // in Kolkata, not UTC
		Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
			fired <- clk.Now()
			return nil, nil
		}}},
	})
	// snippet:end
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	s, _ := cron.Parse("0 9 * * *")
	for range 2 {
		if err := clk.BlockUntil(ctx, 1); err != nil { // the schedule armed its timer
			return err
		}
		clk.Set(s.Next(clk.Now(), kolkata))
		f := <-fired
		fmt.Fprintf(out, "  fired %s (%s)\n", f.In(kolkata).Format(layout), f.UTC().Format("15:04 MST"))
	}
	return nil
}

// atRunTime creates a schedule per tenant while the app runs, then
// removes it.
func atRunTime(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "schedule at run time:")
	clk := clock.NewFake(monday)
	fired := make(chan string, 4)
	c := memory.New(memory.WithClock(clk))
	err := c.Register(workflow.Workflow{
		Name: "report",
		Steps: []workflow.Step{{Name: "build", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			fired <- fmt.Sprintf("%s for %s at %s", in, info.Labels["tenant"], clk.Now().Format(layout))
			return nil, nil
		}}},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		return err
	}
	// snippet:schedule
	id, err := c.Schedule(ctx, workflow.Schedule{
		Name:     "acme-weekly",
		Workflow: "report",
		Cron:     "0 8 * * MON", // Mondays at 08:00
		Location: berlin,        // required: time.UTC for UTC
		Input:    []byte(`"weekly report"`),
		Labels:   map[string]string{"tenant": "acme"},
	})
	if err != nil {
		return err
	}
	// The app stores id (a column on the tenant row) to remove it later.
	// snippet:end
	if err := clk.BlockUntil(ctx, 1); err != nil { // the schedule armed its timer
		return err
	}
	clk.Set(time.Date(2026, 1, 5, 8, 0, 0, 0, berlin))
	fmt.Fprintln(out, " ", <-fired)

	// snippet:unschedule
	if err := c.Unschedule(ctx, id); err != nil {
		return err
	}
	if err := c.Unschedule(ctx, ""); err != nil { // an empty id is a no-op
		return err
	}
	// snippet:end
	for clk.Pending() > 0 { // the schedule's goroutine stops its timer
		time.Sleep(time.Millisecond)
	}
	clk.Advance(14 * 24 * time.Hour)
	select {
	case f := <-fired:
		return fmt.Errorf("fired after Unschedule: %s", f)
	case <-time.After(quietWindow):
		fmt.Fprintln(out, "  after Unschedule: two Mondays passed, no run")
	}
	return nil
}

// upsertByName declares the same named schedule twice on the SQL driver
// (SQLite in a temporary directory) and gets one schedule back, as an app
// that declares its schedules at every boot needs. The memory driver
// upserts by name the same way.
func upsertByName(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "upsert by name:")
	dir, err := os.MkdirTemp("", "cron-schedules")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	report := workflow.Workflow{Name: "report", Steps: []workflow.Step{{Name: "build", Run: noop}}}

	sq, err := sqlstore.New(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	if err := sq.Migrate(ctx); err != nil {
		return err
	}
	mem := memory.New()
	for _, c := range []workflow.Client{sq, mem} {
		if err := c.Register(report); err != nil {
			return err
		}
		if err := c.Start(ctx); err != nil {
			return err
		}
	}
	defer sq.Stop()
	defer mem.Stop()

	// snippet:upsert
	nightly := workflow.Schedule{Name: "nightly", Workflow: "report", Cron: "0 3 * * *", Location: time.UTC}
	first, err := sq.Schedule(ctx, nightly)
	if err != nil {
		return err
	}
	nightly.Cron = "30 3 * * *" // the next boot declares it again, changed
	second, err := sq.Schedule(ctx, nightly)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "  sqlstore, same name twice, same schedule:", first == second)
	// snippet:end
	a, err := mem.Schedule(ctx, nightly)
	if err != nil {
		return err
	}
	b, err := mem.Schedule(ctx, nightly)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "  memory, same name twice, same schedule:", a == b)
	return nil
}

// misfire pauses three hourly schedules for five hours (one jump of the
// fake clock) and counts what each policy fires when the scheduler wakes.
func misfire(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "misfire after a five-hour pause:")
	clk := clock.NewFake(monday)
	fired := make(chan string, 16)
	c := memory.New(memory.WithClock(clk))
	step := []workflow.Step{{Name: "run", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		fired <- string(in)
		return nil, nil
	}}}
	// snippet:misfire
	policies := map[string]workflow.Misfire{
		"skip":      {Policy: workflow.MisfireSkip},                            // fire nothing late
		"fire-once": {Policy: workflow.MisfireFireOnce, Within: 6 * time.Hour}, // the newest missed tick, if under 6h old
		"fire-all":  {Policy: workflow.MisfireFireAll, Max: 3},                 // up to 3 missed ticks, oldest first
	}
	// snippet:end
	if err := c.Register(workflow.Workflow{Name: "hourly", Steps: step}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	names := []string{"skip", "fire-once", "fire-all"}
	for _, name := range names {
		_, err := c.Schedule(ctx, workflow.Schedule{
			Workflow: "hourly",
			Cron:     "0 * * * *",
			Location: time.UTC,
			Input:    []byte(name),
			Misfire:  policies[name],
		})
		if err != nil {
			return err
		}
	}
	if err := clk.BlockUntil(ctx, len(names)); err != nil {
		return err
	}
	clk.Advance(5*time.Hour + 30*time.Minute)               // nothing ran from 00:00 to 05:30
	if err := clk.BlockUntil(ctx, len(names)); err != nil { // every schedule caught up and re-armed
		return err
	}
	count := map[string]int{}
	for range 4 { // fire-once 1 + fire-all 3; skip fires nothing
		count[<-fired]++
	}
	for _, name := range names {
		fmt.Fprintf(out, "  %-9s %d run(s)\n", name, count[name])
	}

	// snippet:due
	hourly, _ := cron.Parse("0 * * * *")
	last, now := monday, monday.Add(5*time.Hour+30*time.Minute)
	for _, name := range names {
		var ticks []string
		for _, t := range policies[name].Due(hourly, last, now, time.UTC) {
			ticks = append(ticks, t.Format("15:04"))
		}
		fmt.Fprintf(out, "  Due %-9s [%s]\n", name, strings.Join(ticks, " "))
	}
	// snippet:end
	return nil
}

// dialect plugs another cron parser into the memory driver: it adds one
// macro and hands everything else to the core parser. It takes the ctx
// only to share the signature of the other sections; Register needs none.
func dialect(_ context.Context, out io.Writer) error {
	fmt.Fprintln(out, "another dialect:")
	// snippet:dialect
	withQuarter := func(expr string) (cron.Schedule, error) {
		if expr == "@quarter-hour" {
			expr = "*/15 * * * *"
		}
		return cron.Parse(expr)
	}
	c := memory.New(memory.WithCronParser(withQuarter))
	// snippet:end
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	err := c.Register(workflow.Workflow{Name: "poll", Cron: []string{"@quarter-hour"}, Steps: []workflow.Step{{Name: "poll", Run: noop}}})
	fmt.Fprintln(out, "  memory with the dialect:", err)
	err = memory.New().Register(workflow.Workflow{Name: "poll", Cron: []string{"@quarter-hour"}, Steps: []workflow.Step{{Name: "poll", Run: noop}}})
	fmt.Fprintln(out, "  memory with cron.Parse:", err)
	return nil
}

// refusals shows what a driver refuses instead of shifting: an implicit
// zone on any driver, and on Hatchet (UTC only, no catch-up, no macros)
// a zone, a misfire policy and a macro. The Hatchet client connects only
// at Start, so Register runs without a server.
func refusals(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "refusals:")
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return err
	}
	c := memory.New()
	if err := c.Register(workflow.Workflow{Name: "report", Steps: []workflow.Step{{Name: "build", Run: noop}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	// snippet:refusals
	_, err = c.Schedule(ctx, workflow.Schedule{Workflow: "report", Cron: "0 8 * * *"}) // no Location
	fmt.Fprintln(out, "  no Location:", err)
	err = c.Register(workflow.Workflow{Name: "local", Cron: []string{"0 8 * * *"}, CronLocation: time.Local,
		Steps: []workflow.Step{{Name: "s", Run: noop}}})
	fmt.Fprintln(out, "  time.Local:", errors.Is(err, workflow.ErrInvalid))

	h := hatchet.New(hatchet.Config{})
	for _, wf := range []workflow.Workflow{
		{Name: "zoned", Cron: []string{"0 9 * * *"}, CronLocation: kolkata},
		{Name: "catch-up", Cron: []string{"0 9 * * *"}, Misfire: workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 3}},
		{Name: "macro", Cron: []string{"@daily"}},
	} {
		wf.Steps = []workflow.Step{{Name: "s", Run: noop}}
		fmt.Fprintf(out, "  hatchet %s: %v\n", wf.Name, h.Register(wf))
	}
	// snippet:end
	fmt.Fprintln(out, "  memory needs for a zoned cron:", workflow.Requires(workflow.Workflow{Cron: []string{"0 9 * * *"}, CronLocation: kolkata}),
		"| has them:", c.Capabilities().Has(workflow.CapCron|workflow.CapCronZone))
	return nil
}
