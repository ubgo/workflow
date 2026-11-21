// Command schedule-keep-runs shows the parts of a schedule that live
// between its runs, on the in-memory driver with a fake clock and one
// worker, so the output never varies:
//
//   - the tick each run stands for (StepInfo.TickAt, TickTime), which
//     stays right when a misfire catch-up fires three missed hours at once;
//   - a cursor kept in the schedule's own DataStore folder and moved with
//     UpdateData, so each run imports the rows after the previous one's;
//   - KeepRuns and CronKeepRuns, which keep only the newest finished runs
//     of a schedule in the run store;
//   - the embedded zone database (cron/tzdata), for a schedule in
//     Europe/Berlin on an image without /usr/share/zoneinfo.
//
// It is the program behind the keep, cursor and tick sections of
// docs/schedules/manage.md, docs/schedules/cron.md and
// docs/schedules/time-zones.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/memory"
)

// keep is how many finished runs each schedule keeps in the run store.
const keep = 2

// batchSize is how many rows one run imports at most.
const batchSize = 3

// newestRow is the highest row ID in the stand-in source table.
const newestRow = 20

// cursorName is the name of the cursor inside the schedule's folder.
const cursorName = "cursor"

// outage is how long the process is down after the 10:00 run: from 10:30
// to 13:30 it misses the 11:00, 12:00 and 13:00 ticks.
const outage = 3 * time.Hour

// catchUp is the misfire policy's bound: every tick of the outage fires
// on return, oldest first.
const catchUp = 3

// wait bounds each wait for runs to end or be pruned; the memory driver
// takes well under a millisecond.
const wait = 5 * time.Second

// hourFormat prints ticks and clock readings as a wall time.
const hourFormat = "15:04"

// onTimeTicks is how many ticks fire on time before the outage: 09:00 and
// 10:00.
const onTimeTicks = 2

// armedTimers is how many timers the fake clock holds while both
// schedules (the import schedule and the declared rollup cron) wait for
// their next tick.
const armedTimers = 2

// afterTick is how long past each on-time tick the clock moves, so the
// outage begins at 10:30.
const afterTick = 30 * time.Minute

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "schedule-keep-runs:", err)
		os.Exit(1)
	}
}

// period returns the hour a run imports for: the hour ending at the run's
// tick. It reads the tick, not the clock, so a run that starts late still
// covers its own hour. ok is false for a run no schedule fired.
// snippet:period
func period(ctx context.Context) (from, to time.Time, ok bool) {
	info, _ := workflow.StepInfoFrom(ctx)
	tick, ok := info.TickAt() // the tick the schedule fired this run for, in UTC
	if !ok {
		return time.Time{}, time.Time{}, false // enqueued by hand: no tick
	}
	return tick.Add(-time.Hour), tick, true
}

// snippet:end

// importer is the scheduled step: it moves the schedule's cursor past the
// next rows and reports what it took, on lines.
type importer struct {
	berlin *time.Location
	clk    clock.Clock
	lines  chan string
}

// run is the import step's handler: it reads its hour from the tick, takes
// the next rows with the cursor and reports both on im.lines.
func (im importer) run(ctx context.Context, _ []byte) ([]byte, error) {
	from, to, ok := period(ctx)
	if !ok {
		return nil, errors.New("import: not fired by a schedule")
	}
	// snippet:cursor
	info, _ := workflow.StepInfoFrom(ctx)
	folder := workflow.Folder("schedule/" + info.Labels[workflow.LabelSchedule]) // this schedule's own folder
	var last int64
	next, err := workflow.UpdateData(ctx, folder, cursorName, func(cur int64, _ bool) (int64, error) {
		last = cur // set again on every attempt: UpdateData retries on a lost race
		return min(cur+batchSize, newestRow), nil
	})
	if err != nil {
		return nil, err
	}
	// Rows last+1..next are this run's alone: the compare-and-set gave them
	// to no other run. Import them here, outside the function UpdateData
	// may call more than once.
	// snippet:end
	im.lines <- fmt.Sprintf("tick %s, run at %s: orders %s-%s, rows %d-%d",
		to.In(im.berlin).Format(hourFormat), im.clk.Now().In(im.berlin).Format(hourFormat),
		from.In(im.berlin).Format(hourFormat), to.In(im.berlin).Format(hourFormat), last+1, next)
	return nil, nil
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	berlin, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		return err
	}
	// Monday 5 January 2026, 08:30 in Berlin: the first hourly tick is 09:00.
	clk := clock.NewFake(time.Date(2026, 1, 5, 8, 30, 0, 0, berlin))
	// One worker runs the catch-up runs one by one, oldest first, so the
	// rows each one takes are the same on every run of the program.
	mem := memory.New(memory.WithClock(clk), memory.WithWorkers(1))

	fmt.Fprintln(w, "-- driver")
	// snippet:caps
	need := workflow.CapsOf(workflow.CapScheduleKeep, workflow.CapTickTime)
	fmt.Fprintf(w, "  memory has %s: %v\n", need, mem.Capabilities().Has(need))
	// snippet:end

	// snippet:data
	c, err := workflow.Use(mem, workflow.Data(workflow.NewMemoryData())) // sqlstore.DataStore or redis.DataStore across processes
	if err != nil {
		return err
	}
	// snippet:end

	im := importer{berlin: berlin, clk: clk, lines: make(chan string, catchUp)}
	rollups := make(chan struct{}, catchUp)
	// snippet:cron-keep
	err = c.Register(workflow.Workflow{
		Name:         "rollup",
		Cron:         []string{"0 * * * *"},
		CronLocation: berlin,
		CronKeepRuns: keep, // each declared cron keeps its newest 2 finished runs
		Misfire:      workflow.Misfire{Policy: workflow.MisfireFireAll, Max: catchUp},
		Steps:        []workflow.Step{{Name: "sum", Run: rollup(rollups)}},
	})
	// snippet:end
	if err != nil {
		return err
	}
	if err := c.Register(workflow.Workflow{Name: "import-orders", Steps: []workflow.Step{{Name: "import", Run: im.run}}}); err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer mem.Stop()

	// snippet:keep
	id, err := c.Schedule(ctx, workflow.Schedule{
		Name:     "hourly-import",
		Workflow: "import-orders",
		Cron:     "0 * * * *",
		Location: berlin,
		KeepRuns: keep, // only the newest 2 finished runs stay in the run store
		Misfire:  workflow.Misfire{Policy: workflow.MisfireFireAll, Max: catchUp},
	})
	// snippet:end
	if err != nil {
		return err
	}

	fmt.Fprintln(w, "-- hourly import, three rows a run")
	for range onTimeTicks { // 09:00 and 10:00 fire on time
		if err := clk.BlockUntil(ctx, armedTimers); err != nil { // both schedules armed
			return err
		}
		now := clk.Now()
		clk.Advance(now.Truncate(time.Hour).Add(time.Hour).Sub(now))
		if err := collect(w, im.lines, rollups, 1); err != nil {
			return err
		}
		clk.Advance(afterTick)
	}

	fmt.Fprintln(w, "-- after a three-hour outage")
	if err := clk.BlockUntil(ctx, armedTimers); err != nil {
		return err
	}
	clk.Advance(outage) // nothing fired from 10:30 to 13:30
	if err := collect(w, im.lines, rollups, catchUp); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- run history")
	q := c.(workflow.RunQuerier)
	if err := history(ctx, w, q, "import-orders (KeepRuns 2)", berlin, workflow.RunQuery{
		Workflow: "import-orders",
		Labels:   map[string]string{workflow.LabelSchedule: string(id)},
	}); err != nil {
		return err
	}
	return history(ctx, w, q, "rollup (CronKeepRuns 2)", berlin, workflow.RunQuery{Workflow: "rollup"})
}

// rollup is the declared cron's step; it only reports that it ran.
func rollup(done chan<- struct{}) workflow.Handler {
	return func(context.Context, []byte) ([]byte, error) {
		done <- struct{}{}
		return nil, nil
	}
}

// collect prints the lines of n import runs and waits for n rollup runs.
func collect(w io.Writer, lines <-chan string, rollups <-chan struct{}, n int) error {
	for range n {
		select {
		case line := <-lines:
			fmt.Fprintln(w, " ", line)
		case <-time.After(wait):
			return errors.New("an import run did not end in time")
		}
		select {
		case <-rollups:
		case <-time.After(wait):
			return errors.New("a rollup run did not end in time")
		}
	}
	return nil
}

// history waits until the query finds only the kept runs, all finished,
// then prints how many there are and the ticks they stand for.
func history(ctx context.Context, w io.Writer, q workflow.RunQuerier, title string, loc *time.Location, query workflow.RunQuery) error {
	deadline := time.Now().Add(wait)
	for {
		page, err := q.Runs(ctx, query)
		if err != nil {
			return err
		}
		if len(page.Runs) == keep && allFinished(page.Runs) {
			// snippet:labels
			var ticks []string
			for _, r := range page.Runs {
				if at, ok := workflow.TickTime(r.Labels); ok { // the tick from a run's labels
					ticks = append(ticks, at.In(loc).Format(hourFormat))
				}
			}
			// snippet:end
			slices.Sort(ticks)
			fmt.Fprintf(w, "  %s: %d runs kept, ticks %s\n", title, len(page.Runs), strings.Join(ticks, " "))
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: %d runs after %s, want %d finished", title, len(page.Runs), wait, keep)
		}
		time.Sleep(time.Millisecond)
	}
}

// allFinished reports whether every run has ended.
func allFinished(runs []workflow.RunInfo) bool {
	return !slices.ContainsFunc(runs, func(r workflow.RunInfo) bool { return !r.Status.IsTerminal() })
}
