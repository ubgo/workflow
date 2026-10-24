// Command cron-builder writes cron expressions from named frequencies
// (cron.Daily().At("9:00").Weekdays() and the rest of the builder's
// frequencies), shows what each one fires on, what the builder refuses, and a
// built expression driving a workflow's cron on the in-memory driver with
// a fake clock.
//
// It is the program behind the "Build an expression from a frequency"
// section of docs/schedules/cron.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "cron-builder:", err)
		os.Exit(1)
	}
}

// monday is the reference instant every listing starts from: Monday 5
// January 2026, 00:00 UTC.
var monday = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "frequencies:")
	// snippet:frequencies
	frequencies := []cron.Builder{
		cron.EverySeconds(30),
		cron.EveryMinutes(15).Weekdays(),
		cron.HourlyAt(17),
		cron.EveryHours(6),
		cron.DailyAt("9:00").Weekdays(),
		cron.TwiceDaily(1, 13),
		cron.WeeklyOn(time.Friday, "17:30"),
		cron.MonthlyOn(15, "8:00"),
		cron.Quarterly(),
		cron.YearlyOn(time.June, 1, "12:00"),
	}
	for _, f := range frequencies {
		s, err := f.Schedule() // the expression, parsed
		if err != nil {
			return err
		}
		next := s.Next(monday, time.UTC)
		fmt.Fprintf(out, "  %-18s next %s\n", s.String(), next.Format("Mon 2 Jan 15:04:05"))
	}
	// snippet:end

	fmt.Fprintln(out, "refusals:")
	// snippet:refusals
	for _, f := range []cron.Builder{
		cron.EveryMinutes(7),                     // */7 would fire at :56 and :00
		cron.Hourly().At("13:00"),                // At would drop the repetition
		cron.Monthly().Days(time.Monday),         // cron fires when EITHER day matches
		cron.YearlyOn(time.February, 30, "9:00"), // never fires
		cron.DailyAt("9:5"),                      // minutes take two digits
	} {
		_, err := f.Expr()
		fmt.Fprintf(out, "  ErrBuilder=%t %v\n", errors.Is(err, cron.ErrBuilder), err)
	}
	// snippet:end

	fmt.Fprintln(out, "on a workflow:")
	return onWorkflow(ctx, out)
}

// onWorkflow registers a workflow whose cron comes from the builder and
// lets a fake week pass.
func onWorkflow(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(monday)
	c := memory.New(memory.WithClock(clk))
	fired := make(chan time.Time, 8)
	// snippet:workflow
	err := c.Register(workflow.Workflow{
		Name:         "standup-reminder",
		Cron:         []string{cron.DailyAt("9:30").Weekdays().MustExpr()}, // "30 9 * * 1-5"
		CronLocation: time.UTC,
		Steps: []workflow.Step{{Name: "remind", Run: func(context.Context, []byte) ([]byte, error) {
			fired <- clk.Now()
			return nil, nil
		}}},
	})
	// snippet:end
	if err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer c.Stop()

	// Move the clock to each next fire, so every tick runs before the
	// next one is armed: six fires span a weekend.
	reminder, err := cron.DailyAt("9:30").Weekdays().Schedule()
	if err != nil {
		return err
	}
	var days []string
	for range 6 {
		if err := clk.BlockUntil(ctx, 1); err != nil {
			return err
		}
		clk.Advance(reminder.Next(clk.Now(), time.UTC).Sub(clk.Now()))
		select {
		case t := <-fired:
			days = append(days, t.Format("Mon 2 Jan 15:04"))
		case <-time.After(5 * time.Second):
			return errors.New("the reminder did not fire")
		}
	}
	fmt.Fprintln(out, " ", strings.Join(days, ", "))
	return nil
}
