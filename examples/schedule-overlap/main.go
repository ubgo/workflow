// Command schedule-overlap keeps a slow hourly job from piling up, runs a
// schedule by hand and reads back its last fire, on the in-memory driver
// with a fake clock: a schedule with the skip overlap policy, a tick held
// back while the previous run is unfinished, a manual trigger refused for
// the same reason, a manual trigger once the run has ended (with the
// schedule's input and labels and a manual mark), and the schedule list
// with its last fire, last run and that run's status.
//
// It is the program behind docs/schedules/overlap.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "schedule-overlap:", err)
		os.Exit(1)
	}
}

// start is Monday 5 January 2026, 08:30 UTC: the first hourly tick is
// 09:00.
var start = time.Date(2026, 1, 5, 8, 30, 0, 0, time.UTC)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(start)
	mem := memory.New(memory.WithClock(clk))
	var c workflow.Client = mem
	need := workflow.CapOverlapSkip | workflow.CapScheduleTrigger | workflow.CapScheduleHistory | workflow.CapTickEvents | workflow.CapScheduleList
	if !c.Capabilities().Has(need) {
		return fmt.Errorf("the driver lacks %s", need&^c.Capabilities())
	}

	// The first run blocks until release is closed, standing in for a sync
	// that takes longer than the hour between ticks.
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	first := true
	err := c.Register(workflow.Workflow{
		Name: "sync-crm",
		Steps: []workflow.Step{{Name: "sync", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			started <- struct{}{}
			if first {
				first = false
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		}}},
	})
	if err != nil {
		return err
	}

	ticks := make(chan workflow.Tick, 8)
	if err := c.(workflow.TickNotifier).OnTick(func(_ context.Context, t workflow.Tick) { ticks <- t }); err != nil {
		return err
	}
	ends := make(chan workflow.RunEvent, 8)
	if err := c.(workflow.RunNotifier).OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer mem.Stop()

	// snippet:schedule
	id, err := c.Schedule(ctx, workflow.Schedule{
		Name:     "crm-hourly",
		Workflow: "sync-crm",
		Cron:     "0 * * * *",
		Location: time.UTC,
		Input:    []byte(`{"full":false}`),
		Labels:   map[string]string{"team": "sales"},
		// Hold a tick back while the run the previous fire started is
		// unfinished. The default, OverlapAllow, starts a run every tick.
		Overlap: workflow.OverlapSkip,
	})
	// snippet:end
	if err != nil {
		return err
	}

	fmt.Fprintln(out, "-- ticks")
	if err := hour(ctx, clk, ticks, out); err != nil { // 09:00 fires
		return err
	}
	<-started
	if err := hour(ctx, clk, ticks, out); err != nil { // 10:00 skipped: 09:00's run still going
		return err
	}

	fmt.Fprintln(out, "-- run now while the 09:00 run is unfinished")
	// snippet:trigger-refused
	trig := c.(workflow.ScheduleTriggerer)
	_, err = trig.TriggerSchedule(ctx, id)
	if errors.Is(err, workflow.ErrOverlap) {
		fmt.Fprintln(out, "  refused:", err)
	}
	// snippet:end
	if err == nil {
		return errors.New("a trigger during an unfinished run was accepted")
	}

	close(release)
	if ev := <-ends; ev.Status != workflow.StatusSucceeded {
		return fmt.Errorf("the 09:00 run ended %s", ev.Status)
	}

	fmt.Fprintln(out, "-- run now after it finished")
	clk.Advance(5 * time.Minute) // 10:05
	// snippet:trigger
	ref, err := trig.TriggerSchedule(ctx, id)
	if err != nil {
		return err
	}
	// snippet:end
	ev := <-ends
	fmt.Fprintf(out, "  %s %s, labels team=%s %s=%s\n", ref.RunID, ev.Status,
		ev.Labels["team"], workflow.LabelTrigger, ev.Labels[workflow.LabelTrigger])

	fmt.Fprintln(out, "-- schedules")
	// snippet:last
	infos, err := c.(workflow.ScheduleLister).Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range infos {
		fmt.Fprintf(out, "  %s overlap=%s last fire %s run %s (%s), next %s\n",
			s.Name, s.Overlap, s.Last.Fire.Format("15:04"), s.Last.Run.RunID, s.Last.Status,
			s.Next.Format("15:04"))
	}
	// snippet:end
	return nil
}

// hour moves the clock to the next hourly tick and prints its outcome.
func hour(ctx context.Context, clk *clock.Fake, ticks <-chan workflow.Tick, out io.Writer) error {
	if err := clk.BlockUntil(ctx, 1); err != nil {
		return err
	}
	now := clk.Now()
	clk.Advance(now.Truncate(time.Hour).Add(time.Hour).Sub(now))
	select {
	case t := <-ticks:
		at := t.At.In(time.UTC).Format("15:04")
		if t.Outcome == workflow.TickFired {
			fmt.Fprintf(out, "  %s fired, run %s\n", at, t.Run.RunID)
		} else {
			fmt.Fprintf(out, "  %s %s (%s)\n", at, t.Outcome, t.Reason)
		}
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("no tick in 5s")
	}
}
